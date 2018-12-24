package statesync

import (
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/looplab/fsm"
	pb "github.com/abchain/fabric/protos"

	"github.com/abchain/fabric/flogging"
	"context"
	"github.com/abchain/fabric/core/ledger/statemgmt/buckettree"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/core/statesync/persist"
	"github.com/golang/proto"
	"bytes"
	"fmt"
	"github.com/spf13/viper"
)

func (syncHandler *stateSyncHandler) runSyncState(ctx context.Context, targetStateHash []byte) error {

	var err error
	var hash []byte

	syncHandler.client = newSyncer(ctx, syncHandler)

	defer logger.Infof("[%s]: Exit. remotePeerIdName <%s>", flogging.GoRDef, syncHandler.remotePeerIdName())
	defer syncHandler.fini()

	//---------------------------------------------------------------------------
	// 1. query local root state hash and state offset
	//---------------------------------------------------------------------------
	level, start := persist.LoadSyncPosition()
	if level == 0 && start == 0 {
		// sync from beginning
		// clear stateCf and cache
		syncHandler.client.ledger.EmptyState()
		level = uint64(buckettree.BucketTreeConfig().GetSyncLevel())
		start = 1
	} else {
		// sync from break point
		hash, err = syncHandler.client.ledger.GetRootStateHashFromDB()
		logger.Infof("[%s]: LoadSyncPosition level <%d>, start <%d>," +
			"RootStateHash: <%x>", flogging.GoRDef, level, start, hash)
		if err != nil {
			return err
		}
	}

	delta := uint64(100)
	end := uint64(buckettree.BucketTreeConfig().GetNumBuckets(int(level)))
	handler := newStateMessageHandler(level, start, end, hash, syncHandler.client)
	syncHandler.client.syncMessageHandler = handler
	//---------------------------------------------------------------------------
	// 2. send rootStateHash, state offset and targetStateHash to peer
	//---------------------------------------------------------------------------
	// level, startNum, endNum, stateHash
	req := syncHandler.client.syncMessageHandler.produceSyncStartRequest()
	_, err = syncHandler.client.issueSyncRequest(req)
	if err == nil {
		logger.Infof("Sync start at: bucker tree level <%d>, bucker num <%d to %d>, current root hash <%x>",
			handler.level, handler.start, handler.end, handler.statehash)

		err = syncHandler.client.syncProcess(start, end, delta)
	}

	//---------------------------------------------------------------------------
	// 3. clear persisted position
	//---------------------------------------------------------------------------
	if err == nil {
		persist.ClearSyncPosition()
		hash, _ = syncHandler.client.ledger.GetRootStateHashFromDB()
		logger.Debugf("RootStateHash: <%x>", hash)
	}
	return err
}


//////////////////////////////////////////////////////////////////////////////
/////////  client
//////////////////////////////////////////////////////////////////////////////
func (sts *syncer) commitStateChunk(stateChunkArray *pb.SyncStateChunkArray) (error, uint64) {
	var err error
	var stateChunkResp *pb.SyncStateChunk
	for _, stateChunkResp = range stateChunkArray.Chunks {

		umDelta := statemgmt.NewStateDelta()
		umDelta.ChaincodeStateDeltas = stateChunkResp.ChaincodeStateDeltas

		err = sts.ledger.ApplyStateDelta(stateChunkResp, umDelta)
		if err != nil {
			break
		}

		err = sts.ledger.CommitAndIndexStateDelta(stateChunkResp, 0)
		if err != nil {
			break
		}
	}
	return err, stateChunkResp.BucketNum
}

func (sts *syncer) syncProcess(start, end, delta uint64) error {
	var err error

	if delta == 0 {
		return fmt.Errorf("Invalid delta!")
	}

	offset := start

	for offset <= end {
		syncMessage := &pb.SyncMessage{}
		syncMessage.StartNum = offset

		syncMessage.EndNum = offset + delta - 1
		if syncMessage.EndNum > end {
			syncMessage.EndNum = end
		}

		logger.Debugf("Ask for startNum--endNum: %d--%d", syncMessage.StartNum, syncMessage.EndNum)

		// 1. feed payload
		err = sts.syncMessageHandler.feedPayload(syncMessage)
		if err != nil {
			break
		}

		err = sts.parent.sendSyncMsg(nil, pb.SyncMsg_SYNC_SESSION_SYNC_MESSAGE, syncMessage)
		if err != nil {
			break
		}

		// 2. process response
		var syncMessageResp *pb.SyncMessage
		var ok bool

		select {
		case syncMessageResp, ok = <- sts.syncMessageChan:
			if !ok {
				err = fmt.Errorf("sync Message channel close")
				break
			}

		case <-sts.Done():
			err = fmt.Errorf("Timed out during wait for start Response")
			break
		}

		if !ok {
			err = fmt.Errorf("sts.syncMessageChan error")
			break
		}

		err = sts.syncMessageHandler.processResponse(syncMessageResp)
		if err != nil {
			break
		}
		logger.Debugf("Recv and commit startNum--endNum: %d--%d", syncMessageResp.StartNum, syncMessageResp.EndNum)

		// 3. ask for more
		offset += delta
	}

	return err
}



func (sts *syncer) afterSyncMessage(e *fsm.Event) {

	payloadMsg := &pb.SyncMessage{}
	msg := sts.parent.onRecvSyncMsg(e, payloadMsg)
	if msg == nil {
		return
	}

	defer func() {
		if x := recover(); x != nil {
			logger.Errorf("Error sending syncMessageChan to channel: %v, <%v>", x, payloadMsg)
		}
	}()

	sts.syncMessageChan <- payloadMsg
}

//////////////////////////////////////////////////////////////////////////////
/////////  server
//////////////////////////////////////////////////////////////////////////////
func (server *stateServer) beforeSyncMessage(e *fsm.Event) {
	payloadMsg := &pb.SyncMessage{}
	syncMsg := server.parent.onRecvSyncMsg(e, payloadMsg)
	if syncMsg == nil || server.correlationId != syncMsg.CorrelationId {
		return
	}

	go server.sendSyncMessageResponse(e, payloadMsg)
}

func (d *stateServer) sendSyncMessageResponse(e *fsm.Event, message *pb.SyncMessage) {

	if message.PayloadType == pb.SyncType_SYNC_STATE {

		stateChunkRequest := &pb.SyncStateChunkArrayRequest{}
		err := proto.Unmarshal(message.Payload, stateChunkRequest)
		if err != nil {
			return
		}
		d.sendStateChuck(e, stateChunkRequest, message.StartNum, message.EndNum)
	} else if message.PayloadType == pb.SyncType_SYNC_BLOCK {

	}
}


func (server *stateServer) verifySyncStateReq(req *pb.SyncStartRequest) error {

	syncState := &pb.SyncState{}
	err := proto.Unmarshal(req.Payload, syncState)
	if err != nil {
		logger.Errorf("Error Unmarshal SyncState: %s", err)
	}

	localHash, err := server.ledger.ComputeBreakPointHash(int(syncState.Level), int(syncState.BucketNum))
	if err != nil {
		logger.Errorf("Error ComputeBreakPointHash: %s", err)
	}

	logger.Infof("localHash:  <%x>", localHash)
	logger.Infof("remoteHash: <%x>", syncState.Statehash)

	bucketIndex := syncState.BucketNum
	if bucketIndex == 1 {
		if syncState.Statehash != nil {
			err = fmt.Errorf("Invalid Statehash. The nil expected")
		}
	} else if bucketIndex > 1 {
		if !bytes.Equal(localHash, syncState.Statehash) {
			err = fmt.Errorf("Invalid Statehash")
		}
	}

	return err
}


func (server *stateServer) sendStateChuck(e *fsm.Event, syncStateRequest *pb.SyncStateChunkArrayRequest, start, end uint64) {

	level := int(syncStateRequest.Level)
	bucketIndex := int(start)
	maxNumBuckets := int(end)

	logger.Infof("Recv level: <%d>, start-end: <%d-%d>", level, bucketIndex, maxNumBuckets)

	err := buckettree.BucketTreeConfig().Verify(level, bucketIndex, maxNumBuckets)

	stateChunkArray := &pb.SyncStateChunkArray{
		Level:        syncStateRequest.Level,
	}

	breakpoint := viper.GetBool("peer.breakpoint")
	if breakpoint {
		if bucketIndex >= buckettree.BucketTreeConfig().GetNumBuckets(level) / 2 {
			err = fmt.Errorf("hit break point")
		}
	}

	if err == nil {
		for i := bucketIndex; i <= maxNumBuckets; i++ {
			res := server.ledger.ProduceStateDeltaFromDB(level, i)

			stateChunk := &pb.SyncStateChunk{
				BucketNum:            uint64(i),
				ChaincodeStateDeltas: res,
			}
			stateChunkArray.Chunks = append(stateChunkArray.Chunks, stateChunk)
		}

		if maxNumBuckets >= buckettree.BucketTreeConfig().GetNumBuckets(level) {
			hash, err := server.ledger.GetRootStateHashFromDB()
			if err != nil {
				logger.Errorf("Error GetRootStateHashFromDB: <%s>", err)
			}
			stateChunkArray.Roothash = hash
			logger.Debugf("RootStateHash by <level-maxNumBuckets> <%d-%d>, <%x>", level, maxNumBuckets, hash)
		}

		logger.Debugf("send <%s> stateDelta: <%s>", server.parent.remotePeerId, stateChunkArray)
	} else {
		stateChunkArray.FailedReason = fmt.Sprintf("%s", err)
	}

	var syncMessage *pb.SyncMessage
	syncMessage, err = feedSyncMessage(start, end, stateChunkArray, pb.SyncType_SYNC_STATE_ACK)
	if err != nil {
		logger.Errorf("Error sending syncStateRequest <%s>: %s", syncStateRequest, err)
		stateChunkArray.FailedReason += fmt.Sprintf(" %s", err)
	}

	err = server.parent.sendSyncMsg(e, pb.SyncMsg_SYNC_SESSION_SYNC_MESSAGE_ACK, syncMessage)
	if err != nil {
		logger.Errorf("Error sending syncStateRequest <%s>: %s", syncStateRequest, err)
	}
}

func feedSyncMessage(start, end uint64, payload proto.Message, syncType pb.SyncType) (*pb.SyncMessage, error) {

	syncMessage := &pb.SyncMessage{}
	syncMessage.StartNum = start
	syncMessage.EndNum = end
	syncMessage.PayloadType = syncType
	var err error
	syncMessage.Payload, err = proto.Marshal(payload)
	return syncMessage, err
}



func (sts *syncer) issueSyncRequest(request *pb.SyncStartRequest) (uint64, error) {

	sts.parent.fsmHandler.Event(enterSyncBegin)
	err := sts.parent.sendSyncMsg(nil, pb.SyncMsg_SYNC_SESSION_START, request)
	endBlockNumber := uint64(0)

	if err != nil {
		return 0, err
	}

	select {
	case response, ok := <-sts.startResponseChan:
		if !ok {
			return 0, fmt.Errorf("startResponse channel close : %s", err)
		}
		logger.Infof("Sync request RejectedReason<%s>, Remote peer Blockchain Height <%d>",
			response.RejectedReason, response.BlockHeight)

		if len(response.RejectedReason) > 0 {
			err = fmt.Errorf("Sync request rejected! Reason: %s", response.RejectedReason)
		} else {
			endBlockNumber = response.BlockHeight - 1
		}

	case <-sts.Done():
		return 0, fmt.Errorf("Timed out during wait for start Response")
	}

	return endBlockNumber, err
}