package statesync

import (
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/looplab/fsm"
	pb "github.com/abchain/fabric/protos"

	"github.com/abchain/fabric/flogging"
	"context"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/golang/proto"
	"fmt"
)


func (syncHandler *stateSyncHandler) runSyncState(ctx context.Context, targetStateHash []byte) error {

	var err error
	var hash []byte

	syncHandler.client = newSyncer(ctx, syncHandler)

	defer logger.Infof("[%s]: Exit. remotePeerIdName <%s>", flogging.GoRDef, syncHandler.remotePeerIdName())
	defer syncHandler.fini()

	//---------------------------------------------------------------------------
	// 1. query local break point state hash and state offset
	//---------------------------------------------------------------------------
	data := syncHandler.client.ledger.LoadStateOffsetFromDB()
	if data == nil {
		syncHandler.client.ledger.EmptyState()
	} else {
		// get break point state hash
		hash, err = syncHandler.client.ledger.GetCurrentStateHash()
		if err != nil {
			return err
		}
		logger.Debugf("GetCurrentStateHash: <%x>", hash)
	}

	offset := &pb.SyncOffset{data}
	syncHandler.client.syncMessageHandler = newStateMessageHandler(offset, hash, syncHandler.client)
	//---------------------------------------------------------------------------
	// 2. handshake: send break point state hash and state offset to peer
	//---------------------------------------------------------------------------
	req := syncHandler.client.syncMessageHandler.produceSyncStartRequest()
	_, err = syncHandler.client.issueSyncRequest(req)
	if err == nil {
		// sync all k-v(s)
		err = syncHandler.client.executeSync()
	}

	//---------------------------------------------------------------------------
	// 3. clear persisted position
	//---------------------------------------------------------------------------
	if err == nil {
		syncHandler.client.ledger.ClearStateOffsetFromDB()
		hash, _ = syncHandler.client.ledger.GetCurrentStateHash()
		logger.Debugf("GetCurrentStateHash: <%x>", hash)
	}
	return err
}

//////////////////////////////////////////////////////////////////////////////
/////////  client
//////////////////////////////////////////////////////////////////////////////
func (sts *syncer) commitStateChunk(stateChunkArray *pb.SyncStateChunk, committedOffset *pb.SyncOffset) error {
	var err error

	umDelta := statemgmt.NewStateDelta()
	umDelta.ChaincodeStateDeltas = stateChunkArray.ChaincodeStateDeltas

	err = sts.ledger.ApplyStateDelta(stateChunkArray, umDelta)
	if err != nil {
		return err
	}

	err = sts.ledger.CommitAndIndexStateDelta(stateChunkArray, 0)
	if err != nil {
		return err
	}

	err = sts.ledger.SaveStateOffset(committedOffset)
	if err != nil {
		return err
	}

	return err
}

func (sts *syncer) executeSync() error {

	nextOffset, err := sts.syncMessageHandler.getInitialOffset()

	if err != nil {
		return err
	}

	for {
		syncMessage := &pb.SyncMessage{}
		syncMessage.Offset = nextOffset

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
			err = fmt.Errorf("Timed out during wait for sync message Response")
			break
		}

		if !ok {
			err = fmt.Errorf("sts.syncMessageChan error")
			break
		}

		nextOffset, err = sts.syncMessageHandler.processResponse(syncMessageResp)
		if err != nil {
			break
		}

		// 3. no more data to sync
		if nextOffset == nil {
			break
		}
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
		d.sendStateChuck(e, message.Offset)
	} else if message.PayloadType == pb.SyncType_SYNC_BLOCK {
		d.sendStateDeltasArray(e, message.Offset, message.CorrelationId)
	}
}

func (d *stateServer) sendStateDeltasArray(e *fsm.Event, offset *pb.SyncOffset, correlationId uint64) {

	blockOffset, err := offset.Unmarshal2BlockOffset()
	if err != nil {
		logger.Errorf("Error Unmarshal2BlockOffset: %s", err)
		panic("todo")
	}

	logger.Debugf("Sending state deltas for block range <%d-%d>", blockOffset.StartNum, blockOffset.EndNum)

	var blockNums []uint64
	syncBlockRange := &pb.SyncBlockRange{}

	syncBlockRange.Start = blockOffset.StartNum
	syncBlockRange.End = blockOffset.EndNum

	if syncBlockRange.Start > syncBlockRange.End {
		// Send in reverse order
		for i := syncBlockRange.Start; i >= syncBlockRange.End; i-- {
			blockNums = append(blockNums, i)
		}
	} else {
		for i := syncBlockRange.Start; i <= syncBlockRange.End; i++ {
			logger.Debugf("%s: Appending to blockNums: %d", flogging.GoRDef, i)
			blockNums = append(blockNums, i)
		}
	}

	blockStateArray := make([]*pb.BlockState, 0)

	failedReason := ""
	var currBlockNum uint64
	for _, currBlockNum = range blockNums {

		var block *pb.Block
		block, err = d.ledger.GetBlockByNumber(currBlockNum)
		if err != nil {
			break
		}

		var stateDelta *statemgmt.StateDelta
		// Get the state deltas for Block from coordinator
		stateDelta, err = d.ledger.GetStateDelta(currBlockNum)
		if err != nil {
			break
		}
		if stateDelta == nil {
			logger.Warningf("Requested to send a stateDelta for blockNum %d which has been discarded",
				currBlockNum)
			break
		}
		stateDeltaBytes := stateDelta.Marshal()

		logger.Debugf("currBlockNum<%d>, stateDelta: <%s>", currBlockNum, stateDelta)

		blockState := &pb.BlockState{StateDelta: stateDeltaBytes, Block: block}
		blockStateArray = append(blockStateArray, blockState)
	}

	if err != nil {
		failedReason += fmt.Sprintf("%s; ", err)
	}

	blockOffset.EndNum = currBlockNum
	syncStateDeltas := &pb.SyncBlockState{
		Range:    &pb.SyncBlockRange{Start: blockOffset.StartNum, End: currBlockNum, CorrelationId: correlationId},
		Syncdata: blockStateArray}

	var data []byte
	data, err = blockOffset.Byte()
	offset.Data = data

	if err != nil {
		failedReason += fmt.Sprintf("%s; ", err)
	}

	var syncMessage *pb.SyncMessage
	syncMessage, err = feedSyncMessage(offset, syncStateDeltas, pb.SyncType_SYNC_BLOCK_ACK)

	if err != nil {
		failedReason += fmt.Sprintf("%s; ", err)
	}
	syncMessage.FailedReason = failedReason

	err = d.parent.sendSyncMsg(e, pb.SyncMsg_SYNC_SESSION_SYNC_MESSAGE_ACK, syncMessage)
	if err != nil {
		logger.Errorf("Error sending SYNC_SESSION_SYNC_MESSAGE_ACK: %s", err)
	}

	logger.Debugf("Successfully sent stateDeltas for blockNum %d-%d", blockOffset.StartNum, currBlockNum)
}



func (server *stateServer) verifySyncStateReq(req *pb.SyncStartRequest) error {

	syncState := &pb.SyncState{}
	err := proto.Unmarshal(req.Payload, syncState)
	if err == nil {
		err = server.ledger.VerifySyncState(syncState)
	}
	logger.Infof("remoteHash: <%x>", syncState.Statehash)

	return err
}


func (server *stateServer) sendStateChuck(e *fsm.Event, offset *pb.SyncOffset) {

	var err error
	stateChunkArray, err := server.ledger.GetStateDeltaFromDB(offset)

	var failedReason string = ""
	if err != nil {
		failedReason = fmt.Sprintf("%s; ", err)
	} else {
		logger.Debugf("send <%s> stateDelta: <%s>", server.parent.remotePeerId, stateChunkArray)
	}

	var syncMessage *pb.SyncMessage
	syncMessage, err = feedSyncMessage(offset, stateChunkArray, pb.SyncType_SYNC_STATE_ACK)
	if err != nil {
		failedReason += fmt.Sprintf("%s", err)
	}

	syncMessage.FailedReason = failedReason
	err = server.parent.sendSyncMsg(e, pb.SyncMsg_SYNC_SESSION_SYNC_MESSAGE_ACK, syncMessage)
	if err != nil {
		logger.Errorf("Error sending SYNC_SESSION_SYNC_MESSAGE_ACK: %s", err)
	}
}

func feedSyncMessage(offset *pb.SyncOffset, payload proto.Message, syncType pb.SyncType) (*pb.SyncMessage, error) {

	syncMessage := &pb.SyncMessage{}
	syncMessage.Offset = offset
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