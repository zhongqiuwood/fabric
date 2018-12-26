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
		hash, err = syncHandler.client.ledger.GetRootStateHashFromDB()
		if err != nil {
			return err
		}
	}

	offset := &pb.StateOffset{data}
	syncHandler.client.syncMessageHandler = newStateMessageHandler(offset, hash, syncHandler.client)
	//---------------------------------------------------------------------------
	// 2. handshake: send break point state hash and state offset to peer
	//---------------------------------------------------------------------------
	req := syncHandler.client.syncMessageHandler.produceSyncStartRequest()
	_, err = syncHandler.client.issueSyncRequest(req)
	if err == nil {
		// sync all k-v(s)
		err = syncHandler.client.runSyncState()
	}

	//---------------------------------------------------------------------------
	// 3. clear persisted position
	//---------------------------------------------------------------------------
	if err == nil {
		syncHandler.client.ledger.ClearStateOffsetFromDB()
		hash, _ = syncHandler.client.ledger.GetRootStateHashFromDB()
		logger.Debugf("RootStateHash: <%x>", hash)
	}
	return err
}

//////////////////////////////////////////////////////////////////////////////
/////////  client
//////////////////////////////////////////////////////////////////////////////
func (sts *syncer) commitStateChunk(stateChunkArray *pb.SyncStateChunk, committedOffset *pb.StateOffset) error {
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

func (sts *syncer) runSyncState() error {

	nextOffset, err := sts.ledger.NextStateOffset(nil)
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
			err = fmt.Errorf("Timed out during wait for start Response")
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

	}
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


func (server *stateServer) sendStateChuck(e *fsm.Event, offset *pb.StateOffset) {

	var err error
	stateChunkArray, err := server.ledger.GetStateDeltaFromDB(offset)

	if err != nil {
		stateChunkArray  = &pb.SyncStateChunk{}
		stateChunkArray.FailedReason = fmt.Sprintf("%s", err)
	} else {
		logger.Debugf("send <%s> stateDelta: <%s>", server.parent.remotePeerId, stateChunkArray)
	}

	var syncMessage *pb.SyncMessage
	syncMessage, err = feedSyncMessage(offset, stateChunkArray, pb.SyncType_SYNC_STATE_ACK)
	if err != nil {
		stateChunkArray.FailedReason += fmt.Sprintf(" %s", err)
	}

	err = server.parent.sendSyncMsg(e, pb.SyncMsg_SYNC_SESSION_SYNC_MESSAGE_ACK, syncMessage)
	if err != nil {
		logger.Errorf("Error sending SYNC_SESSION_SYNC_MESSAGE_ACK: %s", err)
	}
}

func feedSyncMessage(offset *pb.StateOffset, payload proto.Message, syncType pb.SyncType) (*pb.SyncMessage, error) {

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