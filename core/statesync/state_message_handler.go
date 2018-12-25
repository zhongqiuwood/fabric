package statesync

import (
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
	pb "github.com/abchain/fabric/protos"

	"github.com/golang/proto"
	"fmt"
	"bytes"
)

type SyncMessageHandler interface {
	produceSyncStartRequest() *pb.SyncStartRequest
	feedPayload(syncMessage *pb.SyncMessage) error
	processResponse(syncMessage *pb.SyncMessage) (*pb.StateOffset, error)
}

type StateMessageHandler struct {
	client *syncer
	statehash []byte
	offset *pb.StateOffset
}

func newStateMessageHandler(offset *pb.StateOffset, statehash []byte, client *syncer) *StateMessageHandler {
	handler := &StateMessageHandler{}
	handler.client = client
	handler.offset = offset
	handler.statehash = statehash
	return handler
}

func (h *StateMessageHandler) feedPayload(syncMessage *pb.SyncMessage) error {

	payloadMsg := &pb.SyncStateChunkArrayRequest{
	}
	data, err := proto.Marshal(payloadMsg)
	if err != nil {
		logger.Errorf("Error Marshal SyncMsg_SYNC_SESSION_SYNC_MESSAGE: %s", err)
		return err
	}

	syncMessage.PayloadType = pb.SyncType_SYNC_STATE
	syncMessage.Payload = data
	return nil
}


func (h *StateMessageHandler) produceSyncStartRequest() *pb.SyncStartRequest {

	req := &pb.SyncStartRequest{}
	req.PayloadType = pb.SyncType_SYNC_STATE

	payload := &pb.SyncState{}
	payload.Offset = h.offset
	payload.Statehash = h.statehash

	logger.Debugf("Sync request: <Level-BucketNum>: <%d-%d>, local state hash: <%x>",
		payload.Level, payload.BucketNum, payload.Statehash)

	var err error
	req.Payload, err = proto.Marshal(payload)

	if err != nil {
		logger.Errorf("Error Unmarshal SyncState: %s", err)
		return nil
	}
	return req
}

func (h *StateMessageHandler) processResponse(syncMessage *pb.SyncMessage)  (*pb.StateOffset, error) {

	stateChunkResp := &pb.SyncStateChunkArray{}

	err := proto.Unmarshal(syncMessage.Payload, stateChunkResp)
	if err != nil {
		return nil, err
	}

	if len(stateChunkResp.FailedReason) > 0 {
		err = fmt.Errorf("Sync state failed! Reason: %s", stateChunkResp.FailedReason)
		return nil, err
	}

	var offset *pb.StateOffset
	if err, offset = h.client.commitStateChunk(stateChunkResp); err != nil {
		return nil, err
	}

	offset, err = h.client.ledger.LoadStateOffset(syncMessage.Offset)
	if err != nil {
		return nil, err
	}

	if stateChunkResp.Roothash != nil && offset == nil {
		// all buckets synced, verify root hash
		var localHash []byte
		localHash, err = h.client.ledger.GetRootStateHashFromDB()
		if err == nil {
			logger.Infof("remote hash: <%x>", stateChunkResp.Roothash)
			logger.Infof("local hash:  <%x>", localHash)

			if !bytes.Equal(localHash, stateChunkResp.Roothash) {
				err = fmt.Errorf("Sync state failed! Target root hash <%x>, local root hash <%x>",
					stateChunkResp.Roothash, localHash)
			}
		}

	}

	return offset, err
}
