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

	syncMessage.PayloadType = pb.SyncType_SYNC_STATE
	return nil
}


func (h *StateMessageHandler) produceSyncStartRequest() *pb.SyncStartRequest {

	req := &pb.SyncStartRequest{}
	req.PayloadType = pb.SyncType_SYNC_STATE

	payload := &pb.SyncState{}
	payload.Offset = h.offset
	payload.Statehash = h.statehash

	logger.Infof("Sync start at:<%v>", h)

	var err error
	req.Payload, err = proto.Marshal(payload)

	if err != nil {
		logger.Errorf("Error Unmarshal SyncState: %s", err)
		return nil
	}
	return req
}

func (h *StateMessageHandler) processResponse(syncMessage *pb.SyncMessage)  (*pb.StateOffset, error) {

	stateChunkArrayResp := &pb.SyncStateChunk{}
	err := proto.Unmarshal(syncMessage.Payload, stateChunkArrayResp)
	if err != nil {
		return nil, err
	}

	if len(stateChunkArrayResp.FailedReason) > 0 {
		err = fmt.Errorf("Sync state failed! Reason: %s", stateChunkArrayResp.FailedReason)
		return nil, err
	}

	if err = h.client.commitStateChunk(stateChunkArrayResp, syncMessage.Offset); err != nil {
		return nil, err
	}

	var nextOffset *pb.StateOffset
	nextOffset, err = h.client.ledger.NextStateOffset(syncMessage.Offset)
	if err != nil {
		return nil, err
	}

	if stateChunkArrayResp.Roothash != nil && nextOffset == nil {
		// all buckets synced, verify root hash
		var localHash []byte
		localHash, err = h.client.ledger.GetRootStateHashFromDB()
		if err == nil {
			logger.Infof("remote hash: <%x>", stateChunkArrayResp.Roothash)
			logger.Infof("local hash:  <%x>", localHash)

			if !bytes.Equal(localHash, stateChunkArrayResp.Roothash) {
				err = fmt.Errorf("Sync state failed! Target root hash <%x>, local root hash <%x>",
					stateChunkArrayResp.Roothash, localHash)
			}
		}

	}

	return nextOffset, err
}
