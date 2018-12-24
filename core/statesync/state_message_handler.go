package statesync

import (
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
	pb "github.com/abchain/fabric/protos"

	"github.com/abchain/fabric/core/statesync/persist"
	"github.com/golang/proto"
	"fmt"
	"bytes"
)

type SyncMessageHandler interface {
	produceSyncStartRequest() *pb.SyncStartRequest
	feedPayload(syncMessage *pb.SyncMessage) error
	processResponse(syncMessage *pb.SyncMessage) error
}

type StateMessageHandler struct {
	client *syncer
	level uint64
	start uint64
	end uint64
	statehash []byte
}

func newStateMessageHandler(level, start, end uint64, statehash []byte, client *syncer) *StateMessageHandler {
	handler := &StateMessageHandler{}
	handler.client = client
	handler.start = start
	handler.level = level
	handler.end = end
	handler.statehash = statehash
	return handler
}

func (h *StateMessageHandler) feedPayload(syncMessage *pb.SyncMessage) error {

	payloadMsg := &pb.SyncStateChunkArrayRequest{
		Level: h.level,
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
	payload.Level = h.level
	payload.BucketNum = h.start
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

func (h *StateMessageHandler) processResponse(syncMessage *pb.SyncMessage) error {

	stateChunkResp := &pb.SyncStateChunkArray{}

	err := proto.Unmarshal(syncMessage.Payload, stateChunkResp)
	if err != nil {
		return err
	}

	if len(stateChunkResp.FailedReason) > 0 {
		err = fmt.Errorf("Sync state failed! Reason: %s", stateChunkResp.FailedReason)
		return err
	}

	var lastNum uint64
	if err, lastNum = h.client.commitStateChunk(stateChunkResp); err != nil {
		return err
	}

	if err = persist.StoreSyncPosition(h.level, syncMessage.EndNum); err != nil {
		return err
	}

	if stateChunkResp.Roothash != nil && lastNum == h.end {
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

	return err
}
