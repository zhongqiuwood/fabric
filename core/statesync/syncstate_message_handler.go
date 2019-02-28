package statesync

import (
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"

	"github.com/golang/proto"
	"fmt"
	"bytes"
)

type SyncMessageHandler interface {
	produceSyncStartRequest() *pb.SyncStartRequest
	feedPayload(syncMessage *pb.SyncMessage) error
	processResponse(syncMessage *pb.SyncMessage) (*pb.SyncOffset, error)
	getInitialOffset() (*pb.SyncOffset, error)
}

type StateMessageHandler struct {
	client *syncer
	statehash []byte
	offset *pb.SyncOffset
	partialSync *ledger.PartialSync
}
func newStateMessageHandler(client *syncer) *StateMessageHandler {

	handler := &StateMessageHandler{}
	handler.client = client
	var err error

	// TODO: load statehash from db
	handler.partialSync, err = handler.client.ledger.StartPartialSync(handler.statehash)

	var nextOffset *pb.SyncOffset
	nextOffset, err = handler.partialSync.CurrentOffset()

	if err != nil {
		return nil
	}
	handler.offset = nextOffset

	return handler
}

func (h *StateMessageHandler) feedPayload(syncMessage *pb.SyncMessage) error {

	syncMessage.PayloadType = pb.SyncType_SYNC_STATE
	return nil
}

func (h *StateMessageHandler) getInitialOffset() (*pb.SyncOffset, error) {
	return h.offset, nil
}

func (h *StateMessageHandler) produceSyncStartRequest() *pb.SyncStartRequest {

	req := &pb.SyncStartRequest{}
	req.PayloadType = pb.SyncType_SYNC_STATE

	payload := &pb.SyncState{}
	payload.Offset = h.offset
	payload.Statehash = h.statehash

	logger.Infof("Sync start at: statehash<%x>, offset<%x>",
		h.statehash, h.offset.Data)

	var err error
	req.Payload, err = proto.Marshal(payload)

	if err != nil {
		logger.Errorf("Error Unmarshal SyncState: %s", err)
		return nil
	}
	return req
}

func (h *StateMessageHandler) processResponse(syncMessage *pb.SyncMessage)  (*pb.SyncOffset, error) {

	stateChunkArrayResp := &pb.SyncStateChunk{}
	err := proto.Unmarshal(syncMessage.Payload, stateChunkArrayResp)
	if err != nil {
		return nil, err
	}

	if len(syncMessage.FailedReason) > 0 {
		err = fmt.Errorf("Sync state failed! Reason: %s", syncMessage.FailedReason)
		return nil, err
	}

	//if err = h.client.commitStateChunk(stateChunkArrayResp, syncMessage.Offset); err != nil {
	//	return nil, err
	//}

	err = h.partialSync.ApplyPartialSync(stateChunkArrayResp)
	if err != nil {
		return nil, err
	}

	var nextOffset []*pb.SyncOffset
	nextOffset, err = h.partialSync.RequiredParts()

	if err != nil {
		return nil, err
	}

	var res *pb.SyncOffset
	if nextOffset != nil {
		res = nextOffset[0]
	}

	if stateChunkArrayResp.Roothash != nil && res == nil {
		// all buckets synced, verify root hash
		var localHash []byte
		localHash, err = h.client.ledger.GetCurrentStateHash()
		if err == nil {
			logger.Infof("remote hash: <%x>", stateChunkArrayResp.Roothash)
			logger.Infof("local hash:  <%x>", localHash)

			if !bytes.Equal(localHash, stateChunkArrayResp.Roothash) {
				err = fmt.Errorf("Sync state failed! Target root hash <%x>, local root hash <%x>",
					stateChunkArrayResp.Roothash, localHash)
			}
		}
	}

	return res, err
}
