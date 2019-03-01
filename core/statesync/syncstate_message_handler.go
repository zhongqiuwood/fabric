package statesync

import (
	"fmt"
	"github.com/abchain/fabric/core/ledger"
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
)

type SyncMessageHandler interface {
	produceSyncStartRequest() *pb.SyncStartRequest
	feedPayload(syncMessage *pb.SyncMessage) error
	processResponse(syncMessage *pb.SyncMessage) (*pb.SyncOffset, error)
	getInitialOffset() (*pb.SyncOffset, error)
}

type StateMessageHandler struct {
	client    *syncer
	statehash []byte
	//	offset      *pb.SyncOffset
	partialSync *ledger.PartialSync
}

func newStateMessageHandler(client *syncer) *StateMessageHandler {

	handler := &StateMessageHandler{}
	handler.client = client
	var err error

	// TODO: load statehash from db
	handler.partialSync, err = handler.client.ledger.StartPartialSync(handler.statehash)

	// var nextOffset *pb.SyncOffset
	// nextOffset, err = handler.partialSync.CurrentOffset()

	if err != nil {
		return nil
	}
	// handler.offset = nextOffset

	return handler
}

func (h *StateMessageHandler) feedPayload(syncMessage *pb.SyncMessage) error {

	syncMessage.PayloadType = pb.SyncType_SYNC_STATE
	return nil
}

func (h *StateMessageHandler) getOneOffset() (*pb.SyncOffset, error) {
	offsets, err := h.partialSync.RequiredParts()
	if err != nil {
		return nil, err
	} else if len(offsets) == 0 {
		return nil, fmt.Errorf("No task can be found, sync module has some problem")
	} else {
		return offsets[0], nil
	}
}

func (h *StateMessageHandler) getInitialOffset() (*pb.SyncOffset, error) {
	return h.getOneOffset()
}

func (h *StateMessageHandler) produceSyncStartRequest() *pb.SyncStartRequest {

	offset, err := h.getOneOffset()
	if err != nil {
		logger.Errorf("Can not get new task: %s", err)
		return nil
	}

	req := &pb.SyncStartRequest{}
	req.PayloadType = pb.SyncType_SYNC_STATE

	payload := &pb.SyncState{Offset: offset, Statehash: h.statehash}

	logger.Infof("Sync start at: statehash<%x>, offset<%x>",
		payload.Statehash, payload.Offset.Data)

	req.Payload, err = proto.Marshal(payload)

	if err != nil {
		logger.Errorf("Error Unmarshal SyncState: %s", err)
		return nil
	}
	return req
}

func (h *StateMessageHandler) processResponse(syncMessage *pb.SyncMessage) (*pb.SyncOffset, error) {

	stateChunkArrayResp := &pb.SyncStateChunk{}
	err := proto.Unmarshal(syncMessage.Payload, stateChunkArrayResp)
	if err != nil {
		return nil, err
	}

	if len(syncMessage.FailedReason) > 0 {
		err = fmt.Errorf("Sync state failed! Reason: %s", syncMessage.FailedReason)
		return nil, err
	}

	err = h.partialSync.ApplyPartialSync(stateChunkArrayResp)
	if err != nil {
		return nil, err
	}

	if h.partialSync.IsCompleted() {
		localHash, _ := h.client.ledger.GetCurrentStateHash()
		logger.Infof("sync complete to state: <%x>", localHash)
		return nil, nil
	}

	return h.getOneOffset()

	// if stateChunkArrayResp.Roothash != nil && res == nil {
	// 	// all buckets synced, verify root hash
	// 	var localHash []byte
	// 	localHash, err = h.client.ledger.GetCurrentStateHash()
	// 	if err == nil {
	// 		logger.Infof("remote hash: <%x>", stateChunkArrayResp.Roothash)
	// 		logger.Infof("local hash:  <%x>", localHash)

	// 		if !bytes.Equal(localHash, stateChunkArrayResp.Roothash) {
	// 			err = fmt.Errorf("Sync state failed! Target root hash <%x>, local root hash <%x>",
	// 				stateChunkArrayResp.Roothash, localHash)
	// 		}
	// 	}
	// }

	//  return res, err
}
