package statesync

import (
	pb "github.com/abchain/fabric/protos"
	"github.com/looplab/fsm"
)

var syncPhase = []string{"synclocating", "syncdelta", "syncblock", "syncsnapshot"}


var enterGetBlock = "GetBlock"
var enterGetSnapshot = "GetSnapshot"
var enterGetDelta = "GetDelta"
var enterSyncBegin = "SyncBegin"
var enterSyncFinish = "SyncFinish"

func newFsmHandler(h *stateSyncHandler) *fsm.FSM {

	return fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: pb.SyncMsg_SYNC_STATE_NOTIFY.String(), Src: []string{"idle"}, Dst: "idle"},
			{Name: pb.SyncMsg_SYNC_STATE_OPT.String(),    Src: []string{"idle"}, Dst: "idle"},

			//serving phase
			{Name: pb.SyncMsg_SYNC_SESSION_START.String(),        Src: []string{"idle"},  Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_QUERY.String(),        Src: []string{"serve"}, Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_GET_BLOCKS.String(),   Src: []string{"serve"}, Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_GET_SNAPSHOT.String(), Src: []string{"serve"}, Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_GET_DELTAS.String(),   Src: []string{"serve"}, Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_END.String(),          Src: []string{"serve"}, Dst: "idle"},

			//client phase
			{Name: pb.SyncMsg_SYNC_SESSION_START_ACK.String(), Src: []string{"synchandshake"}, Dst: "synclocating"},
			{Name: pb.SyncMsg_SYNC_SESSION_RESPONSE.String(),  Src: []string{"synclocating"},  Dst: "synclocating"},
			{Name: pb.SyncMsg_SYNC_SESSION_BLOCKS.String(),    Src: []string{"syncblock"},     Dst: "syncblock"},
			{Name: pb.SyncMsg_SYNC_SESSION_SNAPSHOT.String(),  Src: []string{"syncsnapshot"},  Dst: "syncsnapshot"},
			{Name: pb.SyncMsg_SYNC_SESSION_DELTAS.String(),    Src: []string{"syncdelta"},     Dst: "syncdelta"},
			{Name: enterSyncBegin,   Src: []string{"idle"}, Dst: "synchandshake"},
			{Name: enterGetBlock,    Src: syncPhase,        Dst: "syncblock"},
			{Name: enterGetSnapshot, Src: syncPhase,        Dst: "syncsnapshot"},
			{Name: enterGetDelta,    Src: syncPhase,        Dst: "syncdelta"},
			{Name: enterSyncFinish,  Src: syncPhase,        Dst: "idle"},

		},
		fsm.Callbacks{

			// server
			"before_" + pb.SyncMsg_SYNC_SESSION_START.String():        func(e *fsm.Event) { h.server.beforeSyncStart(e) },
			"before_" + pb.SyncMsg_SYNC_SESSION_QUERY.String():        func(e *fsm.Event) { h.server.beforeQuery(e) },
			"before_" + pb.SyncMsg_SYNC_SESSION_GET_BLOCKS.String():   func(e *fsm.Event) { h.server.beforeGetBlocks(e) },
			"before_" + pb.SyncMsg_SYNC_SESSION_GET_DELTAS.String():   func(e *fsm.Event) { h.server.beforeGetDeltas(e) },
			"before_" + pb.SyncMsg_SYNC_SESSION_GET_SNAPSHOT.String(): func(e *fsm.Event) { h.server.beforeGetSnapshot(e) },
			"before_" + pb.SyncMsg_SYNC_SESSION_END.String():          func(e *fsm.Event) { h.server.beforeSyncEnd(e) },

			// client
			"after_" + pb.SyncMsg_SYNC_SESSION_START_ACK.String(): func(e *fsm.Event) { h.client.afterSyncStartResponse(e) },
			"after_" + pb.SyncMsg_SYNC_SESSION_RESPONSE.String():  func(e *fsm.Event) { h.client.afterQueryResponse(e) },
			"after_" + pb.SyncMsg_SYNC_SESSION_BLOCKS.String():    func(e *fsm.Event) { h.client.afterSyncBlocks(e) },
			"after_" + pb.SyncMsg_SYNC_SESSION_DELTAS.String():    func(e *fsm.Event) { h.client.afterSyncStateDeltas(e) },
			"after_" + pb.SyncMsg_SYNC_SESSION_SNAPSHOT.String():  func(e *fsm.Event) { h.client.afterSyncStateSnapshot(e) },
			"leave_synclocating":                                  func(e *fsm.Event) { h.client.leaveSyncLocating(e) },
			"leave_syncblock":                                     func(e *fsm.Event) { h.client.leaveSyncBlocks(e) },
			"leave_syncsnapshot":                                  func(e *fsm.Event) { h.client.leaveSyncStateSnapshot(e) },
			"leave_syncdelta":                                     func(e *fsm.Event) { h.client.leaveSyncStateDeltas(e) },
		},
	)

}

//-----------------------------------------------------------------------------
//
// Sync StateHash
//
//-----------------------------------------------------------------------------
// func (d *Handler) RequestStateHash(req *pb.SyncStateHashRequest) (<-chan *pb.SyncStateHash, error) {

// 	channalHandler := d.stateHashHandler

// 	channalHandler.Lock()
// 	defer channalHandler.Unlock()
// 	// Reset the handler
// 	channalHandler.reset()
// 	req.CorrelationId = channalHandler.correlationID

// 	err := d.submitMessage(pb.Message_SYNC_STATE_HASH_REQUEST, req)

// 	if err != nil {
// 		return nil, fmt.Errorf("Error submit Message_SYNC_STATE_HASH_REQUEST during RequestStateHash: %s", err)
// 	}

// 	return channalHandler.channel, nil
// }

// // BlockHeight request
// func (d *Handler) beforeSyncStateHashRequest(e *fsm.Event) {
// 	peerLogger.Debugf("Received message: %s", e.Event)
// 	msg, ok := e.Args[0].(*pb.Message)
// 	if !ok {
// 		e.Cancel(fmt.Errorf("Received unexpected message type"))
// 		return
// 	}

// 	req := &pb.SyncStateHashRequest{}
// 	err := proto.Unmarshal(msg.Payload, req)
// 	if err != nil {
// 		e.Cancel(fmt.Errorf("Error unmarshalling SyncStateHashRequest in beforeSyncStateHashRequest: %s", err))
// 		return
// 	}

// 	if req.Flag == 0 {
// 		go d.sendBlockHeight(req)
// 	} else if req.Flag == 1 {
// 		go d.sendStateHash(req)
// 	}
// }

// func (d *Handler) sendStateHash(req *pb.SyncStateHashRequest) {
// 	peerLogger.Debugf("Sending SyncStateHashRequest for CorrelationId: %d", req.CorrelationId)

// 	block, err := d.ledger.GetBlockByNumber(req.BlockNumber)

// 	if nil != err {
// 		peerLogger.Warningf("Could not retrieve block %d: %s",
// 			req.BlockNumber, err)
// 		return
// 	}

// 	resp := &pb.SyncStateHash{
// 		Request:   req,
// 		StateHash: block.StateHash,
// 	}

// 	d.submitMessage(pb.Message_SYNC_STATE_HASH, resp)
// }

// func (d *Handler) sendBlockHeight(req *pb.SyncStateHashRequest) {
// 	peerLogger.Debugf("Sending Block Height for CorrelationId: %d", req.CorrelationId)

// 	blockChainInfo, err := d.ledger.GetBlockchainInfo()

// 	if err != nil {
// 		peerLogger.Errorf("Error getting GetBlockchainInfo for CorrelationId %d: %s",
// 			req.CorrelationId, err)
// 		return
// 	}

// 	blockChainInfo.CurrentBlockHash = nil
// 	blockChainInfo.PreviousBlockHash = nil

// 	resp := &pb.SyncStateHash{
// 		Request:     req,
// 		BlockHeight: blockChainInfo.Height,
// 	}

// 	d.submitMessage(pb.Message_SYNC_STATE_HASH, resp)
// }

// func (d *Handler) submitMessage(t pb.Message_Type, msg proto.Message) error {

// 	msgBytes, err := proto.Marshal(msg)
// 	if err == nil {
// 		err = d.SendMessage(&pb.Message{Type: t, Payload: msgBytes})
// 	}
// 	if err != nil {
// 		peerLogger.Errorf("Failed to submit Message<%s><%+v> to <%s>, error: %s",
// 			t.String(), msg, d.ToPeerEndpoint, err)
// 	}
// 	return err
// }

// func (d *Handler) beforeSyncStateHash(e *fsm.Event) {
// 	peerLogger.Debugf("Received message: %s", e.Event)
// 	msg, ok := e.Args[0].(*pb.Message)
// 	if !ok {
// 		e.Cancel(fmt.Errorf("Received unexpected message type"))
// 		return
// 	}
// 	// Forward the received SyncStateDeltas to the channel
// 	response := &pb.SyncStateHash{}
// 	err := proto.Unmarshal(msg.Payload, response)
// 	if err != nil {
// 		e.Cancel(fmt.Errorf("Error unmarshalling SyncStateHash in beforeSyncStateHash: %s", err))
// 		return
// 	}
// 	peerLogger.Infof("Received SyncStateHash: <%+v>", response)

// 	// Send the message onto the channel, allow for the fact that channel may be closed on send attempt.
// 	defer func() {
// 		if x := recover(); x != nil {
// 			peerLogger.Errorf("Error sending SyncStateHash to channel: %v", x)
// 		}
// 	}()

// 	channalHandler := d.stateHashHandler
// 	// Use non-blocking send, will WARN and close channel if missed message.
// 	channalHandler.Lock()
// 	defer channalHandler.Unlock()
// 	if channalHandler.shouldHandle(response.Request.CorrelationId) {
// 		select {
// 		case channalHandler.channel <- response:
// 		default:
// 			peerLogger.Warningf("Did NOT send SyncStateHash message to channel for CorrelationId %d, "+
// 				"closing channel as the message has been discarded", response.Request.CorrelationId)
// 			channalHandler.reset()
// 		}
// 	} else {
// 		peerLogger.Warningf("Ignoring SyncStateHash message<%+v>, as current correlationId = %d",
// 			response,
// 			channalHandler.correlationID)
// 	}

// }
