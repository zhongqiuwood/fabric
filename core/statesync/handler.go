package statesync

import (
	"fmt"
	"sync"
	"time"

	"github.com/abchain/fabric/core/comm"

	"github.com/golang/protobuf/proto"
	"github.com/looplab/fsm"
	"github.com/spf13/viper"

	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	pb "github.com/abchain/fabric/protos"
)

type handler struct {
}

func newHandler() (*handler, error) {

}

//-----------------------------------------------------------------------------
//
// Sync StateHash
//
//-----------------------------------------------------------------------------
func (d *Handler) RequestStateHash(req *pb.SyncStateHashRequest) (<-chan *pb.SyncStateHash, error) {

	channalHandler := d.stateHashHandler

	channalHandler.Lock()
	defer channalHandler.Unlock()
	// Reset the handler
	channalHandler.reset()
	req.CorrelationId = channalHandler.correlationID

	err := d.submitMessage(pb.Message_SYNC_STATE_HASH_REQUEST, req)

	if err != nil {
		return nil, fmt.Errorf("Error submit Message_SYNC_STATE_HASH_REQUEST during RequestStateHash: %s", err)
	}

	return channalHandler.channel, nil
}

// BlockHeight request
func (d *Handler) beforeSyncStateHashRequest(e *fsm.Event) {
	peerLogger.Debugf("Received message: %s", e.Event)
	msg, ok := e.Args[0].(*pb.Message)
	if !ok {
		e.Cancel(fmt.Errorf("Received unexpected message type"))
		return
	}

	req := &pb.SyncStateHashRequest{}
	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		e.Cancel(fmt.Errorf("Error unmarshalling SyncStateHashRequest in beforeSyncStateHashRequest: %s", err))
		return
	}

	if req.Flag == 0 {
		go d.sendBlockHeight(req)
	} else if req.Flag == 1 {
		go d.sendStateHash(req)
	}
}

func (d *Handler) sendStateHash(req *pb.SyncStateHashRequest) {
	peerLogger.Debugf("Sending SyncStateHashRequest for CorrelationId: %d", req.CorrelationId)

	block, err := d.ledger.GetBlockByNumber(req.BlockNumber)

	if nil != err {
		peerLogger.Warningf("Could not retrieve block %d: %s",
			req.BlockNumber, err)
		return
	}

	resp := &pb.SyncStateHash{
		Request:   req,
		StateHash: block.StateHash,
	}

	d.submitMessage(pb.Message_SYNC_STATE_HASH, resp)
}

func (d *Handler) sendBlockHeight(req *pb.SyncStateHashRequest) {
	peerLogger.Debugf("Sending Block Height for CorrelationId: %d", req.CorrelationId)

	blockChainInfo, err := d.ledger.GetBlockchainInfo()

	if err != nil {
		peerLogger.Errorf("Error getting GetBlockchainInfo for CorrelationId %d: %s",
			req.CorrelationId, err)
		return
	}

	blockChainInfo.CurrentBlockHash = nil
	blockChainInfo.PreviousBlockHash = nil

	resp := &pb.SyncStateHash{
		Request:     req,
		BlockHeight: blockChainInfo.Height,
	}

	d.submitMessage(pb.Message_SYNC_STATE_HASH, resp)
}

func (d *Handler) submitMessage(t pb.Message_Type, msg proto.Message) error {

	msgBytes, err := proto.Marshal(msg)
	if err == nil {
		err = d.SendMessage(&pb.Message{Type: t, Payload: msgBytes})
	}
	if err != nil {
		peerLogger.Errorf("Failed to submit Message<%s><%+v> to <%s>, error: %s",
			t.String(), msg, d.ToPeerEndpoint, err)
	}
	return err
}

func (d *Handler) beforeSyncStateHash(e *fsm.Event) {
	peerLogger.Debugf("Received message: %s", e.Event)
	msg, ok := e.Args[0].(*pb.Message)
	if !ok {
		e.Cancel(fmt.Errorf("Received unexpected message type"))
		return
	}
	// Forward the received SyncStateDeltas to the channel
	response := &pb.SyncStateHash{}
	err := proto.Unmarshal(msg.Payload, response)
	if err != nil {
		e.Cancel(fmt.Errorf("Error unmarshalling SyncStateHash in beforeSyncStateHash: %s", err))
		return
	}
	peerLogger.Infof("Received SyncStateHash: <%+v>", response)

	// Send the message onto the channel, allow for the fact that channel may be closed on send attempt.
	defer func() {
		if x := recover(); x != nil {
			peerLogger.Errorf("Error sending SyncStateHash to channel: %v", x)
		}
	}()

	channalHandler := d.stateHashHandler
	// Use non-blocking send, will WARN and close channel if missed message.
	channalHandler.Lock()
	defer channalHandler.Unlock()
	if channalHandler.shouldHandle(response.Request.CorrelationId) {
		select {
		case channalHandler.channel <- response:
		default:
			peerLogger.Warningf("Did NOT send SyncStateHash message to channel for CorrelationId %d, "+
				"closing channel as the message has been discarded", response.Request.CorrelationId)
			channalHandler.reset()
		}
	} else {
		peerLogger.Warningf("Ignoring SyncStateHash message<%+v>, as current correlationId = %d",
			response,
			channalHandler.correlationID)
	}

}
