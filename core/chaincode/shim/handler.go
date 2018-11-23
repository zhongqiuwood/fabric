/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package shim

import (
	"errors"
	"fmt"
	"sync"

	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
)

// PeerChaincodeStream interface for stream between Peer and chaincode instance.
type PeerChaincodeStream interface {
	Send(*pb.ChaincodeMessage) error
	Recv() (*pb.ChaincodeMessage, error)
	CloseSend() error
}

func (handler *Handler) triggerNextState(msg *pb.ChaincodeMessage) {
	handler.serialSend(msg)
}

// Handler handler implementation for shim side of chaincode.
type Handler struct {
	sync.RWMutex
	//shim to peer grpc serializer. User only in serialSend
	serialLock sync.Mutex
	To         string
	ChatStream PeerChaincodeStream
	handlingF  func(msg *pb.ChaincodeMessage) error
	cc         Chaincode
	// Multiple queries (and one transaction) with different txids can be executing in parallel for this chaincode
	// responseChannel is the channel on which responses are communicated by the shim to the chaincodeStub.
	responseChannel map[string]chan *pb.ChaincodeMessage
	// Track which TXIDs are transactions and which are queries, to decide whether get/put state and invoke chaincode are allowed.
	isTransaction map[string]bool
}

func shorttxid(txid string) string {
	if len(txid) < 8 {
		return txid
	}
	return txid[0:8]
}

func (handler *Handler) serialSend(msg *pb.ChaincodeMessage) error {
	handler.serialLock.Lock()
	defer handler.serialLock.Unlock()
	if err := handler.ChatStream.Send(msg); err != nil {
		chaincodeLogger.Errorf("[%s]Error sending %s: %s", shorttxid(msg.Txid), msg.Type.String(), err)
		return fmt.Errorf("Error sending %s: %s", msg.Type.String(), err)
	}
	return nil
}

func (handler *Handler) createChannel(txid string) (chan *pb.ChaincodeMessage, error) {
	handler.Lock()
	defer handler.Unlock()
	if handler.responseChannel == nil {
		return nil, fmt.Errorf("[%s]Cannot create response channel", shorttxid(txid))
	}
	if handler.responseChannel[txid] != nil {
		return nil, fmt.Errorf("[%s]Channel exists", shorttxid(txid))
	}
	c := make(chan *pb.ChaincodeMessage)
	handler.responseChannel[txid] = c
	return c, nil
}

func (handler *Handler) sendChannel(msg *pb.ChaincodeMessage) error {
	handler.Lock()
	defer handler.Unlock()
	if handler.responseChannel == nil {
		return fmt.Errorf("[%s]Cannot send message response channel", shorttxid(msg.Txid))
	}
	if handler.responseChannel[msg.Txid] == nil {
		return fmt.Errorf("[%s]sendChannel does not exist", shorttxid(msg.Txid))
	}

	chaincodeLogger.Debugf("[%s]before send", shorttxid(msg.Txid))
	handler.responseChannel[msg.Txid] <- msg
	chaincodeLogger.Debugf("[%s]after send", shorttxid(msg.Txid))

	return nil
}

func (handler *Handler) receiveChannel(c chan *pb.ChaincodeMessage) (*pb.ChaincodeMessage, bool) {
	msg, val := <-c
	return msg, val
}

func (handler *Handler) deleteChannel(txid string) {
	handler.Lock()
	defer handler.Unlock()
	if handler.responseChannel != nil {
		delete(handler.responseChannel, txid)
	}
}

// markIsTransaction marks a TXID as a transaction or a query; true = transaction, false = query
func (handler *Handler) markIsTransaction(txid string, isTrans bool) bool {
	if handler.isTransaction == nil {
		return false
	}
	handler.Lock()
	defer handler.Unlock()
	handler.isTransaction[txid] = isTrans
	return true
}

func (handler *Handler) deleteIsTransaction(txid string) {
	handler.Lock()
	if handler.isTransaction != nil {
		delete(handler.isTransaction, txid)
	}
	handler.Unlock()
}

func (handler *Handler) checkIsTransaction(txid string) bool {
	handler.RLock()
	defer handler.RUnlock()
	return handler.isTransaction[txid]
}

// NewChaincodeHandler returns a new instance of the shim side handler.
func newChaincodeHandler(peerChatStream PeerChaincodeStream, chaincode Chaincode) *Handler {
	v := &Handler{
		ChatStream: peerChatStream,
		cc:         chaincode,
	}
	v.responseChannel = make(map[string]chan *pb.ChaincodeMessage)
	v.isTransaction = make(map[string]bool)
	v.handlingF = v.handleMessage_InitPhase
	return v
}

// handleInit handles request to initialize chaincode.
func (handler *Handler) handleInit(msg *pb.ChaincodeMessage) {
	// The defer followed by triggering a go routine dance is needed to ensure that the previous state transition
	// is completed before the next one is triggered. The previous state transition is deemed complete only when
	// the beforeInit function is exited. Interesting bug fix!!
	go func() {
		var nextStateMsg *pb.ChaincodeMessage

		defer func() {
			handler.triggerNextState(nextStateMsg)
		}()

		// Get the function and args from Payload
		input := &pb.ChaincodeInput{}
		unmarshalErr := proto.Unmarshal(msg.Payload, input)
		if unmarshalErr != nil {
			payload := []byte(unmarshalErr.Error())
			// Send ERROR message to chaincode support and change state
			chaincodeLogger.Debugf("[%s]Incorrect payload format. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
			nextStateMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid}
			return
		}

		// Mark as a transaction (allow put/del state)
		handler.markIsTransaction(msg.Txid, true)

		// Call chaincode's Run
		// Create the ChaincodeStub which the chaincode can use to callback
		stub := new(ChaincodeStub)
		stub.init(msg.Txid, msg.SecurityContext)
		stub.handler = handler
		function, params := getFunctionAndParams(stub)
		res, err := handler.cc.Init(stub, function, params)

		// delete isTransaction entry
		handler.deleteIsTransaction(msg.Txid)

		if err != nil {
			payload := []byte(err.Error())
			// Send ERROR message to chaincode support and change state
			chaincodeLogger.Errorf("[%s]Init failed. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
			nextStateMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid, ChaincodeEvent: stub.chaincodeEvent}
			return
		}

		// Send COMPLETED message to chaincode support and change state
		nextStateMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_COMPLETED, Payload: res, Txid: msg.Txid, ChaincodeEvent: stub.chaincodeEvent}
		chaincodeLogger.Debugf("[%s]Init succeeded. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_COMPLETED)
	}()
}

// handleTransaction Handles request to execute a transaction.
func (handler *Handler) handleTransaction(msg *pb.ChaincodeMessage) {
	// The defer followed by triggering a go routine dance is needed to ensure that the previous state transition
	// is completed before the next one is triggered. The previous state transition is deemed complete only when
	// the beforeInit function is exited. Interesting bug fix!!
	go func() {
		//better not be nil
		var nextStateMsg *pb.ChaincodeMessage

		defer func() {
			handler.triggerNextState(nextStateMsg)
		}()

		// Get the function and args from Payload
		input := &pb.ChaincodeInput{}
		unmarshalErr := proto.Unmarshal(msg.Payload, input)
		if unmarshalErr != nil {
			payload := []byte(unmarshalErr.Error())
			// Send ERROR message to chaincode support and change state
			chaincodeLogger.Debugf("[%s]Incorrect payload format. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
			nextStateMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid}
			return
		}

		// Mark as a transaction (allow put/del state)
		handler.markIsTransaction(msg.Txid, true)

		// Call chaincode's Run
		// Create the ChaincodeStub which the chaincode can use to callback
		stub := new(ChaincodeStub)
		stub.handler = handler
		stub.init(msg.Txid, msg.SecurityContext)
		function, params := getFunctionAndParams(stub)
		res, err := handler.cc.Invoke(stub, function, params)

		// delete isTransaction entry
		handler.deleteIsTransaction(msg.Txid)

		if err != nil {
			payload := []byte(err.Error())
			// Send ERROR message to chaincode support and change state
			chaincodeLogger.Errorf("[%s]Transaction execution failed. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
			nextStateMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid, ChaincodeEvent: stub.chaincodeEvent}
			return
		}

		// Send COMPLETED message to chaincode support and change state
		chaincodeLogger.Debugf("[%s]Transaction completed. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_COMPLETED)
		nextStateMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_COMPLETED, Payload: res, Txid: msg.Txid, ChaincodeEvent: stub.chaincodeEvent}
	}()
}

// handleQuery handles request to execute a query.
func (handler *Handler) handleQuery(msg *pb.ChaincodeMessage) {
	// Query does not transition state. It can happen anytime after Ready
	go func() {
		var serialSendMsg *pb.ChaincodeMessage

		defer func() {
			handler.triggerNextState(serialSendMsg)
		}()

		// Get the function and args from Payload
		input := &pb.ChaincodeInput{}
		unmarshalErr := proto.Unmarshal(msg.Payload, input)
		if unmarshalErr != nil {
			payload := []byte(unmarshalErr.Error())
			// Send ERROR message to chaincode support and change state
			chaincodeLogger.Debugf("[%s]Incorrect payload format. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_QUERY_ERROR)
			serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_QUERY_ERROR, Payload: payload, Txid: msg.Txid}
			return
		}

		// Mark as a query (do not allow put/del state)
		handler.markIsTransaction(msg.Txid, false)

		// Call chaincode's Query
		// Create the ChaincodeStub which the chaincode can use to callback
		stub := new(ChaincodeStub)
		stub.handler = handler
		stub.init(msg.Txid, msg.SecurityContext)
		function, params := getFunctionAndParams(stub)
		res, err := handler.cc.Query(stub, function, params)

		// delete isTransaction entry
		handler.deleteIsTransaction(msg.Txid)

		if err != nil {
			payload := []byte(err.Error())
			// Send ERROR message to chaincode support and change state
			chaincodeLogger.Errorf("[%s]Query execution failed. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_QUERY_ERROR)
			serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_QUERY_ERROR, Payload: payload, Txid: msg.Txid}
			return
		}

		// Send COMPLETED message to chaincode support
		chaincodeLogger.Debugf("[%s]Query completed. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_QUERY_COMPLETED)
		serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_QUERY_COMPLETED, Payload: res, Txid: msg.Txid}
	}()
}

// TODO: Implement method to get and put entire state map and not one key at a time?
// handleGetState communicates with the validator to fetch the requested state information from the ledger.
func (handler *Handler) handleGetState(key string, txid string) ([]byte, error) {
	// Create the channel on which to communicate the response from validating peer
	respChan, uniqueReqErr := handler.createChannel(txid)
	if uniqueReqErr != nil {
		chaincodeLogger.Debug("[%s]GS:Another state request pending for this Txid. Cannot process.", shorttxid(txid))
		return nil, uniqueReqErr
	}

	defer handler.deleteChannel(txid)

	// Send GET_STATE message to validator chaincode support
	payload := []byte(key)
	msg := &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_GET_STATE, Payload: payload, Txid: txid}
	chaincodeLogger.Debugf("[%s]Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_GET_STATE)
	if err := handler.serialSend(msg); err != nil {
		chaincodeLogger.Errorf("[%s]error sending GET_STATE %s", shorttxid(txid), err)
		return nil, errors.New("could not send msg")
	}

	// Wait on responseChannel for response
	responseMsg, ok := handler.receiveChannel(respChan)
	if !ok {
		chaincodeLogger.Errorf("[%s]Received unexpected message type", shorttxid(responseMsg.Txid))
		return nil, errors.New("Received unexpected message type")
	}

	if responseMsg.Type.String() == pb.ChaincodeMessage_RESPONSE.String() {
		// Success response
		chaincodeLogger.Debugf("[%s]GetState received payload %s", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_RESPONSE)
		return responseMsg.Payload, nil
	}
	if responseMsg.Type.String() == pb.ChaincodeMessage_ERROR.String() {
		// Error response
		chaincodeLogger.Errorf("[%s]GetState received error %s", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_ERROR)
		return nil, errors.New(string(responseMsg.Payload[:]))
	}

	// Incorrect chaincode message received
	chaincodeLogger.Errorf("[%s]Incorrect chaincode message %s received. Expecting %s or %s", shorttxid(responseMsg.Txid), responseMsg.Type, pb.ChaincodeMessage_RESPONSE, pb.ChaincodeMessage_ERROR)
	return nil, errors.New("Incorrect chaincode message received")
}

// handlePutState communicates with the validator to put state information into the ledger.
func (handler *Handler) handlePutState(key string, value []byte, txid string) error {
	// Check if this is a transaction
	isTransaction := handler.checkIsTransaction(txid)
	chaincodeLogger.Debugf("[%s]Inside putstate, isTransaction = %t", shorttxid(txid), isTransaction)
	if !isTransaction {
		return errors.New("Cannot put state in query context")
	}

	payload := &pb.PutStateInfo{Key: key, Value: value}
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return errors.New("Failed to process put state request")
	}

	// Create the channel on which to communicate the response from validating peer
	respChan, uniqueReqErr := handler.createChannel(txid)
	if uniqueReqErr != nil {
		chaincodeLogger.Errorf("[%s]PS:Another state request pending for this Txid. Cannot process.", shorttxid(txid))
		return uniqueReqErr
	}

	defer handler.deleteChannel(txid)

	// Send PUT_STATE message to validator chaincode support
	msg := &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_PUT_STATE, Payload: payloadBytes, Txid: txid}
	chaincodeLogger.Debugf("[%s]Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_PUT_STATE)
	if err = handler.serialSend(msg); err != nil {
		chaincodeLogger.Errorf("[%s]error sending PUT_STATE %s", msg.Txid, err)
		return errors.New("could not send msg")
	}

	// Wait on responseChannel for response
	responseMsg, ok := handler.receiveChannel(respChan)
	if !ok {
		chaincodeLogger.Errorf("[%s]Received unexpected message type", msg.Txid)
		return errors.New("Received unexpected message type")
	}

	if responseMsg.Type.String() == pb.ChaincodeMessage_RESPONSE.String() {
		// Success response
		chaincodeLogger.Debugf("[%s]Received %s. Successfully updated state", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_RESPONSE)
		return nil
	}

	if responseMsg.Type.String() == pb.ChaincodeMessage_ERROR.String() {
		// Error response
		chaincodeLogger.Errorf("[%s]Received %s. Payload: %s", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_ERROR, responseMsg.Payload)
		return errors.New(string(responseMsg.Payload[:]))
	}

	// Incorrect chaincode message received
	chaincodeLogger.Errorf("[%s]Incorrect chaincode message %s received. Expecting %s or %s", shorttxid(responseMsg.Txid), responseMsg.Type, pb.ChaincodeMessage_RESPONSE, pb.ChaincodeMessage_ERROR)
	return errors.New("Incorrect chaincode message received")
}

// handleDelState communicates with the validator to delete a key from the state in the ledger.
func (handler *Handler) handleDelState(key string, txid string) error {
	// Check if this is a transaction
	if !handler.checkIsTransaction(txid) {
		return errors.New("Cannot del state in query context")
	}

	// Create the channel on which to communicate the response from validating peer
	respChan, uniqueReqErr := handler.createChannel(txid)
	if uniqueReqErr != nil {
		chaincodeLogger.Errorf("[%s]DS:Another state request pending for this Txid. Cannot process create createChannel.", shorttxid(txid))
		return uniqueReqErr
	}

	defer handler.deleteChannel(txid)

	// Send DEL_STATE message to validator chaincode support
	payload := []byte(key)
	msg := &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_DEL_STATE, Payload: payload, Txid: txid}
	chaincodeLogger.Debugf("[%s]Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_DEL_STATE)
	if err := handler.serialSend(msg); err != nil {
		chaincodeLogger.Errorf("[%s]error sending DEL_STATE %s", shorttxid(msg.Txid), pb.ChaincodeMessage_DEL_STATE)
		return errors.New("could not send msg")
	}

	// Wait on responseChannel for response
	responseMsg, ok := handler.receiveChannel(respChan)
	if !ok {
		chaincodeLogger.Errorf("[%s]Received unexpected message type", shorttxid(msg.Txid))
		return errors.New("Received unexpected message type")
	}

	if responseMsg.Type.String() == pb.ChaincodeMessage_RESPONSE.String() {
		// Success response
		chaincodeLogger.Debugf("[%s]Received %s. Successfully deleted state", msg.Txid, pb.ChaincodeMessage_RESPONSE)
		return nil
	}
	if responseMsg.Type.String() == pb.ChaincodeMessage_ERROR.String() {
		// Error response
		chaincodeLogger.Errorf("[%s]Received %s. Payload: %s", msg.Txid, pb.ChaincodeMessage_ERROR, responseMsg.Payload)
		return errors.New(string(responseMsg.Payload[:]))
	}

	// Incorrect chaincode message received
	chaincodeLogger.Errorf("[%s]Incorrect chaincode message %s received. Expecting %s or %s", shorttxid(responseMsg.Txid), responseMsg.Type, pb.ChaincodeMessage_RESPONSE, pb.ChaincodeMessage_ERROR)
	return errors.New("Incorrect chaincode message received")
}

func (handler *Handler) handleRangeQueryState(startKey, endKey string, txid string) (*pb.RangeQueryStateResponse, error) {
	// Create the channel on which to communicate the response from validating peer
	respChan, uniqueReqErr := handler.createChannel(txid)
	if uniqueReqErr != nil {
		chaincodeLogger.Debugf("[%s]RNQ:Another state request pending for this Txid. Cannot process.", shorttxid(txid))
		return nil, uniqueReqErr
	}

	defer handler.deleteChannel(txid)

	// Send RANGE_QUERY_STATE message to validator chaincode support
	payload := &pb.RangeQueryState{StartKey: startKey, EndKey: endKey}
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, errors.New("Failed to process range query state request")
	}
	msg := &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_RANGE_QUERY_STATE, Payload: payloadBytes, Txid: txid}
	chaincodeLogger.Debugf("[%s]Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_RANGE_QUERY_STATE)
	if err = handler.serialSend(msg); err != nil {
		chaincodeLogger.Errorf("[%s]error sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_RANGE_QUERY_STATE)
		return nil, errors.New("could not send msg")
	}

	// Wait on responseChannel for response
	responseMsg, ok := handler.receiveChannel(respChan)
	if !ok {
		chaincodeLogger.Errorf("[%s]Received unexpected message type", txid)
		return nil, errors.New("Received unexpected message type")
	}

	if responseMsg.Type.String() == pb.ChaincodeMessage_RESPONSE.String() {
		// Success response
		chaincodeLogger.Debugf("[%s]Received %s. Successfully got range", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_RESPONSE)

		rangeQueryResponse := &pb.RangeQueryStateResponse{}
		unmarshalErr := proto.Unmarshal(responseMsg.Payload, rangeQueryResponse)
		if unmarshalErr != nil {
			chaincodeLogger.Errorf("[%s]unmarshall error", shorttxid(responseMsg.Txid))
			return nil, errors.New("Error unmarshalling RangeQueryStateResponse.")
		}

		return rangeQueryResponse, nil
	}
	if responseMsg.Type.String() == pb.ChaincodeMessage_ERROR.String() {
		// Error response
		chaincodeLogger.Errorf("[%s]Received %s", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_ERROR)
		return nil, errors.New(string(responseMsg.Payload[:]))
	}

	// Incorrect chaincode message received
	chaincodeLogger.Errorf("Incorrect chaincode message %s recieved. Expecting %s or %s", responseMsg.Type, pb.ChaincodeMessage_RESPONSE, pb.ChaincodeMessage_ERROR)
	return nil, errors.New("Incorrect chaincode message received")
}

func (handler *Handler) handleRangeQueryStateNext(id, txid string) (*pb.RangeQueryStateResponse, error) {
	// Create the channel on which to communicate the response from validating peer
	respChan, uniqueReqErr := handler.createChannel(txid)
	if uniqueReqErr != nil {
		chaincodeLogger.Debugf("[%s]RNQN:Another state request pending for this Txid. Cannot process.", shorttxid(txid))
		return nil, uniqueReqErr
	}

	defer handler.deleteChannel(txid)

	// Send RANGE_QUERY_STATE_NEXT message to validator chaincode support
	payload := &pb.RangeQueryStateNext{ID: id}
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, errors.New("Failed to process range query state next request")
	}
	msg := &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_RANGE_QUERY_STATE_NEXT, Payload: payloadBytes, Txid: txid}
	chaincodeLogger.Debugf("[%s]Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_RANGE_QUERY_STATE_NEXT)
	if err = handler.serialSend(msg); err != nil {
		chaincodeLogger.Errorf("[%s]error sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_RANGE_QUERY_STATE_NEXT)
		return nil, errors.New("could not send msg")
	}

	// Wait on responseChannel for response
	responseMsg, ok := handler.receiveChannel(respChan)
	if !ok {
		chaincodeLogger.Errorf("[%s]Received unexpected message type", txid)
		return nil, errors.New("Received unexpected message type")
	}

	if responseMsg.Type.String() == pb.ChaincodeMessage_RESPONSE.String() {
		// Success response
		chaincodeLogger.Debugf("[%s]Received %s. Successfully got range", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_RESPONSE)

		rangeQueryResponse := &pb.RangeQueryStateResponse{}
		unmarshalErr := proto.Unmarshal(responseMsg.Payload, rangeQueryResponse)
		if unmarshalErr != nil {
			chaincodeLogger.Errorf("[%s]unmarshall error", shorttxid(responseMsg.Txid))
			return nil, errors.New("Error unmarshalling RangeQueryStateResponse.")
		}

		return rangeQueryResponse, nil
	}
	if responseMsg.Type.String() == pb.ChaincodeMessage_ERROR.String() {
		// Error response
		chaincodeLogger.Errorf("[%s]Received %s", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_ERROR)
		return nil, errors.New(string(responseMsg.Payload[:]))
	}

	// Incorrect chaincode message received
	chaincodeLogger.Errorf("Incorrect chaincode message %s recieved. Expecting %s or %s", responseMsg.Type, pb.ChaincodeMessage_RESPONSE, pb.ChaincodeMessage_ERROR)
	return nil, errors.New("Incorrect chaincode message received")
}

func (handler *Handler) handleRangeQueryStateClose(id, txid string) (*pb.RangeQueryStateResponse, error) {
	// Create the channel on which to communicate the response from validating peer
	respChan, uniqueReqErr := handler.createChannel(txid)
	if uniqueReqErr != nil {
		chaincodeLogger.Debugf("[%s]RNQC:Another state request pending for this Txid. Cannot process.", shorttxid(txid))
		return nil, uniqueReqErr
	}

	defer handler.deleteChannel(txid)

	// Send RANGE_QUERY_STATE_CLOSE message to validator chaincode support
	payload := &pb.RangeQueryStateClose{ID: id}
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, errors.New("Failed to process range query state close request")
	}
	msg := &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_RANGE_QUERY_STATE_CLOSE, Payload: payloadBytes, Txid: txid}
	chaincodeLogger.Debugf("[%s]Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_RANGE_QUERY_STATE_CLOSE)
	if err = handler.serialSend(msg); err != nil {
		chaincodeLogger.Errorf("[%s]error sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_RANGE_QUERY_STATE_CLOSE)
		return nil, errors.New("could not send msg")
	}

	// Wait on responseChannel for response
	responseMsg, ok := handler.receiveChannel(respChan)
	if !ok {
		chaincodeLogger.Errorf("[%s]Received unexpected message type", txid)
		return nil, errors.New("Received unexpected message type")
	}

	if responseMsg.Type.String() == pb.ChaincodeMessage_RESPONSE.String() {
		// Success response
		chaincodeLogger.Debugf("[%s]Received %s. Successfully got range", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_RESPONSE)

		rangeQueryResponse := &pb.RangeQueryStateResponse{}
		unmarshalErr := proto.Unmarshal(responseMsg.Payload, rangeQueryResponse)
		if unmarshalErr != nil {
			chaincodeLogger.Errorf("[%s]unmarshall error", shorttxid(responseMsg.Txid))
			return nil, errors.New("Error unmarshalling RangeQueryStateResponse.")
		}

		return rangeQueryResponse, nil
	}
	if responseMsg.Type.String() == pb.ChaincodeMessage_ERROR.String() {
		// Error response
		chaincodeLogger.Errorf("[%s]Received %s", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_ERROR)
		return nil, errors.New(string(responseMsg.Payload[:]))
	}

	// Incorrect chaincode message received
	chaincodeLogger.Errorf("Incorrect chaincode message %s recieved. Expecting %s or %s", responseMsg.Type, pb.ChaincodeMessage_RESPONSE, pb.ChaincodeMessage_ERROR)
	return nil, errors.New("Incorrect chaincode message received")
}

// handleInvokeChaincode communicates with the validator to invoke another chaincode.
func (handler *Handler) handleInvokeChaincode(chaincodeName string, args [][]byte, txid string) ([]byte, error) {
	// Check if this is a transaction
	if !handler.checkIsTransaction(txid) {
		return nil, errors.New("Cannot invoke chaincode in query context")
	}

	chaincodeID := &pb.ChaincodeID{Name: chaincodeName}
	input := &pb.ChaincodeInput{Args: args}
	payload := &pb.ChaincodeSpec{ChaincodeID: chaincodeID, CtorMsg: input}
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, errors.New("Failed to process invoke chaincode request")
	}

	// Create the channel on which to communicate the response from validating peer
	respChan, uniqueReqErr := handler.createChannel(txid)
	if uniqueReqErr != nil {
		chaincodeLogger.Errorf("[%s]Another request pending for this Txid. Cannot process.", txid)
		return nil, uniqueReqErr
	}

	defer handler.deleteChannel(txid)

	// Send INVOKE_CHAINCODE message to validator chaincode support
	msg := &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_INVOKE_CHAINCODE, Payload: payloadBytes, Txid: txid}
	chaincodeLogger.Debugf("[%s]Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_INVOKE_CHAINCODE)
	if err = handler.serialSend(msg); err != nil {
		chaincodeLogger.Errorf("[%s]error sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_INVOKE_CHAINCODE)
		return nil, errors.New("could not send msg")
	}

	// Wait on responseChannel for response
	responseMsg, ok := handler.receiveChannel(respChan)
	if !ok {
		chaincodeLogger.Errorf("[%s]Received unexpected message type", shorttxid(msg.Txid))
		return nil, errors.New("Received unexpected message type")
	}

	if responseMsg.Type.String() == pb.ChaincodeMessage_RESPONSE.String() {
		// Success response
		chaincodeLogger.Debugf("[%s]Received %s. Successfully invoked chaincode", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_RESPONSE)
		respMsg := &pb.ChaincodeMessage{}
		if err := proto.Unmarshal(responseMsg.Payload, respMsg); err != nil {
			chaincodeLogger.Errorf("[%s]Error unmarshaling called chaincode response: %s", shorttxid(responseMsg.Txid), err)
			return nil, err
		}
		if respMsg.Type == pb.ChaincodeMessage_COMPLETED {
			// Success response
			chaincodeLogger.Debugf("[%s]Received %s. Successfully invoed chaincode", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_RESPONSE)
			return respMsg.Payload, nil
		}
		chaincodeLogger.Errorf("[%s]Received %s. Error from chaincode", shorttxid(responseMsg.Txid), respMsg.Type.String())
		return nil, errors.New(string(respMsg.Payload[:]))
	}
	if responseMsg.Type.String() == pb.ChaincodeMessage_ERROR.String() {
		// Error response
		chaincodeLogger.Errorf("[%s]Received %s.", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_ERROR)
		return nil, errors.New(string(responseMsg.Payload[:]))
	}

	// Incorrect chaincode message received
	chaincodeLogger.Debugf("[%s]Incorrect chaincode message %s received. Expecting %s or %s", shorttxid(responseMsg.Txid), responseMsg.Type, pb.ChaincodeMessage_RESPONSE, pb.ChaincodeMessage_ERROR)
	return nil, errors.New("Incorrect chaincode message received")
}

// handleQueryChaincode communicates with the validator to query another chaincode.
func (handler *Handler) handleQueryChaincode(chaincodeName string, args [][]byte, txid string) ([]byte, error) {
	chaincodeID := &pb.ChaincodeID{Name: chaincodeName}
	input := &pb.ChaincodeInput{Args: args}
	payload := &pb.ChaincodeSpec{ChaincodeID: chaincodeID, CtorMsg: input}
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, errors.New("Failed to process query chaincode request")
	}

	// Create the channel on which to communicate the response from validating peer
	respChan, uniqueReqErr := handler.createChannel(txid)
	if uniqueReqErr != nil {
		chaincodeLogger.Debug("Another request pending for this Txid. Cannot process.")
		return nil, uniqueReqErr
	}

	defer handler.deleteChannel(txid)

	// Send INVOKE_QUERY message to validator chaincode support
	msg := &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_INVOKE_QUERY, Payload: payloadBytes, Txid: txid}
	chaincodeLogger.Debugf("[%s]Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_INVOKE_QUERY)
	if err = handler.serialSend(msg); err != nil {
		chaincodeLogger.Errorf("[%s]error sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_INVOKE_QUERY)
		return nil, errors.New("could not send msg")
	}

	// Wait on responseChannel for response
	responseMsg, ok := handler.receiveChannel(respChan)
	if !ok {
		chaincodeLogger.Errorf("[%s]Received unexpected message type", shorttxid(msg.Txid))
		return nil, errors.New("Received unexpected message type")
	}

	if responseMsg.Type.String() == pb.ChaincodeMessage_RESPONSE.String() {
		respMsg := &pb.ChaincodeMessage{}
		if err := proto.Unmarshal(responseMsg.Payload, respMsg); err != nil {
			chaincodeLogger.Errorf("[%s]Error unmarshaling called chaincode responseP: %s", shorttxid(responseMsg.Txid), err)
			return nil, err
		}
		if respMsg.Type == pb.ChaincodeMessage_QUERY_COMPLETED {
			// Success response
			chaincodeLogger.Debugf("[%s]Received %s. Successfully queried chaincode", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_RESPONSE)
			return respMsg.Payload, nil
		}
		chaincodeLogger.Errorf("[%s]Error from chaincode: %s", shorttxid(responseMsg.Txid), string(respMsg.Payload[:]))
		return nil, errors.New(string(respMsg.Payload[:]))
	}
	if responseMsg.Type.String() == pb.ChaincodeMessage_ERROR.String() {
		// Error response
		chaincodeLogger.Errorf("[%s]Received %s.", shorttxid(responseMsg.Txid), pb.ChaincodeMessage_ERROR)
		return nil, errors.New(string(responseMsg.Payload[:]))
	}

	// Incorrect chaincode message received
	chaincodeLogger.Errorf("[%s]Incorrect chaincode message %s recieved. Expecting %s or %s", shorttxid(responseMsg.Txid), responseMsg.Type, pb.ChaincodeMessage_RESPONSE, pb.ChaincodeMessage_ERROR)
	return nil, errors.New("Incorrect chaincode message received")
}

func (handler *Handler) handleMessage_InitPhase(msg *pb.ChaincodeMessage) error {

	switch msg.Type {
	case pb.ChaincodeMessage_REGISTERED: //omit it
	case pb.ChaincodeMessage_INIT:
		handler.handleInit(msg)
		fallthrough
	case pb.ChaincodeMessage_READY:
		//handler can come into tx phase simply after init, no matter it success or not
		//(it was fabric's response to considering the resp and dispatch more request)
		handler.handlingF = handler.handleMessage_OpPhase
	default:
		return fmt.Errorf("[%s]Received %s which is not allowed", shorttxid(msg.Txid), msg.Type)
	}

	return nil
}

func (handler *Handler) handleMessage_OpPhase(msg *pb.ChaincodeMessage) error {
	switch msg.Type {
	case pb.ChaincodeMessage_TRANSACTION:
		handler.handleTransaction(msg)
	case pb.ChaincodeMessage_QUERY:
		handler.handleQuery(msg)
	default:
		return fmt.Errorf("[%s]Received %s which is not allowed", shorttxid(msg.Txid), msg.Type)
	}

	return nil
}

// handleMessage message handles loop for shim side of chaincode/validator stream.
func (handler *Handler) handleMessage(msg *pb.ChaincodeMessage) error {

	switch msg.Type {
	case pb.ChaincodeMessage_KEEPALIVE:
		// Received a keep alive message, we don't do anything with it for now
		// and it does not touch the state machine, just response the original message
		return handler.serialSend(msg)
	case pb.ChaincodeMessage_RESPONSE, pb.ChaincodeMessage_ERROR:
		if err := handler.sendChannel(msg); err != nil {
			chaincodeLogger.Errorf("[%s]error sending %s :%s", shorttxid(msg.Txid), msg.Type, err)
			return err
		} else {
			chaincodeLogger.Debugf("[%s]Received %s, communicated", shorttxid(msg.Txid), msg.Type)
			return nil
		}
	default:
		return handler.handlingF(msg)
	}

	return nil
}

func getFunctionAndParams(stub ChaincodeStubInterface) (function string, params []string) {
	allargs := stub.GetStringArgs()
	function = ""
	params = []string{}
	if len(allargs) >= 1 {
		function = allargs[0]
		params = allargs[1:]
	}
	return
}
