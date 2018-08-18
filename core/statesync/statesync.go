package statesync

import (
	"fmt"
	_ "github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/peer"
	"github.com/abchain/fabric/core/statesync/stub"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"github.com/looplab/fsm"
	"github.com/op/go-logging"
	_ "github.com/spf13/viper"
	"golang.org/x/net/context"
	"sync"
)

var logger = logging.MustGetLogger("statesync")

func init() {
	stub.DefaultFactory = newStateSyncHandler
}

var stateSyncCore *StateSync
var syncerErr error
var once sync.Once


func NewStateSync(p peer.Peer) {

	once.Do(func() {
		stateSyncCore, syncerErr = GetNewStateSync(p)
		if syncerErr != nil {
			logger.Error("Create new state syncer fail:", syncerErr)
		}
	})
}

func GetNewStateSync(p peer.Peer) (*StateSync, error) {

	stub := p.GetStreamStub("sync")
	if stub == nil {
		return nil, fmt.Errorf("peer have no sync streamstub")
	}

	return &StateSync{StreamStub: stub}, nil
}

// gives a reference to a singleton
func GetStateSync() (*StateSync, error) {
	return stateSyncCore, syncerErr
}


type StateSync struct {
	*pb.StreamStub
	sync.RWMutex
	curCorrrelation uint64
	curTask         context.Context
}

type ErrInProcess struct {
	error
}

type ErrHandlerFatal struct {
	error
}

//main entry for statesync: it is synchronous and NOT re-entriable, but you can call IsBusy
//at another thread and check its status
func (s *StateSync) SyncToState(ctx context.Context, targetState []byte, opt *syncOpt, peer []*pb.PeerID) error {

	var err error
	s.Lock()
	if s.curTask != nil {
		s.Unlock()
		return &ErrInProcess{fmt.Errorf("Another task is running")}
	}

	s.curTask = ctx
	s.curCorrrelation++
	s.Unlock()
	handlers := s.PickHandlers(peer)

	for _, handler := range handlers {
		err = handler.ExecuteSync(targetState)
		break
	}

	defer func() {
		s.Lock()
		s.curTask = nil
		s.Unlock()
	}()

	return err
}

//if busy, return current correlation Id, els return 0
func (s *StateSync) IsBusy() uint64 {

	s.RLock()
	defer s.RUnlock()

	if s.curTask == nil {
		return uint64(0)
	} else {
		return s.curCorrrelation
	}
}

type stateSyncHandler struct {
	//localPeerId   *pb.PeerID
	remotePeerId  *pb.PeerID
	fsmHandler *fsm.FSM
	server  *stateServer
	client  *syncer
	stream  *pb.StreamHandler
	sync.Once
}

func newStateSyncHandler(remoterId *pb.PeerID) pb.StreamHandlerImpl {
	logger.Debug("create handler for peer", remoterId)
	h := &stateSyncHandler{
		//localPeerId: localId,
		remotePeerId: remoterId,
	}

	h.fsmHandler = newFsmHandler(h)

	return h
}

func (h *stateSyncHandler) leaveIdle(e *fsm.Event) {

	stateUpdate := "leaveIdle"
	h.dumpStateUpdate(stateUpdate)
}

func (h *stateSyncHandler) enterIdle(e *fsm.Event) {

	stateUpdate := "enterIdle"
	h.dumpStateUpdate(stateUpdate)
}

func (h *stateSyncHandler) dumpStateUpdate(stateUpdate string) {
	logger.Debugf("StateSyncHandler Syncing state update: %s. correlationId<%d>, remotePeerId<%s>",
		stateUpdate, 0, h.remotePeerIdName())
}

func (h *stateSyncHandler) remotePeerIdName() string {
	return h.remotePeerId.Name
}

//this should be call with a stateSyncHandler whose HandleMessage is called at least once
func pickStreamHandler(h *stateSyncHandler) *pb.StreamHandler {

	if syncerErr != nil {
		return nil
	}

	streamHandlers := stateSyncCore.PickHandlers([]*pb.PeerID{h.remotePeerId})
	h.Do(func() {
		if len(streamHandlers) > 0 {
			h.stream = streamHandlers[0]
		}

	})

	return h.stream
}

func (h *stateSyncHandler) Stop() { return }

func (h *stateSyncHandler) Tag() string { return "StateSync" }

func (h *stateSyncHandler) EnableLoss() bool { return false }

func (h *stateSyncHandler) NewMessage() proto.Message { return new(pb.SyncMsg) }

func (h *stateSyncHandler) HandleMessage(m proto.Message) error {

	wrapmsg := m.(*pb.SyncMsg)

	err := h.fsmHandler.Event(wrapmsg.Type.String(), wrapmsg)

	//CAUTION: DO NOT return error in non-fatal case or you will end the stream
	if err != nil {

		if _, ok := err.(*ErrHandlerFatal); ok {
			return err
		}
		logger.Errorf("Handle sync message fail: %s", err)
	}

	return nil
}

func (h *stateSyncHandler) BeforeSendMessage(proto.Message) error {
	return nil
}

func (h *stateSyncHandler) OnWriteError(e error) {
	logger.Error("Sync handler encounter writer error:", e)
}

func (h *stateSyncHandler) InitStreamHandlerImpl() {

	streamHandler := pickStreamHandler(h)

	h.server = newStateServer(h, streamHandler)
	h.client = newSyncer(nil, h, streamHandler)
}

func (h *stateSyncHandler) ExecuteSync(targetState []byte) error {
	return h.client.InitiateSync(targetState)
}

