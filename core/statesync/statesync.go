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

func NewStateSync(p peer.Peer) {
	logger.Debug("State sync module inited")

	once.Do(func() {
		stateSyncCore, syncerErr = GetNewStateSync(p)
		if syncerErr != nil {
			logger.Error("Create new state syncer fail:", syncerErr)
		}
	})
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
func (s *StateSync) SyncToState(ctx context.Context, targetState []byte, opt *syncOpt) error {

	s.Lock()
	if s.curTask != nil {
		s.Unlock()
		return &ErrInProcess{fmt.Errorf("Another task is running")}
	}

	s.curTask = ctx
	s.curCorrrelation++
	s.Unlock()

	// qiu: ask peer for state

	defer func() {
		s.Lock()
		s.curTask = nil
		s.Unlock()
	}()

	return fmt.Errorf("Not implied")
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
	peerId  *pb.PeerID
	handler *fsm.FSM
	server  *stateServer
	client  *syncer
	stream  *pb.StreamHandler
	sync.Once
}

func newStateSyncHandler(id *pb.PeerID) pb.StreamHandlerImpl {
	logger.Debug("create handler for peer", id)
	h := &stateSyncHandler{
		peerId: id,
	}

	h.handler = newHandler(h)
	return h
}

//this should be call with a stateSyncHandler whose HandleMessage is called at least once
func pickStreamHandler(h *stateSyncHandler) *pb.StreamHandler {

	if syncerErr != nil {
		return nil
	}

	streams := stateSyncCore.PickHandlers([]*pb.PeerID{h.peerId})
	h.Do(func() {
		if len(streams) > 0 {
			h.stream = streams[0]
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

	err := h.handler.Event(wrapmsg.Type.String(), wrapmsg)

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
