package statesync

import (
	"fmt"
	_ "github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/peer"
	"github.com/abchain/fabric/core/statesync/stub"
	"github.com/abchain/fabric/flogging"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"github.com/looplab/fsm"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	_ "github.com/spf13/viper"
	"golang.org/x/net/context"
	"sync"
)

var logger = logging.MustGetLogger("statesyncstub")

func init() {
	stub.DefaultSyncFactory = newStateSyncHandlerFactory
}

var stateSyncCore *StateSyncStub
var syncerErr error
var once sync.Once

//func NewStateSync(p peer.Peer) {
//
//	once.Do(func() {
//		stateSyncCore, syncerErr = NewStateSyncWithPeer(p)
//		if syncerErr != nil {
//			logger.Error("Create new state syncer fail:", syncerErr)
//		}
//
//		ep, _ := p.GetPeerEndpoint()
//		//also set options for this peer
//		p.SetStreamOption("sync", StreamFilter{ep})
//	})
//}

// gives a reference to a singleton
func GetStateSync() (*StateSyncStub, error) {
	return stateSyncCore, syncerErr
}

type StateSyncStub struct {
	self *pb.PeerID
	*pb.StreamStub
	sync.RWMutex
	curCorrrelation uint64
	curTask         context.Context
}

func NewStateSyncWithPeer(p peer.Peer) *StateSyncStub {

	self, err := p.GetPeerEndpoint()
	if err != nil {
		panic("No self endpoint")
	}

	gctx, _ := context.WithCancel(p.GetPeerCtx())
	sycnStub := &StateSyncStub{
		self:    self.ID,
		curTask: gctx,
	}

	err = p.AddStreamStub("sync", stub.DefaultSyncFactory, sycnStub)
	if err != nil {
		logger.Error("Bind sync stub to peer fail: ", err)
		return nil
	}

	syncStreamStub := p.GetStreamStub("sync")
	if syncStreamStub == nil {
		logger.Error("peer have no sync streamstub")
		return nil
	}

	sycnStub.StreamStub = syncStreamStub
	return sycnStub
}

type ErrInProcess struct {
	error
}

type ErrHandlerFatal struct {
	error
}

//if busy, return current correlation Id, els return 0
func (s *StateSyncStub) IsBusy() uint64 {

	s.RLock()
	defer s.RUnlock()

	if s.curTask == nil {
		return uint64(0)
	} else {
		return s.curCorrrelation
	}
}

type stateSyncHandler struct {
	remotePeerId *pb.PeerID
	fsmHandler   *fsm.FSM
	server       *stateServer
	client       *syncer
}

func newStateSyncHandlerFactory(remoterId *pb.PeerID) pb.StreamHandlerImpl {
	logger.Debug("create handler for peer", remoterId)

	h := &stateSyncHandler{
		remotePeerId: remoterId,
	}

	h.fsmHandler = newFsmHandler(h)
	return h
}

func (s *StateSyncStub) SyncToState(ctx context.Context, targetState []byte, opt *syncOpt, peer *pb.PeerID) error {

	var err error
	s.Lock()
	if s.curTask != nil {
		s.Unlock()
		return &ErrInProcess{fmt.Errorf("Another task is running")}
	}

	s.curTask = ctx
	s.curCorrrelation++
	s.Unlock()

	// use stream stub get stream handler by PeerId
	// down cast stream handler to stateSyncHandler
	// call stateSyncHandler run
	handler := s.PickHandler(peer)

	if handler == nil {

		logger.Errorf("[%s]: Failed to find sync handler for peer <%v>",
			flogging.GoRDef, peer)

		err = fmt.Errorf("[%s]: Failed to find sync handler for peer <%v>",
			flogging.GoRDef, peer)

		return err
	}

	peerSyncHandler, ok := handler.StreamHandlerImpl.(*stateSyncHandler)

	//err = s.executeSync(ctx, handler, targetState)
	if !ok {
		logger.Errorf("[%s]: Target peer <%v>, failed to execute sync: %s",
			flogging.GoRDef, peer, err)
	}

	peerSyncHandler.run(ctx, targetState)

	defer func() {
		s.Lock()
		s.curTask = nil
		s.Unlock()
	}()

	return err
}

//func (s *StateSyncStub) executeSync(ctx context.Context, handler *pb.StreamHandler, targetState []byte) error {
//
//	peerSyncHandler, ok := handler.StreamHandlerImpl.(*stateSyncHandler)
//	if !ok {
//		panic("type error, not stateSyncHandler")
//	}
//
//	return peerSyncHandler.run(ctx, targetState)
//}

func (syncHandler *stateSyncHandler) run(ctx context.Context, targetState []byte) error {

	syncHandler.client = newSyncer(ctx, syncHandler)

	defer logger.Infof("[%s]: Exit. remotePeerIdName <%s>", flogging.GoRDef, syncHandler.remotePeerIdName())
	defer syncHandler.fini()

	logger.Infof("[%s]: Enter. remotePeerIdName <%s>", flogging.GoRDef, syncHandler.remotePeerIdName())
	//---------------------------------------------------------------------------
	// 1. query
	//---------------------------------------------------------------------------
	mostRecentIdenticalHistoryPosition, endBlockNumber, err := syncHandler.client.getSyncTargetBlockNumber()

	if mostRecentIdenticalHistoryPosition >= endBlockNumber {
		logger.Infof("[%s]: No sync required. mostRecentIdenticalHistoryPosition: %d, endBlockNumber: %d",
			flogging.GoRDef, mostRecentIdenticalHistoryPosition, endBlockNumber)
		return nil
	}

	if err != nil {
		logger.Errorf("[%s]: getSyncTargetBlockNumber err: %s", flogging.GoRDef, err)
		return err
	}
	logger.Infof("[%s]: query done. mostRecentIdenticalHistoryPosition: %d, endBlockNumber: %d",
		flogging.GoRDef, mostRecentIdenticalHistoryPosition, endBlockNumber)

	startBlockNumber := mostRecentIdenticalHistoryPosition + 1

	//---------------------------------------------------------------------------
	// 2. switch to the right checkpoint
	//---------------------------------------------------------------------------
	enableStatesyncTest := viper.GetBool("peer.enableStatesyncTest")
	if !enableStatesyncTest {

		checkpointPosition, err := syncHandler.client.switchToBestCheckpoint(mostRecentIdenticalHistoryPosition)
		if err != nil {
			logger.Errorf("[%s]: InitiateSync, switchToBestCheckpoint err: %s", flogging.GoRDef, err)

			return err
		}
		startBlockNumber = checkpointPosition + 1
		logger.Infof("[%s]: InitiateSync, switch done, startBlockNumber<%d>, endBlockNumber<%d>",
			flogging.GoRDef, startBlockNumber, endBlockNumber)
	}
	//---------------------------------------------------------------------------
	// 3. sync detals & blocks
	//---------------------------------------------------------------------------
	// go to syncdelta state
	syncHandler.fsmHandler.Event(enterGetDelta)
	_, err = syncHandler.client.syncDeltas(startBlockNumber, endBlockNumber)

	if err != nil {
		logger.Errorf("[%s]: sync detals err: %s", flogging.GoRDef, err)
		return err
	}
	logger.Infof("[%s]: sync detals done", flogging.GoRDef)

	return err
}

//---------------------------------------------------------------------------
// 1. acknowledge sync start request
//---------------------------------------------------------------------------
func (syncHandler *stateSyncHandler) beforeSyncStart(e *fsm.Event) {

	syncMsg := syncHandler.onRecvSyncMsg(e, nil)

	if syncMsg == nil {
		return
	}

	syncHandler.server = newStateServer(syncHandler)

	syncHandler.server.correlationId = syncMsg.CorrelationId

	size, err := syncHandler.server.ledger.GetBlockchainSize()
	if err != nil {
		e.Cancel(err)
		return
	}

	resp := &pb.SyncStateResp{}
	resp.BlockHeight = size

	err = syncHandler.sendSyncMsg(e, pb.SyncMsg_SYNC_SESSION_START_ACK, resp)
	if err != nil {
		syncHandler.server.ledger.Release()
	}
}

func (syncHandler *stateSyncHandler) fini() {

	err := syncHandler.sendSyncMsg(nil, pb.SyncMsg_SYNC_SESSION_END, nil)

	if err != nil {
		logger.Errorf("[%s]: sendSyncMsg SyncMsg_SYNC_SESSION_END err: %s", flogging.GoRDef, err)
	}

	syncHandler.fsmHandler.Event(enterSyncFinish)
}

func (syncHandler *stateSyncHandler) sendSyncMsg(e *fsm.Event, msgType pb.SyncMsg_Type, payloadMsg proto.Message) error {

	logger.Debugf("%s: <%s> to <%s>", flogging.GoRDef, msgType.String(), syncHandler.remotePeerIdName())
	var data = []byte(nil)

	if payloadMsg != nil {
		tmp, err := proto.Marshal(payloadMsg)
		if err != nil {
			lerr := fmt.Errorf("Error Marshalling payload message for <%s>: %s", msgType.String(), err)
			logger.Info(lerr.Error())
			if e != nil {
				e.Cancel(&fsm.NoTransitionError{Err: lerr})
			}
			return lerr
		}
		data = tmp
	}

	stream, err := pickStreamHandler(syncHandler)

	if err == nil {
		err = stream.SendMessage(&pb.SyncMsg{
			Type:    msgType,
			Payload: data})

		if err != nil {
			logger.Errorf("Error sending %s : %s", msgType, err)
		}
	} else {
		logger.Errorf("%s", err)
	}
	return err
}

func pickStreamHandler(h *stateSyncHandler) (*pb.StreamHandler, error) {

	if syncerErr != nil {
		return nil, syncerErr
	}

	var err error
	var stream *pb.StreamHandler

	streamHandlers := stateSyncCore.PickHandlers([]*pb.PeerID{&pb.PeerID{h.remotePeerIdName()}})
	if len(streamHandlers) > 0 {
		stream = streamHandlers[0]
	} else {
		err = fmt.Errorf("Failed to pick a stream handler <%s>.",
			h.remotePeerIdName())
	}

	return stream, err
}

func (syncHandler *stateSyncHandler) onRecvSyncMsg(e *fsm.Event, payloadMsg proto.Message) *pb.SyncMsg {

	logger.Debugf("%s: from <%s>", flogging.GoRDef, syncHandler.remotePeerIdName())

	if _, ok := e.Args[0].(*pb.SyncMsg); !ok {
		e.Cancel(fmt.Errorf("Received unexpected sync message type"))
		return nil
	}
	msg := e.Args[0].(*pb.SyncMsg)

	if payloadMsg != nil {
		err := proto.Unmarshal(msg.Payload, payloadMsg)
		if err != nil {
			e.Cancel(fmt.Errorf("Error unmarshalling %s: %s", msg.Type.String(), err))
			return nil
		}
	}

	logger.Debugf("<%s> from <%s>", msg.Type.String(), syncHandler.remotePeerIdName())
	return msg
}

func (h *stateSyncHandler) leaveIdle(e *fsm.Event) {

	stateUpdate := "leaveIdle"
	h.dumpStateUpdate(stateUpdate)
}

func (h *stateSyncHandler) enterIdle(e *fsm.Event) {

	stateUpdate := "enterIdle"
	h.dumpStateUpdate(stateUpdate)

	if h.client != nil {
		h.client.fini()
	}

}

func (h *stateSyncHandler) dumpStateUpdate(stateUpdate string) {
	logger.Debugf("%s: StateSyncHandler Syncing state update: %s. correlationId<%d>, remotePeerId<%s>", flogging.GoRDef,
		stateUpdate, 0, h.remotePeerIdName())
}

func (h *stateSyncHandler) remotePeerIdName() string {
	return h.remotePeerId.GetName()
}

func (h *stateSyncHandler) Stop() { return }

func (h *stateSyncHandler) Tag() string { return "StateSyncStub" }

func (h *stateSyncHandler) EnableLoss() bool { return false }

func (h *stateSyncHandler) NewMessage() proto.Message { return new(pb.SyncMsg) }

func (h *stateSyncHandler) HandleMessage(m proto.Message) error {

	wrapmsg := m.(*pb.SyncMsg)

	err := h.fsmHandler.Event(wrapmsg.Type.String(), wrapmsg)

	//CAUTION: DO NOT return error in non-fatal case or you will end the stream
	if err != nil {

		if _, ok := err.(ErrHandlerFatal); ok {
			return err
		}
		logger.Errorf("Handle sync message <%s> fail: %s", wrapmsg.Type.String(), err)
	}

	return nil
}

func (h *stateSyncHandler) BeforeSendMessage(proto.Message) error {

	return nil
}

func (h *stateSyncHandler) OnWriteError(e error) {
	logger.Error("Sync handler encounter writer error:", e)
}
