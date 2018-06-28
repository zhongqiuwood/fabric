package gossip

import (
	"fmt"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/gossip/stub"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"sync"
	"time"
)

type CatalogHandler interface {
	Name() string
	SelfUpdate(model.UpdateIn)
	HandleUpdate(*pb.PeerID, *pb.Gossip_Update, CatalogPeerPolicies)
	HandleDigest(*pb.PeerID, *pb.Gossip_Digest, CatalogPeerPolicies)
}

type CatalogPeerPolicies interface {
	AllowRecvUpdate() bool
	PushUpdateQuota() int
	PushUpdate(int)
	RecordViolation(error)
	ScoringPeer(s int, weight uint)
}

type CatalogHelper interface {
	Name() string
	GetPolicies() CatalogPolicies //caller do not need to check the interface
	GetStatus() model.Status

	EncodeDigest(model.Digest) *pb.Gossip_Digest
	EncodeUpdate(model.UpdateOut) proto.Message
	DecodeUpdate(CatalogPeerPolicies, []byte) (model.UpdateIn, error)
	DecodeProtoDigest(CatalogPeerPolicies, *pb.Gossip_Digest) model.Digest
}

type puller struct {
	model.PullerHelper
	*model.Puller
	hf func(*model.Puller)
}

func (p *puller) init(ph model.PullerHelper, hf func(*model.Puller)) {
	p.PullerHelper = ph
	p.hf = hf
}

func (p *puller) Handle(puller *model.Puller) {
	p.Puller = puller
	if p.hf == nil {
		logger.Criticalf("Your code never call init method before providing it in the CanPull callback")
	}

	//we use a new thread for handing puller task
	go p.hf(puller)
}

type pullWorks struct {
	sync.Mutex
	m map[CatalogPeerPolicies]*puller
}

func (h *pullWorks) popPuller(cpo CatalogPeerPolicies) *puller {

	h.Lock()
	defer h.Unlock()
	p := h.m[cpo]
	delete(h.m, cpo)
	return p
}

func (h *pullWorks) newPuller(cpo CatalogPeerPolicies) *puller {
	h.Lock()
	defer h.Unlock()

	if _, ok := h.m[cpo]; ok {
		return nil
	}

	p := &puller{}
	h.m[cpo] = p

	return p
}

type scheduleWorks struct {
	cancelSchedule context.CancelFunc
	pushingCnt     int
	plannedCnt     int
	sync.Mutex
}

func (s *scheduleWorks) pushedCnt() int {
	s.Lock()
	defer s.Unlock()
	return s.pushingCnt
}

func (s *scheduleWorks) pushDone() {

	s.Lock()
	defer s.Unlock()
	s.pushingCnt++
}

func (s *scheduleWorks) endSchedule() {
	s.Lock()
	defer s.Unlock()

	if s.cancelSchedule != nil {
		//cancel last schedule and use a new one
		s.cancelSchedule()
		s.cancelSchedule = nil
	}
}

func (s *scheduleWorks) planPushingCount(newCnt int) {
	s.Lock()
	defer s.Unlock()

	if s.plannedCnt < newCnt {
		s.plannedCnt = newCnt
	}
}

//prepare a context and pop the planned pushing count
func (s *scheduleWorks) newSchedule(ctx context.Context) (context.Context, int) {

	s.Lock()
	defer s.Unlock()

	//check plannedCnt is important: that is, NEVER start a schedule
	//with 0 planned count
	if s.cancelSchedule != nil || s.plannedCnt == 0 {
		return nil, 0
	}

	wctx, cancelF := context.WithCancel(ctx)
	s.cancelSchedule = cancelF
	s.pushingCnt = 0

	plannedCnt := s.plannedCnt
	s.plannedCnt = 0

	return wctx, plannedCnt
}

type catalogHandler struct {
	CatalogHelper
	model    *model.Model
	pulls    pullWorks
	schedule scheduleWorks
	sstub    *pb.StreamStub
}

func NewCatalogHandlerImpl(stub *pb.StreamStub, helper CatalogHelper) (ret *catalogHandler) {

	return &catalogHandler{
		CatalogHelper: helper,
		sstub:         stub,
		model:         model.NewGossipModel(helper.GetStatus()),
		pulls:         pullWorks{m: make(map[CatalogPeerPolicies]*puller)},
	}
}

type sessionHandler struct {
	*catalogHandler
	cpo CatalogPeerPolicies
}

//implement of pushhelper and pullerhelper
func (h *sessionHandler) EncodeDigest(d model.Digest) proto.Message {

	msg := &pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: h.Name(),
		M:       &pb.Gossip_Dig{h.CatalogHelper.EncodeDigest(d)},
	}

	h.cpo.PushUpdate(msg.EstimateSize())

	return msg
}

func (h *sessionHandler) EncodeUpdate(u model.UpdateOut) proto.Message {

	udsent := &pb.Gossip_Update{}

	if u != nil {
		payloadByte, err := proto.Marshal(h.CatalogHelper.EncodeUpdate(u))
		if err == nil {
			udsent.Payload = payloadByte
			h.cpo.PushUpdate(len(payloadByte))
		} else {
			logger.Error("Encode update failure:", err)
		}
	}

	return &pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: h.Name(),
		M:       &pb.Gossip_Ud{udsent},
	}
}

func (h *sessionHandler) runPullTask(cpo CatalogPeerPolicies, puller *model.Puller) (ret error) {

	pctx, _ := context.WithTimeout(context.Background(),
		time.Duration(h.GetPolicies().PullTimeout())*time.Second)
	ret = puller.Process(pctx)
	return
}

func (h *sessionHandler) CanPull() model.PullerHandler {

	if !h.cpo.AllowRecvUpdate() {
		return nil
	}

	if pos := h.pulls.newPuller(h.cpo); pos == nil {
		return nil
	} else {
		pos.init(h, func(p *model.Puller) {

			err := h.runPullTask(h.cpo, p)

			//when we succefully accept an update, we also schedule a new push process
			if err == nil {
				h.schedule.planPushingCount(h.GetPolicies().PushCount())
				h.schedulePush()
			} else {
				h.cpo.RecordViolation(fmt.Errorf("Passive pulling fail: %s", err))
			}
		})
		return pos
	}
}

func (h *catalogHandler) SelfUpdate(u model.UpdateIn) {

	h.model.RecvUpdate(u)
	go h.schedulePush()
}

func (h *catalogHandler) HandleDigest(peer *pb.PeerID, msg *pb.Gossip_Digest, cpo CatalogPeerPolicies) {

	strm := h.sstub.PickHandler(peer)
	if strm != nil {
		logger.Errorf("No stream found for %s", peer.Name)
		return
	}

	digest := h.DecodeProtoDigest(cpo, msg)
	err := model.AcceptPush(&sessionHandler{h, cpo}, strm, h.model, digest)
	if err != nil {
		logger.Error("Sending push message fail", err)
	} else {
		h.schedule.pushDone()
	}
}

func (h *catalogHandler) HandleUpdate(peer *pb.PeerID, msg *pb.Gossip_Update, cpo CatalogPeerPolicies) {

	puller := h.pulls.popPuller(cpo)
	if puller == nil {
		//something wrong! if no corresponding puller, we never handle this message
		logger.Errorf("No puller in catalog %s avaiable for peer %s", h.Name(), peer.Name)
		return
	}

	ud, err := h.DecodeUpdate(cpo, msg.Payload)
	if err != nil {
		cpo.RecordViolation(fmt.Errorf("Decode update for catalog %s fail: %s", h.Name(), err))
		return
	}

	//the final update is executed in another thread
	puller.NotifyUpdate(ud)

}

func (h *catalogHandler) executePush() error {

	logger.Debug("try execute a pushing process")
	wctx, pushCnt := h.schedule.newSchedule(context.Background())
	if wctx == nil {
		logger.Debug("Another pushing process is running or no plan any more")
		return fmt.Errorf("Resource is occupied")
	}
	defer h.schedule.endSchedule()

	for strm := range h.sstub.OverAllHandlers(wctx) {

		if h.schedule.pushedCnt() >= pushCnt {
			break
		}

		ph, ok := stub.ObtainHandler(strm.StreamHandler).(*handlerImpl)
		if !ok {
			panic("type error, not handlerImpl")
		}
		cpo := ph.GetPeerPolicy()

		if pos := h.pulls.newPuller(cpo); pos != nil {
			pos.Puller = model.NewPuller(&sessionHandler{h, cpo}, strm.StreamHandler, h.model)
			pctx, _ := context.WithTimeout(wctx, time.Duration(h.GetPolicies().PullTimeout())*time.Second)
			err := pos.Puller.Process(pctx)

			if err == context.DeadlineExceeded {
				cpo.RecordViolation(fmt.Errorf("Aggressive pulling fail: %s", err))
			} else if err == nil {
				logger.Debug("Scheduled pulling from peer [%s]", strm.GetName())
			}
		}
	}

	logger.Infof("Finished a push process, plan %d and %d finished", pushCnt, h.schedule.pushedCnt())

	return nil
}

func (h *catalogHandler) schedulePush() {

	//complete for the schedule resource until it failed
	for h.executePush() == nil {
	}

}
