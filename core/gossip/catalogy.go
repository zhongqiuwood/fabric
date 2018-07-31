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
	Model() *model.Model
	//just notify the model is updated
	SelfUpdate()
	HandleUpdate(*pb.Gossip_Update, CatalogPeerPolicies)
	HandleDigest(*pb.Gossip_Digest, CatalogPeerPolicies)
}

type CatalogPeerPolicies interface {
	GetPeer() *pb.PeerID
	GetId() string //equal to GetPeer().GetName()
	AllowRecvUpdate() bool
	PushUpdateQuota() int
	PushUpdate(int)
	RecordViolation(error)
	ScoringPeer(s int, weight uint)
}

type CatalogHelper interface {
	Name() string
	GetPolicies() CatalogPolicies //caller do not need to check the interface

	TransDigestToPb(model.Digest) *pb.Gossip_Digest
	TransPbToDigest(*pb.Gossip_Digest) model.Digest

	UpdateMessage() proto.Message
	EncodeUpdate(CatalogPeerPolicies, model.Update, proto.Message) proto.Message
	DecodeUpdate(CatalogPeerPolicies, proto.Message) (model.Update, error)
}

type puller struct {
	model.PullerHelper
	*model.Puller
	hf      func(*model.Puller)
	gotPush bool
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

//if we check-and-create puller under "responsed-pulling" condiction, no-op also indicate the existed
//puller a pushing is processed
func (h *pullWorks) newPuller(cpo CatalogPeerPolicies, isResponsedPull bool) *puller {
	h.Lock()
	defer h.Unlock()

	if pl, ok := h.m[cpo]; ok {
		if isResponsedPull {
			pl.gotPush = true
		}
		return nil
	}

	p := &puller{}
	h.m[cpo] = p

	return p
}

type scheduleWorks struct {
	cancelSchedules map[context.Context]context.CancelFunc
	totalPushingCnt int64
	plannedCnt      int64
	sync.Mutex
}

func (s *scheduleWorks) reachPlan() bool {
	s.Lock()
	defer s.Unlock()

	return s.plannedCnt <= s.totalPushingCnt
}

func (s *scheduleWorks) pushDone() {

	s.Lock()
	defer s.Unlock()

	//it is not count for any "exceed the planning" pushing
	if s.totalPushingCnt < s.plannedCnt {
		s.totalPushingCnt++
	}

}

func (s *scheduleWorks) endSchedule(ctx context.Context) {

	s.Lock()
	defer s.Unlock()

	if cf, ok := s.cancelSchedules[ctx]; ok {
		cf()
		delete(s.cancelSchedules, ctx)
	}

	if len(s.cancelSchedules) == 0 && s.totalPushingCnt < s.plannedCnt {
		logger.Infof("all schedules end with planned count %d left, drop planning",
			s.plannedCnt-s.totalPushingCnt)
		s.plannedCnt = s.totalPushingCnt
	}
}

//if there is no enough planned push count, prepare a context and return the actuall additional planned count
func (s *scheduleWorks) newSchedule(ctx context.Context, wishCount int) (context.Context, int) {

	s.Lock()
	defer s.Unlock()

	curPlannedCnt := int(s.plannedCnt - s.totalPushingCnt)

	//no need to schedule more
	if len(s.cancelSchedules) > 0 && curPlannedCnt >= wishCount {
		return nil, 0
	}

	if curPlannedCnt < wishCount {
		wishCount = wishCount - curPlannedCnt
	}

	wctx, cancelF := context.WithCancel(ctx)
	s.cancelSchedules[wctx] = cancelF
	s.plannedCnt = s.plannedCnt + int64(wishCount)

	return wctx, wishCount
}

type catalogHandler struct {
	CatalogHelper
	hctx     context.Context
	model    *model.Model
	pulls    pullWorks
	schedule scheduleWorks
	sstub    *pb.StreamStub
}

func NewCatalogHandlerImpl(stub *pb.StreamStub, ctx context.Context, helper CatalogHelper, model *model.Model) (ret *catalogHandler) {

	return &catalogHandler{
		CatalogHelper: helper,
		sstub:         stub,
		hctx:          ctx,
		model:         model,
		pulls:         pullWorks{m: make(map[CatalogPeerPolicies]*puller)},
		schedule:      scheduleWorks{cancelSchedules: make(map[context.Context]context.CancelFunc)},
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
		M:       &pb.Gossip_Dig{h.TransDigestToPb(d)},
	}

	h.cpo.PushUpdate(msg.EstimateSize())

	return msg
}

func (h *sessionHandler) EncodeUpdate(u model.Update) proto.Message {

	udsent := &pb.Gossip_Update{}

	if u != nil {
		payloadByte, err := proto.Marshal(
			h.CatalogHelper.EncodeUpdate(h.cpo, u,
				h.CatalogHelper.UpdateMessage()))
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

func (h *sessionHandler) CanPull() model.PullerHandler {

	if !h.cpo.AllowRecvUpdate() {
		logger.Infof("Reject a responding pulling to peer [%s]", h.cpo.GetId())
		return nil
	}

	if pos := h.pulls.newPuller(h.cpo, true); pos == nil {
		return nil
	} else {

		logger.Debugf("Start a responding pulling to peer [%s]", h.cpo.GetId())

		pos.init(h, func(p *model.Puller) {

			if p != nil {
				pctx, _ := context.WithTimeout(h.hctx,
					time.Duration(h.GetPolicies().PullTimeout())*time.Second)
				err := p.Process(pctx)

				//when we succefully accept an update, we also schedule a new push process
				if err == nil {
					h.executePush(map[string]bool{h.cpo.GetId(): true})
					return
				} else {
					h.cpo.RecordViolation(fmt.Errorf("Passive pulling fail: %s", err))
				}
			}

			h.pulls.popPuller(h.cpo)
		})
		return pos
	}
}

func (h *catalogHandler) Model() *model.Model {

	return h.model
}

func (h *catalogHandler) SelfUpdate() {

	go h.executePush(map[string]bool{})
}

func (h *catalogHandler) HandleDigest(msg *pb.Gossip_Digest, cpo CatalogPeerPolicies) {

	strm := h.sstub.PickHandler(cpo.GetPeer())
	if strm == nil {
		logger.Errorf("No stream found for %s", cpo.GetId())
		return
	}

	err := model.AcceptPulling(&sessionHandler{h, cpo}, strm, h.model, h.TransPbToDigest(msg))
	if err != nil {
		logger.Error("Sending push message fail", err)
	} else {
		h.schedule.pushDone()
	}
}

func (h *catalogHandler) HandleUpdate(msg *pb.Gossip_Update, cpo CatalogPeerPolicies) {

	puller := h.pulls.popPuller(cpo)
	if puller == nil {
		//something wrong! if no corresponding puller, we never handle this message
		logger.Errorf("No puller in catalog %s avaiable for peer %s", h.Name(), cpo.GetId())
		return
	}

	var umsg proto.Message

	if len(msg.Payload) > 0 {
		umsg = h.CatalogHelper.UpdateMessage()
		err := proto.Unmarshal(msg.Payload, umsg)
		if err != nil {
			cpo.RecordViolation(fmt.Errorf("Unmarshal message for update in catalog %s fail: %s", h.Name(), err))
			return
		}
	}

	ud, err := h.DecodeUpdate(cpo, umsg)
	if err != nil {
		cpo.RecordViolation(fmt.Errorf("Decode update for catalog %s fail: %s", h.Name(), err))
		return
	}

	//the final update is executed in another thread
	puller.NotifyUpdate(ud)

}

func (h *catalogHandler) executePush(excluded map[string]bool) error {

	logger.Debug("try execute a pushing process")
	wctx, pushCnt := h.schedule.newSchedule(h.hctx, h.GetPolicies().PushCount())
	if wctx == nil {
		logger.Debug("Another pushing process is running or no plan any more")
		return fmt.Errorf("Resource is occupied")
	}
	defer h.schedule.endSchedule(wctx)
	var pushedCnt int

	for strm := range h.sstub.OverAllHandlers(wctx) {

		logger.Debugf("finish (%d/%d) pulls, try execute a pushing on stream to %s",
			pushedCnt, pushCnt, strm.GetName())

		if pushedCnt >= pushCnt || h.schedule.reachPlan() {
			break
		}

		ph, ok := stub.ObtainHandler(strm.StreamHandler).(*handlerImpl)
		if !ok {
			panic("type error, not handlerImpl")
		}
		cpo := ph.GetPeerPolicy()

		//check excluded first
		_, ok = excluded[cpo.GetId()]
		if ok {
			logger.Debugf("Skip exclueded peer [%s]", cpo.GetId())
			continue
		}

		if pos := h.pulls.newPuller(cpo, false); pos != nil {
			logger.Debugf("start pulling on stream to %s", cpo.GetId())

			var err error
			pos.Puller = model.NewPuller(&sessionHandler{h, cpo}, strm.StreamHandler, h.model)

			if pos.Puller != nil {
				pctx, _ := context.WithTimeout(wctx, time.Duration(h.GetPolicies().PullTimeout())*time.Second)
				err = pos.Puller.Process(pctx)

				logger.Debugf("Scheduled pulling from peer [%s] finish: %v", cpo.GetId(), err)
				if err == context.DeadlineExceeded {
					cpo.RecordViolation(fmt.Errorf("Aggressive pulling fail: %s", err))
				}
			} else {
				logger.Info("we just gave up the pulling to %s", cpo.GetId())
				err = fmt.Errorf("Give up pulling")
			}

			if err != nil {
				pos = h.pulls.popPuller(cpo)
			}

			if pos.gotPush {
				pushedCnt++
				logger.Debugf("Scheduled pulling has pushed our status to peer [%s]", cpo.GetId())
			}

		} else {
			logger.Debugf("stream %s has a working puller, try next one", cpo.GetId())
		}
	}

	logger.Infof("Finished a push process, plan %d and %d finished", pushCnt, pushedCnt)

	return nil
}
