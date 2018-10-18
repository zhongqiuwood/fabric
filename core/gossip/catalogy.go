package gossip

import (
	"fmt"
	model "github.com/abchain/fabric/core/gossip/model"
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

type CatalogHandlerEx interface {
	CatalogHandler
	OnConnectNewPeer(*pb.PeerID)
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

type pullWorks struct {
	sync.Mutex
	m map[CatalogPeerPolicies]*model.Puller
	//a home-make sync.condition which can work along with a context
	occFlag       int
	releaseNotify chan error
}

func (h *pullWorks) queryPuller(cpo CatalogPeerPolicies) *model.Puller {

	h.Lock()
	defer h.Unlock()
	return h.m[cpo]
}

func (h *pullWorks) popPuller(cpo CatalogPeerPolicies) *model.Puller {

	h.Lock()
	defer h.Unlock()
	p, ok := h.m[cpo]
	if ok {
		delete(h.m, cpo)

		if h.occFlag > 1 {
			select {
			case h.releaseNotify <- nil:
			default:
				logger.Errorf("Seems [%s] try to made notify while no peer is waiting any more", cpo.GetId())
			}
		} else if h.occFlag != 1 {
			panic("Impossible occFlag")
		}
		h.occFlag--

	}
	return p
}

//if we check-and-create puller under "responsed-pulling" condiction, no-op also indicate the existed
//puller a pushing is processed
func (h *pullWorks) newPuller(ctx context.Context, cpo CatalogPeerPolicies, m *model.Model) *model.Puller {
	h.Lock()
	defer h.Unlock()

	if _, ok := h.m[cpo]; ok {
		return nil
	}

	puller := model.NewPuller(m)
	h.m[cpo] = puller
	h.occFlag++

	if h.occFlag > 1 {

		//we must wait for another pulling is finish, so avoid a concurrent-polling which may
		//waste a lot of bandwidth and comp. cost
		//this routine work like a condiction
		h.Unlock()
		select {
		case <-h.releaseNotify:
		case <-ctx.Done():
			logger.Infof("Peer [%s] waiting for another pulling fail, give up this pulling", cpo.GetId())
			h.Lock()
			h.m[cpo] = nil
			h.occFlag--
			return nil
		}

		h.Lock()
	}
	return puller
}

type scheduleWorks struct {
	cancelSchedules   map[context.Context]context.CancelFunc
	totalPushingCnt   int64
	plannedCnt        int64
	maxConcurrentTask int
	sync.RWMutex
}

func (s *scheduleWorks) reachPlan(target int64) bool {
	s.Lock()
	defer s.Unlock()

	return target <= s.totalPushingCnt
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

//schedule works as following:
//1. each request will be passed (return a context) if current "pending" count (plannedcnt - totalpushing)
//   is not larger than the wishCount
//2. passed request obtained a "target" pushing number and should check if currently totalpushing has reached the
//   target number
//3. caller with passed request can do everything possible to make the totalpushing count increase (it can even
//   do nothing and just wait there until there is enough incoming digests)
func (s *scheduleWorks) newSchedule(ctx context.Context, wishCount int) (context.Context, int64) {

	s.Lock()
	defer s.Unlock()

	curPlannedCnt := s.totalPushingCnt + int64(wishCount)

	//no need to schedule more
	if len(s.cancelSchedules) > s.maxConcurrentTask {
		logger.Infof("A schedule is rejected because the concurrent task exceed limit: %d", s.maxConcurrentTask)
		return nil, 0
	} else if curPlannedCnt <= s.plannedCnt {
		//no need to start new schedule
		return nil, 0
	}

	s.plannedCnt = curPlannedCnt

	wctx, cancelF := context.WithCancel(ctx)
	s.cancelSchedules[wctx] = cancelF

	return wctx, curPlannedCnt
}

type catalogHandler struct {
	CatalogHelper
	hctx     context.Context //hctx CAN NOT be used for waiting directly!
	model    *model.Model
	pulls    pullWorks
	schedule scheduleWorks
	sstub    *pb.StreamStub
}

func NewCatalogHandlerImpl(stub *pb.StreamStub, ctx context.Context, helper CatalogHelper, m *model.Model) (ret *catalogHandler) {

	return &catalogHandler{
		CatalogHelper: helper,
		sstub:         stub,
		hctx:          ctx,
		model:         m,
		pulls:         pullWorks{m: make(map[CatalogPeerPolicies]*model.Puller), releaseNotify: make(chan error, 1)},
		schedule:      scheduleWorks{cancelSchedules: make(map[context.Context]context.CancelFunc), maxConcurrentTask: 5},
	}
}

type sessionHandler struct {
	*catalogHandler
	cpo        CatalogPeerPolicies
	pullingCtx context.Context
}

func genSessionHandler(h *catalogHandler, cpo CatalogPeerPolicies) *sessionHandler {
	return &sessionHandler{h, cpo, nil}
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

func (h *sessionHandler) Process(strm *pb.StreamHandler, d model.Digest, responding bool) (err error) {

	cpo := h.cpo
	logger.Debugf("Start a pulling (responding %v) to peer [%s]", responding, cpo.GetId())

	pctx, pctxend := context.WithTimeout(h.hctx,
		time.Duration(h.GetPolicies().PullTimeout())*time.Second)
	defer pctxend()
	h.pullingCtx = pctx

	var puller *model.Puller
	if responding {
		puller, err = model.AcceptPulling(h, strm, h.Model(), d)
	} else {
		puller, err = model.StartPulling(h, strm)
	}

	if puller != nil {
		defer h.pulls.popPuller(cpo)
	}

	if err != nil {
		logger.Errorf("accepting pulling fail: %s", err)
		return
	} else if puller == nil {
		logger.Debugf("do not start pulling to peer [%s]", cpo.GetId())
		return
	}

	err = puller.Process(pctx)

	//when we succefully accept an update, we also trigger a new push process
	if err == nil {
		//notice we should exclude current stream
		//so the triggered push wouldn't make duplicated pulling
		if responding {
			go h.executePush(map[*pb.StreamHandler]bool{strm: true})
		}
	} else if err == model.EmptyUpdate {
		logger.Debugf("pull nothing from peer [%s]", cpo.GetId())
	} else {
		cpo.RecordViolation(fmt.Errorf("Pulling (responding %v) fail: %s", responding, err))
	}

	return
}

func (h *sessionHandler) CanPull() *model.Puller {

	if h.pullingCtx == nil {
		panic("WRONG CODE: CanPull can be only called implicitly within the Process method")
	}

	if !h.GetPolicies().AllowRecvUpdate() || !h.cpo.AllowRecvUpdate() {
		logger.Infof("Policy has rejected a pulling to peer [%s]", h.cpo.GetId())
		return nil
	}

	return h.pulls.newPuller(h.pullingCtx, h.cpo, h.Model())
}

func (h *catalogHandler) Model() *model.Model {

	return h.model
}

var emptyExcluded = make(map[*pb.StreamHandler]bool)

func (h *catalogHandler) SelfUpdate() {

	go h.executePush(emptyExcluded)
}

func (h *catalogHandler) HandleDigest(msg *pb.Gossip_Digest, cpo CatalogPeerPolicies) {

	strm := h.sstub.PickHandler(cpo.GetPeer())
	if strm == nil {
		logger.Errorf("No stream found for %s", cpo.GetId())
		return
	}

	sess := genSessionHandler(h, cpo)
	go sess.Process(strm, h.TransPbToDigest(msg), true)

	//everytime we accept a digest, it is counted as a push
	h.schedule.pushDone()
}

func (h *catalogHandler) HandleUpdate(msg *pb.Gossip_Update, cpo CatalogPeerPolicies) {

	puller := h.pulls.queryPuller(cpo)
	if puller == nil {
		//something wrong! if no corresponding puller, we never handle this message
		logger.Errorf("No puller in catalog %s avaiable for peer %s", h.Name(), cpo.GetId())
		return
	}

	umsg := h.CatalogHelper.UpdateMessage()

	if len(msg.Payload) > 0 {
		err := proto.Unmarshal(msg.Payload, umsg)
		if err != nil {
			cpo.RecordViolation(fmt.Errorf("Unmarshal message for update in catalog %s fail: %s", h.Name(), err))
			return
		}
	}

	ud, err := h.DecodeUpdate(cpo, umsg)
	if err != nil {
		cpo.RecordViolation(fmt.Errorf("Decode update for catalog %s fail: %s", h.Name(), err))
	}

	//the final update is executed in another thread, even nil update is accepted
	logger.Debugf("Accept update from peer [%s]", cpo.GetId())
	puller.NotifyUpdate(ud)

}

func (h *catalogHandler) executePush(excluded map[*pb.StreamHandler]bool) error {

	logger.Debug("try execute a pushing process for handler", h.Name())
	wctx, targetCnt := h.schedule.newSchedule(h.hctx, h.GetPolicies().PushCount())
	if wctx == nil {
		logger.Debug("Another pushing process is running or no plan any more")
		return fmt.Errorf("Resource is occupied")
	}
	defer h.schedule.endSchedule(wctx)

	var pushCnt int
	for strm := range h.sstub.OverAllHandlers(wctx) {

		if h.schedule.reachPlan(targetCnt) {
			break
		} else if _, ok := excluded[strm.StreamHandler]; ok {
			logger.Debugf("stream %s is excluded, try next one", strm.Id.GetName())
			continue
		}

		logger.Debugf("finish (%d) pulls, try execute a pushing on stream to %s",
			pushCnt, strm.Id.GetName())

		ph, ok := ObtainHandler(strm.StreamHandler).(*handlerImpl)
		if !ok {
			panic("type error, not handlerImpl")
		}
		cpo := ph.GetPeerPolicy()

		if err := genSessionHandler(h, cpo).Process(strm.StreamHandler, nil, false); err == model.EmptyDigest {
			//***GIVEN UP THE WHOLE PUSH PROCESS***
			logger.Infof("Catalogy handler has forbidden a pulling process")
			break
		} else if err == nil {
			pushCnt++
			logger.Debugf("Scheduled pulling from peer [%s] finish", cpo.GetId())
		} else {
			logger.Debugf("Scheduled pulling from peer [%s] failed (%v)", cpo.GetId(), err)
		}

	}

	logger.Debugf("Finished a push process,  %d finished", pushCnt)

	return nil
}
