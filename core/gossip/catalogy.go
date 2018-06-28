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
	SelfUpdate(model.Status)
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

	EncodeDigest(model.Digest) *pb.Gossip_Digest
	EncodeUpdate(model.UpdateOut) proto.Message
	DecodeUpdate(CatalogPeerPolicies, []byte) (model.UpdateIn, error)
	DecodeProtoDigest(CatalogPeerPolicies, *pb.Gossip_Digest) model.Digest
}

type puller struct {
	PullerHelper
	*model.Puller
	hf func(*model.Puller)
}

func (p *puller) init(ph PullerHelper, hf func(*model.Puller)) {
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

func (h *pullWorks) endPuller(cpo CatalogPeerPolicies) {
	h.Lock()
	defer h.Unlock()
	delete(h.m, cpo)
}

func (h *pullWorks) checkPuller(cpo CatalogPeerPolicies) *model.Puller {

	h.Lock()
	defer h.Unlock()
	return h.m[cpo]
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
	endSchedule context.CancelFunc
	expectCnt   int
	sync.Mutex
}

func (s *scheduleWorks) endSchedule() {
	s.Lock()
	defer s.Unlock()

	if s.endSchedule != nil {
		//cancel last schedule and use a new one
		s.endSchedule()
		s.endSchedule = nil
	}
}

func (s *scheduleWorks) pendingPush() int {
	s.Lock()
	defer s.Unlock()
	return s.expectCnt
}

func (s *scheduleWorks) pushDone() {

	s.Lock()
	defer s.Unlock()
	if s.expectCnt > 0 {
		s.expectCnt--
	}

}

func (s *scheduleWorks) resetSchedule(newScheduleCnt int) {
	s.Lock()
	defer s.Unlock()

}

func (s *scheduleWorks) newSchedule(ctx context.Context, newScheduleCnt int) context.Context {

	s.Lock()
	defer s.Unlock()

	if h.schedule.endSchedule != nil {
		//we have a running schedule now
		if s.expectCnt < newScheduleCnt {
			s.expectCnt = newScheduleCnt
		}

		return nil
	}

	wctx, cancelF := context.WithCancel(ctx)
	s.endSchedule = cancelF
	s.expectCnt = newScheduleCnt

	return wctx
}

type catalogHandler struct {
	CatalogHelper
	model    *model.Model
	pulls    pullWorks
	schedule scheduleWorks
	sstub    *pb.StreamStub
}

func NewCatalogHandlerImpl(stub *pb.StreamStub, helper CatalogHelper) (ret *catalogHandler) {

	ret = &catalogHandler{
		CatalogHelper: helper,
		sstub:         stub,
		pulls:         pullWorks{m: make(map[CatalogPeerPolicies]*model.Puller)},
	}

	id, status := helper.SelfStatus()

	ret.model = model.NewGossipModel(
		&model.Peer{
			Id:     id,
			Status: status,
		})

	return
}

type sessionHandler struct {
	*catalogHandler
	cpo CatalogPeerPolicies
}

//implement of pushhelper and pullerhelper
func (h *sessionHandler) EncodeDigest(model.Digest) proto.Message {

	msg := &pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: h.Name(),
		M:       &pb.Gossip_Dig{h.CatalogHelper.EncodeDigest(m)},
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

	h.pulls.endPuller(h.cpo)
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
				h.schedulePush()
			} else {
				h.cpo.RecordViolation(fmt.Errorf("Passive pulling fail: %s", err))
			}
		})
		return pos
	}
}

func (h *catalogHandler) SelfUpdate(s model.Status) {

	//	h.model.LocalUpdate(s)

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

	puller := h.checkPuller(cpo)
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

func (h *catalogHandler) schedulePush() {

	logger.Debug("Schedule a push process")

	wctx := h.schedule.newSchedule(context.Background(), h.GetPolicies().PushCount())
	if wctx == nil {
		logger.Debug("Reseting a running schedule and left")
		return
	}
	defer h.schedule.endSchedule()

	for strm := range h.sstub.OverAllHandlers(wctx) {

		if h.schedule.pendingPush() <= 0 {
			break
		}

		ph, ok := stub.ObtainHandler(strm.StreamHandler).(*handlerImpl)
		if !ok {
			panic("type error, not handlerImpl")
		}
		cpo := ph.GetPeerPolicy()

		if pos := h.pulls.newPuller(cpo); pos != nil {
			pos.Puller = model.NewPuller(h, strm, h.model)
			pctx, _ := context.WithTimeout(wctx, time.Duration(h.GetPolicies().PullTimeout())*time.Second)
			err := pos.Puller.Process(pctx)

			if err != nil {
				cpo.RecordViolation(fmt.Errorf("Aggressive pulling fail: %s", err))
			} else {
				logger.Debug("Scheduled pulling from peer [%s]", strm.GetName())
			}
		}
	}

	if h.schedule.pendingPush() > 0 {
		break
	}
}

// func (h *catalogHandler) checkPuller(cpo CatalogPeerPolicies) *model.Puller {

// 	h.pulls.Lock()
// 	defer h.pulls.Unlock()
// 	return h.pulls.m[cpo]
// }

// func (h *catalogHandler) newPuller(cpo CatalogPeerPolicies, stream *pb.StreamHandler) (*model.Puller, error) {
// 	h.pulls.Lock()
// 	defer h.pulls.Unlock()

// 	if _, ok := h.pulls.m[cpo]; ok {
// 		return nil, fmt.Errorf("Puller task exist")
// 	}

// 	//TODO: we may hook EncodeDigest by provided a custom pullhelper which known cpo
// 	//so we can call pushdata method within it
// 	puller := model.NewPullTask(h, h.model, stream)
// 	h.pulls.m[cpo] = puller

// 	return puller, nil
// }

// //a blocking function
// func (h *catalogHandler) runAggresivePullTask(cpo CatalogPeerPolicies, stream *pb.StreamHandler) error {

// 	puller, err := h.newPuller(cpo, stream)
// 	if err != nil {
// 		return err
// 	}

// 	return h.runPullTask(cpo, puller)
// }
