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
}

type CatalogHelper interface {
	Name() string
	GetPolicies() CatalogPolicies //caller do not need to check the interface

	SelfStatus() model.Status
	AssignStatus() model.Status
	AssignUpdate(CatalogPeerPolicies) model.Update

	EncodeUpdate(model.Update) ([]byte, error)
	DecodeUpdate([]byte) (model.Update, error)
	ToProtoDigest(model.Digest) *pb.Gossip_Digest_PeerState
}

type catalogPeerUpdate struct {
	invoked []string
	model.Update
}

//overload Add
func (u *catalogPeerUpdate) Add(id string, s_in model.Status) model.Update {

	if s_in == nil {
		return u
	}

	u.invoked = append(u.invoked, id)

	u.Update = u.Add(id, s_in)
	return u
}

type pullWorks struct {
	sync.Mutex
	m map[CatalogPeerPolicies]*model.Puller
}

type scheduleWorks struct {
	endSchedule context.CancelFunc
	expectCnt   int
	sync.Mutex
}

type catalogHandler struct {
	CatalogHelper
	model    *model.Model
	pulls    pullWorks
	schedule scheduleWorks
	sstub    *pb.StreamStub
}

func NewCatalogHandlerImpl(self string, stub *pb.StreamStub, helper CatalogHelper) (ret *catalogHandler) {

	ret = &catalogHandler{
		CatalogHelper: helper,
		sstub:         stub,
		pulls:         pullWorks{m: make(map[CatalogPeerPolicies]*model.Puller)},
	}

	ret.model = model.NewGossipModel(ret,
		&model.Peer{
			Id:     self,
			Status: helper.SelfStatus(),
		})

	return
}

func (h *catalogHandler) HandleDigest(peer *pb.PeerID, msg *pb.Gossip_Digest, cpo CatalogPeerPolicies) {

	strm := h.sstub.PickHandler(peer)
	if strm != nil {
		logger.Errorf("No stream found for %s", peer.Name)
		return
	}

	var isPassivePull bool
	if cpo.AllowRecvUpdate() {
		//we try to stimulate a pull process first
		puller, err := h.newPuller(cpo, strm)
		if err != nil {
			isPassivePull = true
			go h.runPassiveTask(cpo, puller)
		}
	}

	//digest need to be transformed
	dgtmp := make(map[string]model.Digest)
	for k, d := range msg.Data {
		dgtmp[k] = d
	}
	//A trustable update must be enclosed with a signed digest
	ud := h.model.RecvPullDigest(dgtmp, h.AssignUpdate(cpo))

	udsent := &pb.Gossip_Update{}
	payloadByte, err := h.EncodeUpdate(ud)
	if err == nil {
		udsent.Payload = payloadByte
		cpo.PushUpdate(len(payloadByte))
	} else {
		logger.Error("Encode update failure:", err)
	}

	//send reply update message, send it even on encode failure!
	//NOTICE: if stream is NOT enable to drop message, send in HandMessage
	//may cause a deadlock, but in gossip package this is OK
	strm.SendMessage(&pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: h.Name(),
		M:       &pb.Gossip_Ud{udsent},
	})

	if isPassivePull {
		h.addOneUpdate()
	}

}

func (h *catalogHandler) SelfUpdate(s model.Status) {

	h.model.LocalUpdate(s)

	go h.schedulePush()
}

func (h *catalogHandler) HandleUpdate(peer *pb.PeerID, msg *pb.Gossip_Update, cpo CatalogPeerPolicies) {

	puller := h.checkPuller(cpo)
	if puller == nil {
		//something wrong! if no corresponding puller, we never handle this message
		logger.Errorf("No puller in catalog %s avaiable for peer %s", h.Name(), peer.Name)
		return
	}

	ud, err := h.DecodeUpdate(msg.Payload)
	if err != nil {
		cpo.RecordViolation(fmt.Errorf("Decode update for catalog %s fail: %s", h.Name(), err))
		return
	}

	//the final update is executed in another thread
	puller.NotifyUpdate(ud)

}

//implement for modelHelper
func (h *catalogHandler) AcceptPeer(id string, d model.Digest) (*model.Peer, error) {

	//TODO: now we always accept
	logger.Infof("Catalog %s now know peer %s", h.Name(), id)

	return &model.Peer{Id: id,
		Status: h.AssignStatus(),
	}, nil
}

//implement for a PullerHelper
func (h *catalogHandler) EncodeDigest(m map[string]model.Digest) proto.Message {

	ret := &pb.Gossip_Digest{}

	//digest sent in pulling request do NOT need to be trustable
	//(wrong digest only harm peer itself)
	for k, d := range m {
		pbd := h.ToProtoDigest(d)
		ret.Data[k] = pbd
	}

	return &pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: h.Name(),
		M:       &pb.Gossip_Dig{ret},
	}
}

func (h *catalogHandler) addOneUpdate() {

	h.schedule.Lock()
	if h.schedule.endSchedule != nil {
		h.schedule.expectCnt--
		if h.schedule.expectCnt <= 0 {
			//done
			h.schedule.endSchedule()
		}
	}

	h.schedule.Unlock()
}

func (h *catalogHandler) schedulePush() {

	logger.Debug("Schedule a push process")

	h.schedule.Lock()
	defer h.schedule.Unlock()

	if h.schedule.endSchedule != nil {
		//cancel last schedule and use a new one
		h.schedule.endSchedule()
	}

	wctx, cancelF := context.WithCancel(context.Background())
	h.schedule.endSchedule = cancelF
	h.schedule.expectCnt = h.GetPolicies().PushCount()
	h.schedule.Unlock()

	for strm := range h.sstub.OverAllHandlers(wctx) {

		ph, ok := stub.ObtainHandler(strm.StreamHandler).(*handlerImpl)
		if !ok {
			panic("type error, not handlerImpl")
		}
		cpo := ph.GetPeerPolicy()

		err := h.runAggresivePullTask(cpo, strm.StreamHandler)
		if err != nil {
			cpo.RecordViolation(fmt.Errorf("Aggressive pulling fail: %s", err))
		}
	}

	h.schedule.Lock()
	h.schedule.endSchedule = nil
}

func (h *catalogHandler) checkPuller(cpo CatalogPeerPolicies) *model.Puller {

	h.pulls.Lock()
	defer h.pulls.Unlock()
	return h.pulls.m[cpo]
}

func (h *catalogHandler) newPuller(cpo CatalogPeerPolicies, stream *pb.StreamHandler) (*model.Puller, error) {
	h.pulls.Lock()
	defer h.pulls.Unlock()

	if _, ok := h.pulls.m[cpo]; ok {
		return nil, fmt.Errorf("Puller task exist")
	}

	//TODO: we may hook EncodeDigest by provided a custom pullhelper which known cpo
	//so we can call pushdata method within it
	puller := model.NewPullTask(h, h.model, stream)
	h.pulls.m[cpo] = puller

	return puller, nil
}

func (h *catalogHandler) runPullTask(cpo CatalogPeerPolicies, puller *model.Puller) (ret error) {

	pctx, _ := context.WithTimeout(context.Background(),
		time.Duration(h.GetPolicies().PullTimeout())*time.Second)
	ret = puller.Process(pctx)

	//end, clear puller task
	h.pulls.Lock()
	delete(h.pulls.m, cpo)
	h.pulls.Unlock()

	return
}

func (h *catalogHandler) runPassiveTask(cpo CatalogPeerPolicies, puller *model.Puller) {
	err := h.runPullTask(cpo, puller)

	//when we succefully accept an update, we also schedule a new push process
	if err == nil {
		h.schedulePush()
	} else {
		cpo.RecordViolation(fmt.Errorf("Passive pulling fail: %s", err))
	}
}

//a blocking function
func (h *catalogHandler) runAggresivePullTask(cpo CatalogPeerPolicies, stream *pb.StreamHandler) error {

	puller, err := h.newPuller(cpo, stream)
	if err != nil {
		return err
	}

	return h.runPullTask(cpo, puller)
}
