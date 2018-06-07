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
	SelfUpdate(model.Digest)
	HandleUpdate(*pb.PeerID, *pb.Gossip, CatalogPeerPolicies)
	HandleDigest(*pb.PeerID, *pb.Gossip, CatalogPeerPolicies)
	AssignPeerPolicy() CatalogPeerPolicies
	GetPolicies() CatalogPolicies
}

type CatalogPeerPolicies interface {
	AllowPushUpdate() bool
	RecvUpdate(int)
	PushUpdate(int)
	Stop()
}

type CatalogHelper interface {
	Name() string
	GetPolicies() CatalogPolicies //caller do not need to check the interface

	SelfStatus() TrustableStatus
	AssignStatus() TrustableStatus
	AssignUpdate() model.Update
	AssignPeerPolicy() CatalogPeerPolicies

	EncodeUpdate(model.Update) ([]byte, error)
	DecodeUpdate([]byte) (model.Update, error)
	ToProtoDigest(model.Digest) *pb.Gossip_Digest_PeerState
	MergeProtoDigest(*pb.Gossip_Digest_PeerState, *pb.Gossip_Digest_PeerState) *pb.Gossip_Digest_PeerState
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
	m map[string]*model.Puller
}

type scheduleWorks struct {
	endSchedule context.CancelFunc
	expectCnt   int
	sync.Mutex
}

type catalogHandler struct {
	CatalogHelper
	GossipCrypto
	model    trustableModel
	pulls    pullWorks
	schedule scheduleWorks
	sstub    *pb.StreamStub
}

func NewCatalogHandlerImpl(self string, stub *pb.StreamStub,
	crypto GossipCrypto, helper CatalogHelper) (ret *catalogHandler) {

	ret = &catalogHandler{
		GossipCrypto:  crypto,
		CatalogHelper: helper,
		sstub:         stub,
		pulls:         pullWorks{m: make(map[string]*model.Puller)},
	}

	ret.model = newTrustableModel(ret,
		&model.Peer{
			Id:     self,
			Status: helper.SelfStatus(),
		})

	return
}

func (h *catalogHandler) HandleDigest(peer *pb.PeerID, msg *pb.Gossip, cpo CatalogPeerPolicies) {

	strm := h.sstub.PickHandler(peer)
	if strm != nil {
		logger.Errorf("No stream found for %s", peer.Name)
		return
	}

	var isPassivePull bool
	if cpo.AllowPushUpdate() {
		//we try to stimulate a pull process first
		puller, err := h.newPuller(peer, strm)
		if err != nil {
			isPassivePull = true
			go h.runPassiveTask(peer, puller)
		}
	}

	//digest need to be transformed
	dgtmp := make(map[string]model.Digest)
	for k, d := range msg.Dig.Data {
		dgtmp[k] = d
	}
	//A trustable update must be enclosed with a signed digest
	ud, ok := h.model.RecvPullDigest(dgtmp,
		&catalogPeerUpdate{Update: h.AssignUpdate()}).(*catalogPeerUpdate)
	if !ok {
		panic("wrong type, not catalogPeerUpdate")
	}

	ret := &pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: h.Name(),
		Dig:     &pb.Gossip_Digest{make(map[string]*pb.Gossip_Digest_PeerState)},
	}

	vDigests := h.model.GetVerifiedDigest()

	//copy the verified digest into message
	for _, id := range ud.invoked {
		ret.Dig.Data[id] = vDigests[id].(*pb.Gossip_Digest_PeerState)
	}

	payloadByte, err := h.EncodeUpdate(ud.Update)
	if err == nil {
		ret.Payload = payloadByte
		cpo.PushUpdate(len(payloadByte))
	} else {
		logger.Error("Encode update failure:", err)
	}

	//send reply update message, send it even on encode failure!
	//NOTICE: if stream is NOT enable to drop message, send in HandMessage
	//may cause a deadlock, but in gossip package this is OK
	strm.SendMessage(ret)

	if isPassivePull {
		h.addOneUpdate()
	}

}

func (h *catalogHandler) SelfUpdate(vdigest model.Digest) {

	var err error
	vd := h.ToProtoDigest(vdigest)
	vd, err = h.Sign(h.Name(), vd)
	if err != nil {
		logger.Error("Sign digest fail:", err)
		return
	}

	h.model.UpdateProofDigest(map[string]model.Digest{h.model.GetSelf(): vd})

	go h.schedulePush()
}

func (h *catalogHandler) HandleUpdate(peer *pb.PeerID, msg *pb.Gossip, cpo CatalogPeerPolicies) {

	cpo.RecvUpdate(len(msg.Payload))

	puller := h.checkPuller(peer)
	if puller == nil {
		//something wrong! if no corresponding puller, we never handle this message
		logger.Errorf("No puller in catalog %s avaiable for peer %s", msg.Catalog, peer.Name)
		return
	}

	ud, err := h.DecodeUpdate(msg.Payload)
	if err != nil {
		logger.Errorf("Decode update for catalog %s fail: %s", msg.Catalog, err)
		return
	}

	//test verified digest
	newDigest := make(map[string]model.Digest)
	curDigest := h.model.GetVerifiedDigest()
	for id, dig := range msg.Dig.Data {
		oldDig, ok := curDigest[id].(*pb.Gossip_Digest_PeerState)
		if ok {
			dig = h.MergeProtoDigest(oldDig, dig)
		}

		if oldDig != dig {
			//TODO digest is new, check if it was verified!
			//verify digest
			if h.Verify(id, h.Name(), dig) {
				newDigest[id] = dig
			}
		}
	}

	//handling pushing request, for the trustable process, update verified
	//part first
	h.model.UpdateProofDigest(newDigest)

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

	ret := &pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: h.Name(),
		Dig:     &pb.Gossip_Digest{make(map[string]*pb.Gossip_Digest_PeerState)},
		IsPull:  true,
	}

	//digest sent in pulling request do NOT need to be trustable
	//(wrong digest only harm peer itself)
	for k, d := range m {
		pbd := h.ToProtoDigest(d)
		ret.Dig.Data[k] = pbd
	}

	return ret
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
		err := h.runAggresivePullTask(strm.Id, strm.StreamHandler)
		if err != nil {
			logger.Infof("Aggressive pulling to peer %s fail: %s", strm.Id.Name, err)
		}
	}

	h.schedule.Lock()
	h.schedule.endSchedule = nil
}

func (h *catalogHandler) checkPuller(peer *pb.PeerID) *model.Puller {

	h.pulls.Lock()
	defer h.pulls.Unlock()
	return h.pulls.m[peer.Name]
}

func (h *catalogHandler) newPuller(peer *pb.PeerID, stream *pb.StreamHandler) (*model.Puller, error) {
	h.pulls.Lock()
	defer h.pulls.Unlock()

	if _, ok := h.pulls.m[peer.Name]; ok {
		return nil, fmt.Errorf("Puller task exist")
	}

	puller := model.NewPullTask(h, h.model.Model, stream)
	h.pulls.m[peer.Name] = puller

	return puller, nil
}

func (h *catalogHandler) runPullTask(peer *pb.PeerID, puller *model.Puller) (ret error) {

	pctx, _ := context.WithTimeout(context.Background(),
		time.Duration(h.GetPolicies().PullTimeout())*time.Second)
	ret = puller.Process(pctx)

	//end, clear puller task
	h.pulls.Lock()
	delete(h.pulls.m, peer.Name)
	h.pulls.Unlock()

	return
}

func (h *catalogHandler) runPassiveTask(peer *pb.PeerID, puller *model.Puller) {
	err := h.runPullTask(peer, puller)

	//when we succefully accept an update, we also schedule a new push process
	if err == nil {
		h.schedulePush()
	} else {
		logger.Infof("Passive pulling to peer %s fail: %s", peer.Name, err)
	}
}

//a blocking function
func (h *catalogHandler) runAggresivePullTask(peer *pb.PeerID, stream *pb.StreamHandler) error {

	puller, err := h.newPuller(peer, stream)
	if err != nil {
		return err
	}

	return h.runPullTask(peer, puller)
}
