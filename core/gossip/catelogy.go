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

//act like model.NeighbourHelper but it was "catalogy-wide" (i.e. for all neighbours)
type CatalogHelper interface {
	Name() string
	GetPolicies() CatalogPolicies //caller do not need to check the interface
	SelfStatus() TrustableStatus
	AssignStatus() TrustableStatus
	AssignUpdate() model.Update

	EncodeUpdate(model.Update) ([]byte, error)
	DecodeUpdate([]byte) (model.Update, error)
	ToProtoDigest(model.Digest) *pb.Gossip_Digest_PeerState
	MergeProtoDigest(*pb.Gossip_Digest_PeerState, *pb.Gossip_Digest_PeerState) *pb.Gossip_Digest_PeerState
}

//catalogyHandler struggle to provide a "trustable" gossip implement
//base on following purpose:
//1. A digest expressed by pb.Gossip_Digest_PeerState provided a "up-limit"
//   status for any update, so evil peer could not trick other by making
//   fake updates and forbidden any "real" update being applied

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
	lastSchedule context.CancelFunc
	pushCnt      int
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

func newCatalogHandler(self string, stub *pb.StreamStub,
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

func (h *catalogHandler) HandleDigest(peer *pb.PeerID, msg *pb.Gossip) {

	strm := h.sstub.PickHandler(peer)
	if strm != nil {
		logger.Errorf("No stream found for %s", peer.Name)
		return
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
	} else {
		logger.Error("Encode update failure:", err)
	}

	//send reply update message, send it even on encode failure!
	//NOTICE: if stream is NOT enable to drop message, send in HandMessage
	//may cause a deadlock, but in gossip package this is OK
	strm.SendMessage(ret)

	h.schedule.Lock()
	h.schedule.pushCnt++
	if h.schedule.pushCnt >= h.GetPolicies().PushCount() {
		//current push has reach expected count so we can end the schedule Task
	}
	h.schedule.Unlock()

	//finally we try to stimulate a pull process first
	go h.runPassivePullTask(peer, strm)
}

func (h *catalogHandler) HandleUpdate(peer *pb.PeerID, msg *pb.Gossip) {

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
		//verify digest
		//global.Verify()
		oldDig, ok := curDigest[id].(*pb.Gossip_Digest_PeerState)
		if ok {
			dig = h.MergeProtoDigest(oldDig, dig)
		}

		if oldDig != dig {
			//TODO digest is new, check if it was verified!
			newDigest[id] = dig
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

func (h *catalogHandler) schedulePush() {

	h.schedule.Lock()
	defer h.schedule.Unlock()

	if h.schedule.lastSchedule != nil {
		//cancel last schedule and use a new one
		h.schedule.lastSchedule()
	}

	wctx, cancelF := context.WithCancel(context.Background())
	h.schedule.lastSchedule = cancelF
	h.schedule.Unlock()
	requireCnt := h.GetPolicies().PushCount()

	for strm := range h.sstub.OverAllHandlers(wctx) {
		runPullTask(strm.Id, strm.StreamHandler)
	}
}

func (h *catalogHandler) checkPuller(peer *pb.PeerID) *model.Puller {

	h.pulls.Lock()
	defer h.pulls.Unlock()
	return h.pulls.m[peer.Name]
}

func (h *catalogHandler) runPassivePullTask(peer *pb.PeerID, stream *pb.StreamHandler) {
	err := h.runPullTask(peer, stream)

	if err != nil {
		h.schedulePush()
	}
}

//a blocking function
func (h *catalogHandler) runPullTask(peer *pb.PeerID, stream *pb.StreamHandler) error {

	h.pulls.Lock()
	defer h.pulls.Unlock()

	if _, ok := h.pulls.m[peer.Name]; ok {
		return fmt.Errorf("Puller task exist")
	}

	puller := model.NewPullTask(h, h.model.Model)
	h.pulls.m[peer.Name] = puller
	h.pulls.Unlock()

	pctx, _ := context.WithTimeout(context.Background(),
		time.Duration(h.GetPolicies().PullTimeout())*time.Second)
	puller.Process(pctx, stream)

	//end
	h.pulls.Lock()
	delete(h.pulls.m, peer.Name)

	return nil
}
