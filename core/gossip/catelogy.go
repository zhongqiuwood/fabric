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
	SelfStatus() model.Status
	AssignStatus() model.Status
	AssignNeighbourHelper(string) model.NeighbourHelper

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

type catalogVerifyDigest struct {
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
	m map[*pb.StreamHandler]*model.Puller
}

type catalogHandler struct {
	CatalogHelper
	GossipCrypto
	model trustableModel
	pulls pullWorks
}

func newCatalogHandler(self string, crypto GossipCrypto, helper CatalogHelper) (ret *catalogHandler) {

	ret = &catalogHandler{
		GossipCrypto:  crypto,
		CatalogHelper: helper,
		model:         trustableModel{VerifiedDigest: make(map[string]trustableDigest)},
		pulls:         pullWorks{m: make(map[*pb.StreamHandler]*model.Puller)},
	}

	ret.model.Model = model.NewGossipModel(ret,
		&model.Peer{
			Id:     self,
			Status: helper.SelfStatus(),
		})

	return
}

func (h *catalogHandler) checkPuller(strm *pb.StreamHandler) *model.Puller {
	h.pulls.Lock()
	defer h.pulls.Unlock()
	return h.pulls.m[strm]
}

func (h *catalogHandler) HandleDigest(strm *pb.StreamHandler, msg *pb.Gossip) {

	//digest need to be transformed
	dgtmp := make(map[string]model.Digest)
	for k, d := range msg.Dig.Data {
		dgtmp[k] = d
	}
	reply := h.model.RecvPullDigest(dgtmp)

	//A trustable update must be enclosed with a signed digest
	ud, ok := ud_in.(*catalogPeerUpdate)
	if !ok {
		panic("wrong type, not catalogPeerUpdate")
	}

	//try to stimulate a pull process first
	go global.runPullTask(context.Background(), strm)

	ret := &pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: h.Name(),
		Dig:     &pb.Gossip_Digest{make(map[string]*pb.Gossip_Digest_PeerState)},
	}

	for _, id := range ud.invoked {
		ret.Dig.Data[id] = h.digestCache[id]
	}

	payloadByte, err := h.EncodeUpdate(ud.Update)
	if err == nil {
		ret.Payload = payloadByte
	} else {
		logger.Error("Encode update failure:", err)
	}

	return ret

	//send reply update message
	//NOTICE: if stream is NOT enable to drop message, send in HandMessage
	//may cause a deadlock, but in gossip package this is OK
	strm.SendMessage(h.buildUpdate(reply))
}

func (h *catalogHandler) HandleUpdate(strm *pb.StreamHandler, msg *pb.Gossip) {

	puller := checkPuller(strm)
	if puller == nil {
		//something wrong! if no corresponding puller, we never handle this message
		logger.Errorf("No puller in catalog %s avaiable for stream %s", msg.Catalog, strm.GetName())
		return
	}

	ud, err := h.DecodeUpdate(msg.Payload)
	if err != nil {
		logger.Errorf("Decode update for catalog %s fail: %s", msg.Catalog, err)
		return
	}

	for id, dig := range msg.Dig.Data {
		//verify digest
		//global.Verify()
		oldDig := global.digestCache[id]
		global.digestCache[id] = global.MergeProtoDigest(oldDig, dig)
	}

	//handling pushing request, for the trustable process, update verified
	//part first
	h.model.UpdateVerifiedDigest()
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

}

//a blocking function
func (h *catalogHandler) runPullTask(ctx context.Context, stream *pb.StreamHandler) error {

	h.pulls.Lock()
	defer h.pulls.Unlock()

	if _, ok := h.pulls.m[stream]; ok {
		return fmt.Errorf("Puller task exist")
	}

	puller, err := model.NewPullTask(h, stream, h.model)
	if err != nil {
		return fmt.Errorf("Create puller fail:", err)
	}

	h.pulls.m[stream] = puller
	h.pulls.Unlock()

	pctx, _ := context.WithTimeout(ctx, h.GetPolicies().PullTimeout()*time.Second)
	puller.Process(pctx)

	//end
	h.pulls.Lock()
	delete(h.pulls.m, stream)

}

func (h *catalogHandler) buildUpdate(ud_in model.Update) proto.Message {

}
