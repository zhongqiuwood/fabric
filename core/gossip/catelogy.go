package gossip

import (
	model "github.com/abchain/fabric/core/gossip/model"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
)

//act like model.NeighbourHelper but it was "catalogy-wide" (i.e. for all neighbours)
type CatalogHelper interface {
	Name() string
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

type catelogPeerUpdate struct {
	invoked []string
	model.Update
}

//overload Add
func (u *catelogPeerUpdate) Add(id string, s_in model.Status) model.Update {

	if s_in == nil {
		return u
	}

	u.invoked = append(u.invoked, id)

	u.Update = u.Add(id, s_in)
	return u
}

type catalogHandler struct {
	CatalogHelper
	GossipCrypto
	model *model.Model

	//for "trustable" gossip, each update MUST accompany with a trustable digest
	//we can not build other's digest so we must cache the latest one
	digestCache map[string]*pb.Gossip_Digest_PeerState
}

func newCatelogHandler(self string, crypto GossipCrypto, helper CatalogHelper) (ret *catalogHandler) {

	ret = &catalogHandler{
		GossipCrypto:  crypto,
		CatalogHelper: helper,
	}

	ret.model = model.NewGossipModel(ret,
		&model.Peer{
			Id:     self,
			Status: helper.SelfStatus(),
		})

	return
}

//implement for modelHelper
func (h *catalogHandler) AcceptPeer(id string, d model.Digest) (*model.Peer, error) {

	//TODO: now we always accept
	logger.Infof("Catelog %s now know peer %s", h.Name(), id)

	return &model.Peer{Id: id,
		Status: h.AssignStatus(),
	}, nil
}

//implement for a PullerHelper
func (h *catalogHandler) FailPulling(id string) {

}

func (h *catalogHandler) OnRecvUpdate(model.Update) error {
	return nil
}

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

func (h *catalogHandler) buildUpdate(ud_in model.Update) proto.Message {

	//A trustable update must be enclosed with a signed digest
	ud, ok := ud_in.(*catelogPeerUpdate)
	if !ok {
		panic("wrong type, not catelogPeerUpdate")
	}

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
}

func (h *catalogHandler) newNeighbourPeer(id string) *model.NeighbourPeer {

	return model.NewNeighbourPeer(h.model, h.AssignNeighbourHelper(id))
}
