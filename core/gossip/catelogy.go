package gossip

import (
	model "github.com/abchain/fabric/core/gossip/model"
	pb "github.com/abchain/fabric/protos"
)

//act like model.NeighbourHelper but it was "catalogy-wide" (i.e. for all neighbours)
type CatalogHelper interface {
	Name() string
	SelfStatus() model.Status
	AssignStatus() model.Status
	AllowPushUpdate(string) (model.Update, error)

	EncodeUpdate(model.Update) ([]byte, error)
	DecodeUpdate([]byte) (model.Update, error)
	ToProtoDigest(model.Digest) *pb.Gossip_Digest_PeerState
}

//catalogyHandler struggle to provide a "trustable" gossip implement 
//base on following purpose:
//1. A digest expressed by pb.Gossip_Digest_PeerState provided a "up-limit"
//   status for any update, so evil peer could not trick other by making
//   fake updates and forbidden any "real" update being applied

type catelogPeerUpdate struct{
	invoked []string
	model.Update
}

//overload Add 
func (u *catelogPeerUpdate) Add(id string, s_in model.Status) Update {

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
	sstub *pb.StreamStub

	//for "trustable" gossip, each update MUST accompany with a trustable digest
	//we can not build other's digest so we must cache the latest one
	digestCache map[string]*pb.Gossip_Digest_PeerState
}

func newCatelogHandler(self string, s *pb.StreamStub, 
	crypto GossipCrypto, helper CatalogHelper) (ret *catelogHandler) {

	ret = &catelogHandler{
		sstub:          s,
		GossipCrypto:	crypto,
		CatalogyHelper: helper,
	}

	ret.model = model.NewGossipModel(ret, 
		&model.Peer{
			Id: self,
			Status: helper.SelfStatus(),
		})

	return
}

func (h *catelogHandler) AcceptPeer(id string, Digest) (*model.Peer, error) {

	//TODO: now we always accept
	logger.Infof("Catelog %s now know peer %s", h.Name(), id)

	return &model.Peer{Id: id,
		Status: h.AssignStatus(),
	}, nil
}
