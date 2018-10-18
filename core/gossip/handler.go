package gossip

import (
	pb "github.com/abchain/fabric/protos"
)

var ObtainHandler func(*pb.StreamHandler) GossipHandler

type peerPoliciesWrapper struct {
	*pb.PeerID
	PeerPolicies
}

func (w peerPoliciesWrapper) GetPeer() *pb.PeerID {
	return w.PeerID
}

type handlerImpl struct {
	ppo   peerPoliciesWrapper
	cores map[string]CatalogHandler
}

func newHandler(peer *pb.PeerID, handlers map[string]CatalogHandler) *handlerImpl {

	return &handlerImpl{
		ppo:   peerPoliciesWrapper{peer, NewPeerPolicy(peer.GetName())},
		cores: handlers,
	}
}

func (g *handlerImpl) Stop() {

}

func (g *handlerImpl) GetPeerPolicy() CatalogPeerPolicies {
	return g.ppo
}

func (g *handlerImpl) HandleMessage(msg *pb.Gossip) error {

	global, ok := g.cores[msg.GetCatalog()]
	if !ok {
		logger.Error("Recv gossip message with catelog not recognized: ", msg.GetCatalog())
		return nil
	}

	g.ppo.RecvUpdate(msg.EstimateSize())

	if dig := msg.GetDig(); dig != nil {
		global.HandleDigest(dig, g.ppo)
	} else {
		global.HandleUpdate(msg.GetUd(), g.ppo)
	}

	return g.ppo.IsPolicyViolated()
}
