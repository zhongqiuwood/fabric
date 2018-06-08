package gossip

import (
	pb "github.com/abchain/fabric/protos"
)

type handlerImpl struct {
	peer  *pb.PeerID
	ppo   *peerPolicies
	cores map[string]CatalogHandler
}

func newHandler(peer *pb.PeerID, handlers map[string]CatalogHandler) *handlerImpl {

	return &handlerImpl{
		peer:  peer,
		ppo:   newPeerPolicy(peer.GetName()),
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
		logger.Errorf("Recv gossip message with catelog not recognized: ", msg.GetCatalog())
		return nil
	}

	g.ppo.recvUpdate(msg.EstimateSize())

	if dig := msg.GetDig(); dig != nil {
		global.HandleDigest(g.peer, dig, g.ppo)
	} else {
		global.HandleUpdate(g.peer, msg.GetUd(), g.ppo)
	}

	return g.ppo.isPolicyViolated()
}
