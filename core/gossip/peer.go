package gossip

import (
	"fmt"
	"github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("gossip")

type Gossip interface {
	BroadcastTx(*pb.Transaction) error
}

type GossipStub struct {
	*pb.StreamStub
	peer.Discoverer
}

type GossipHandler struct {
	peerId *pb.PeerID
}

func init() {
	stub.DefaultFactory = func(id *pb.PeerID) stub.GossipHandler {
		logger.Debug("create handler for peer", id)
		return &GossipHandler{peerId: id}
	}
}

func (t *GossipHandler) HandleMessage(m *pb.Gossip) error {
	return nil
}

func (t *GossipHandler) Stop() {

}

var gossipStub *GossipStub

//init the singleton of gossipstub
func NewGossip(p peer.Peer) {

	nb, err := p.GetNeighbour()
	logger.Debug("Gossip module inited")

	if err != nil {

		logger.Errorf("No neighbour for this peer (%s), gossip run without access control", err)
		gossipStub = &GossipStub{
			StreamStub: p.GetStreamStub("gossip"),
		}

	} else {

		dis, err := nb.GetDiscoverer()
		if err != nil {
			logger.Errorf("No discovery for this peer (%s), gossip run without access control", err)
		}

		gossipStub = &GossipStub{
			StreamStub: p.GetStreamStub("gossip"),
			Discoverer: dis,
		}
	}

}

// GetGossip - gives a reference to a 'singleton' GossipStub
func GetGossip() Gossip {

	return gossipStub
}

func (s *GossipStub) BroadcastTx(*pb.Transaction) error {
	return fmt.Errorf("No implement")
}
