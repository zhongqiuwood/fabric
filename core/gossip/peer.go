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
	model   *Model
	peerIDs map[string]int
}

type GossipHandler struct {
	peerId *pb.PeerID
}

func init() {
	stub.DefaultFactory = func(id *pb.PeerID) stub.GossipHandler {
		logger.Debug("create handler for peer", id)
		gossipStub.peerIDs[id.String()] = 1
		return &GossipHandler{peerId: id}
	}
}

func (t *GossipHandler) HandleMessage(m *pb.Gossip) error {
	if m.GetDigest() != nil {
		gossipStub.model.applyUpdate(m)
	} else if m.GetUpdate() != nil {
		// process update
	}
	return nil
}

func (t *GossipHandler) Stop() {

}

var gossipStub *GossipStub

//init the singleton of gossipstub
func NewGossip(p peer.Peer) {

	nb, err := p.GetNeighbour()
	logger.Debug("Gossip module inited")

	model := &Model{
		merger: &VersionMergerDummy{},
	}
	if err != nil {

		logger.Errorf("No neighbour for this peer (%s), gossip run without access control", err)
		gossipStub = &GossipStub{
			StreamStub: p.GetStreamStub("gossip"),
			model:      model,
		}

	} else {

		dis, err := nb.GetDiscoverer()
		if err != nil {
			logger.Errorf("No discovery for this peer (%s), gossip run without access control", err)
		}

		gossipStub = &GossipStub{
			StreamStub: p.GetStreamStub("gossip"),
			Discoverer: dis,
			model:      model,
		}
	}
	gossipStub.peerIDs = map[string]int{}

}

// GetGossip - gives a reference to a 'singleton' GossipStub
func GetGossip() Gossip {

	return gossipStub
}

func (s *GossipStub) BroadcastTx(tx *pb.Transaction) error {
	s.model.updateSelf("tx", tx.Txid) // query block hash as tx.Txid
	return fmt.Errorf("No implement")
}
