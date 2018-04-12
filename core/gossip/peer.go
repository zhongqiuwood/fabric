package gossip

import (
	"github.com/abchain/fabric/peer"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("gossip")

type GossipStub struct {
	peer.Peer
}

type gossipHandler struct {
	*GossipStub
}

func (t *GossipStub) GossipStart(stream pb.Peer_GossipInServer) error {

	return nil
}

func (t *GossipStub) GossipIn(stream pb.Peer_GossipInServer) error {

	return nil
}
