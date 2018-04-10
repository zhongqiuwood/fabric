package gossip

import (
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("gossip")

type GossipHandler struct {
}

func (t *GossipHandler) GossipIn(pb.Peer_GossipInServer) error {
	return nil
}
