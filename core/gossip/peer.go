package gossip

import (
	"github.com/abchain/fabric/core/gossip/stub"
	_ "github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("gossip")

type GossipHandler struct {
}

func GetGossip() {

	logger.Debug("Gossip module inited")

	stub.DefaultFactory = func(id *pb.PeerID) stub.GossipHandler {
		logger.Debug("create handler for peer", id)
		return &GossipHandler{}
	}
}

func (t *GossipHandler) HandleMessage(m *pb.Gossip) error {
	return nil
}

func (t *GossipHandler) Stop() {

}
