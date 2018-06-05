package gossip

import (
	"fmt"
	pb "github.com/abchain/fabric/protos"
)

type handlerImpl struct {
	peer  *pb.PeerID
	cores map[string]*catalogHandler
}

func newHandler(peer *pb.PeerID, handlers map[string]*catalogHandler) *handlerImpl {

	return &handlerImpl{
		peer:  peer,
		cores: handlers,
	}
}

func (g *handlerImpl) Stop() {}

func (g *handlerImpl) HandleMessage(msg *pb.Gossip) error {

	global, ok := g.cores[msg.GetCatalog()]
	if !ok {
		logger.Errorf("Recv gossip message with catelog not recognized: ", msg.GetCatalog())
		return nil
	}

	if msg.GetIsPull() { //handling pulling request
		global.HandleDigest(g.peer, msg)

	} else if msg.Payload != nil {
		global.HandleUpdate(g.peer, msg)
	}

	return nil
}
