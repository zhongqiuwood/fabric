package gossip

import (
	"fmt"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
)

type handlerImpl struct {
	peer  *pb.PeerID
	cores map[string]*catalogHandler
	sstub *pb.StreamStub
}

func newHandler(peer *pb.PeerID, stub *pb.StreamStub, handlers map[string]*catalogHandler) *handlerImpl {

	return &handlerImpl{
		peer:  peer,
		cores: handlers,
		sstub: stub,
	}
}

func (g *handlerImpl) Stop() {}

func (g *handlerImpl) HandleMessage(msg *pb.Gossip) error {

	strm := g.sstub.PickHandler(g.peer)
	if strm != nil {
		return fmt.Errorf("No stream found for %s", g.peer.Name)
	}

	global, ok := g.cores[msg.GetCatalog()]
	if !ok {
		logger.Errorf("Recv gossip message with catelog not recognized: ", msg.GetCatalog())
		return nil
	}

	if msg.GetIsPull() { //handling pulling request
		global.HandleDigest(strm, msg)

	} else if msg.Payload != nil {
		global.HandleUpdate(strm, msg)
	}

	return nil
}
