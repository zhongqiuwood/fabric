package gossip

import (
	"fmt"
	model "github.com/abchain/fabric/core/gossip/model"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
)

type handlerImpl struct {
	peer      *pb.PeerID
	catalogHs map[string]*catalogHandler
	inners    map[*catalogHandler]*model.NeighbourPeer
	sstub     *pb.StreamStub
}

func newHandler(peer *pb.PeerID, stub *pb.StreamStub, handlers map[string]*catalogHandler) *handlerImpl {

	inners := make(map[*catalogHandler]*model.NeighbourPeer)
	for _, h := range handlers {
		inners[h] = h.newNeighbourPeer(peer.Name)
	}

	return &handlerImpl{
		peer:      peer,
		catalogHs: handlers,
		sstub:     stub,
		inners:    inners,
	}
}

func (g *handlerImpl) Stop() {}

func (g *handlerImpl) HandleMessage(msg *pb.Gossip) error {

	global := g.catalogHs[msg.GetCatalog()]
	if global == nil {
		logger.Errorf("Recv gossip message with catelog not recognized: ", msg.GetCatalog())
		return nil
	}

	inner := g.inners[global]
	if inner == nil {
		panic("corresponding inner object is not generated")
	}

	if msg.GetIsPull() {

		strms := g.sstub.PickHandlers([]*pb.PeerID{g.peer})
		if len(strms) != 1 {
			//no stream, we just giveup
			return fmt.Errorf("No stream found for peer %s", g.peer.Name)
		}
		strm := strms[0]

		//handling pulling request
		//also try to stimulate a pull process first
		if !inner.IsPulling() {
			puller, err := model.NewPullTask(global, strm, inner)
			if err != nil {
				logger.Error("Fail to create puller", err)
			} else {
				go puller.Process(context.TODO())
			}
		}

		dgtmp := make(map[string]model.Digest)
		for k, d := range msg.Dig.Data {
			dgtmp[k] = d
		}
		reply := inner.AcceptDigest(dgtmp)

		//send reply update message
		//NOTICE: if stream is NOT enable to drop message, send in HandMessage
		//may cause a deadlock, but in gossip package this is OK
		strm.SendMessage(global.buildUpdate(reply))

	} else if msg.Payload != nil {
		//handling pushing request, for the trustable process, merging
		//digest first
		for id, dig := range msg.Dig.Data {
			//verify digest
			//global.Verify()
			oldDig := global.digestCache[id]
			global.digestCache[id] = global.MergeProtoDigest(oldDig, dig)
		}

		ud, err := global.DecodeUpdate(msg.Payload)
		if err != nil {
			//It was not fatal. Log it but don't return
			logger.Errorf("Decode update for catelog %s fail: %s", msg.Catalog, err)
			return nil
		}

		inner.AcceptUpdate(ud)
	}

	return nil
}
