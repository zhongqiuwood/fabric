package gossip

import (
	model "github.com/abchain/fabric/core/gossip/model"

	pb "github.com/abchain/fabric/protos"
	"sync"
	"time"
)

type updateImpl struct {
	model.Update //update provided from CatalogyHelper
	withDigest   bool
	needPulling  bool
}

type handlerImpl struct {
	global *catalogHandler
	inner  *model.NeighbourPeer
}

func newHandler(catalogH *catalogHandler) *handlerImpl {
	h := &handlerImpl{
		global:      catalogH,
		digestCache: make(map[string]*pb.Gossip_Digest_PeerState),
	}

	h.inner = model.NewNeighbourPeer(catalogH.model, h)

}

func (g *handlerImpl) Stop() {}

func (g *handlerImpl) HandleMessage(msg *pb.Gossip) {

	//the sequence is somewhat subtle: we expect different behavior
	//on reply update (with or without an digest, and set IsPull flag)
	//depending on if handler is under a "Pulling" process. The pulling
	//status will be clear after AcceptUpdate is called so we must handle
	//digest first
	if msg.Dig != nil && len(msg.Dig.Data) > 0 {
		dgtmp := make(map[string]model.Digest)
		for k, d := range msg.Dig.Data {
			dgtmp[k] = d
		}
		g.inner.ReplyUpdate(dgtmp)
	}

	if msg.Payload != nil {
		ud, err := g.DecodeUpdate(msg.Payload)
		if err != nil {
			//It was not fatal. Log it but don't return
			logger.Errorf("Decode update for catelog %s fail: %s", msg.Catalog, err)
			return
		}

		g.inner.AcceptUpdate(ud)
	}

	return
}

func (g *handlerImpl) AllowPushUpdate() (model.Update, error) {

	err := g.CatelogyHelper.AllowSendUpdate(id)
	if err != nil {
		return nil, err
	}

	ret := &updateImpl{
		//digest can be omitted if we are under pulling (we have just sent one)
		withDigest: !g.inner.IsPulling(),
	}

	//if we are not response for
	if !g.inner.IsPulling() {
		ret.withDigest = true
	}

	return ret, nil
}

//we have three different models for sync peer's current state, "hot" (un-commited)
//transactions and commited transactions
func (g *handlerImpl) EncodeDigest(m map[string]model.Digest) proto.Message {

	ret := &pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: g.Name(),
		Dig:     &pb.Gossip_Digest{make(map[string]*pb.Gossip_Digest_PeerState)},
	}

	for k, d := range m {
		pbd := g.ToProtoDigest(d)
		ret.Dig.Data[k] = pbd
	}

	return ret
}

func (g *handlerImpl) EncodeUpdate(ud_in model.Update) proto.Message {

	ud, ok := ud_in.(*updateImpl)
	if !ok {
		panic("wrong type, not updateImpl")
	}

	ret := &pb.Gossip{
		Seq:     getGlobalSeq(),
		Catalog: g.Name(),
	}

	if ud.withDigest {
		ret.Dig = g.digestCache
	}

	payloadByte, err := g.global.EncodeUpdate(u)
	if err == nil {
		ret.Payload = payloadByte
	} else {
		logger.Error("Encode update failure:", err)
	}

	return ret
}
