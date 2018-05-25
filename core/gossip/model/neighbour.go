package gossip_model

import (
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
)

type NeighbourHelper interface {
	//If we still allow send update to this peer (return NON-error)
	//This method also track the push-status of local
	AllowPushUpdate(string) error
	//help to encode messages
	EncodeDigest(map[string]Digest) proto.Message
	EncodeUpdate(Update) proto.Message
}

type neighbourPeer struct {
	workPuller *puller
	stream     *pb.StreamHandler
	global     *Model
	helper     NeighbourHelper
}

func NewNeighbourPeer(s *pb.StreamHandler, model *Model,
	helper NeighbourHelper) *neighbourPeer {

	nP := &neighbourPeer{
		stream: s,
		helper: helper,
		global: model,
	}
	return nP

}

//peer should reply update (that is, also "push" its status to far-end) if
//possible, ignoring any other conditions
func (nP *neighbourPeer) ReplyUpdate(dg map[string]Digest) {
	if nP.helper.AllowPushUpdate(nP.stream.GetName()) != nil {
		//simply omit this message
		return
	}

	ud := nP.global.RecvPullDigest(dg)
	//gossip allows message being dropped so we don't worry about dead-locking here
	nP.stream.SendMessage(nP.helper.EncodeUpdate(ud))
}

func (nP *neighbourPeer) IsPulling() bool {
	return nP.workPuller != nil
}

//accept update, normally it should only accept when we are under a "pull" task
//so we just send it to the puller
func (nP *neighbourPeer) AcceptUpdate(ud Update) {
	nP.workPuller.NotifyUpdate(ud)
	//**any puller (if exist) finish its task and neighbour never track it any longer**
	nP.workPuller = nil

}
