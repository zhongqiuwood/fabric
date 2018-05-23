package gossip

import (
	model "github.com/abchain/fabric/core/gossip/model"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

type pullerHelper interface {
	//Notify if we failed from a pulling
	FailPulling(string)
	//Notify we receive a update, if return nil, not apply this
	OnRecvUpdate(model.Update) error
	//help to encode messages
	EncodeDigest(map[string]model.Digest) proto.Message
	EncodeUpdate(model.Update) proto.Message
}

type puller struct {
	update chan Update
	pullerHelper
	stream *pb.StreamHandler
	global *model.Model
}

func newPuller(ctx context.Context, model *model.Model, helper pullerHelper,
	streams []*pb.StreamHandler) *puller {

	msg := helper.EncodeDigest(model.GenPullDigest())

	puller := &puller{
		stream:       s,
		global:       model,
		pullerHelper: helper,
		update:       make(chan Update),
	}
}

func (p *puller) process() {

}

type neighbourPeer struct {
	*fsm.FSM
	*model.Peer
	stream *pb.StreamHandler
	global *model.Model
	helper NeighbourHelper
}

func NewNeighbourPeer(id string, s *pb.StreamHandler, model *model.Model,
	helper NeighbourHelper) *neighbourPeer {

	nP := &neighbourPeer{
		stream: s,
		helper: helper,
		Peer:   model.Peers[id],
	}

	nP.FSM = fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: "startpull", Src: []string{"idle"}, Dst: "pulling"},
			{Name: "digest", Src: []string{"idle"}, Dst: "idle"},
			{Name: "digest", Src: []string{"pulling"}, Dst: "pulling"},
			{Name: "update", Src: []string{"pulling"}, Dst: "idle"},
		},
		fsm.Callbacks{
			"before_digest": func(e *fsm.Event) { nP.replyUpdate(e) },
			"after_update":  func(e *fsm.Event) { nP.acceptUpdate(e) },
			"enter_pulling": func(e *fsm.Event) { nP.sendDigest(e) },
		},
	)

	return nP

}

func (nP *neighbourPeer) replyUpdate(e *fsm.Event) {
	if nP.helper.AllowUpdate(nP.Id) != nil {
		//simply omit this message
		return
	}

	if len(e.Args) != 1 {
		panic("Wrong event arguments")
	}

	if dg, ok := e.Args[0].(map[string]model.Digest); !ok {
		panic("Wrong type of argument")
	} else {
		ud := nP.global.RecvPullDigest(dg)
		nP.stream.SendMessage(nP.helper.EncodeUpdate(ud))
	}
}

func (nP *neighbourPeer) acceptUpdate(e *fsm.Event) {

	if len(e.Args) != 1 {
		panic("Wrong event arguments")
	}

	if ud, ok := e.Args[0].(model.Update); !ok {
		panic("Wrong type of argument")
	} else {
		if nP.helper.OnRecvUpdate(ud) == nil {
			nP.global.RecvUpdate(ud)
		}
	}

}

func (nP *neighbourPeer) sendDigest(e *fsm.Event) {
	if len(e.Args) != 1 {
		panic("Wrong event arguments")
	}

	if dg, ok := e.Args[0].(map[string]model.Digest); !ok {
		panic("Wrong type of argument")
	} else {
		nP.stream.SendMessage(nP.helper.EncodeDigest(dg))
	}
}
