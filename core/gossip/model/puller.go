package gossip_model

import (
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

type PullerHelper interface {
	EncodeDigest(Digest) proto.Message
}

type Puller struct {
	model  *Model
	update chan Update
}

type PullerHandler interface {
	PullerHelper
	Handle(*Puller)
}

//The push-pull model
type PushHelper interface {
	//Handle method in PullerHandler is ensured to be called
	CanPull() PullerHandler
	//must allow nil input and encode an message include "empty" update
	EncodeUpdate(Update) proto.Message
}

//d can be set to nil and just return empty update (sometimes this indicate an
//invitation of pulling)
func AcceptPulling(p PushHelper, stream *pb.StreamHandler, model *Model, d Digest) error {

	if ph := p.CanPull(); ph != nil {
		ph.Handle(NewPuller(ph, stream, model))
	}

	if d == nil {
		return stream.SendMessage(p.EncodeUpdate(nil))
	}

	ud := model.RecvPullDigest(d)

	//NOTICE: if stream is NOT enable to drop message, send in HandMessage
	//may cause a deadlock, but in gossip package this is OK
	return stream.SendMessage(p.EncodeUpdate(ud))
}

func NewPuller(ph PullerHelper, stream *pb.StreamHandler, model *Model) *Puller {

	dg := model.GenPullDigest()
	if dg == nil {
		return nil
	}

	stream.SendMessage(ph.EncodeDigest(dg))

	return &Puller{
		model:  model,
		update: make(chan Update),
	}

}

func (p *Puller) NotifyUpdate(ud Update) {
	if p == nil {
		return
	}
	p.update <- ud
}

func (p *Puller) Process(ctx context.Context) (e error) {

	select {
	case <-ctx.Done():
		e = ctx.Err()
	case ud := <-p.update:
		e = p.model.RecvUpdate(ud)
	}

	return
}
