package gossip_model

import (
	"errors"
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

//The push-pull model
type PushHelper interface {
	PullerHelper
	//Handle method in PullerHandler is ensured to be called
	CanPull() *Puller
	//must allow nil input and encode an message include "empty" update
	EncodeUpdate(Update) proto.Message
}

//d can be set to nil and just return empty update (sometimes this indicate an
//invitation of pulling)
//YA-fabric 0.9:
//This accept pulling process has following suppose:
//a. the handling of pulling (RecvPullDigest in model) is fast
//b. generate a digest request (decide what should be pulled from far-end) is highly-cost
//c. for the "responding" pulling, the first pulling request needed to be handled first
//   to feed extra information for the model
//
//And after all, the update message must be sent AFTER the "responding" digest
func AcceptPulling(p PushHelper, stream *pb.StreamHandler, model *Model, d Digest) func() (*Puller, error) {

	var msg proto.Message

	if d == nil {
		msg = p.EncodeUpdate(nil)
	} else {
		//suppose this is fast
		msg = p.EncodeUpdate(model.RecvPullDigest(d))
	}

	if puller := p.CanPull(); puller != nil {
		//bind update-msg into puller so it will be sent after digest msg
		return func() (*Puller, error) {

			defer stream.SendMessage(msg)
			err := puller.Start(p, stream)
			if err != nil {
				return nil, err
			}

			return puller, nil
		}

	} else {
		//NOTICE: if stream is NOT enable to drop message, send in HandMessage
		//may cause a deadlock, but in gossip package this is OK
		//in fact we can just omit the error of sendmessage
		stream.SendMessage(msg)
		return nil
	}

}

func NewPuller(model *Model) *Puller {

	return &Puller{
		model:  model,
		update: make(chan Update),
	}

}

var EmptyDigest = errors.New("digest is null")
var EmptyUpdate = errors.New("update is null")

func (p *Puller) Start(ph PullerHelper, stream *pb.StreamHandler) error {
	dg := p.model.GenPullDigest()
	if dg == nil {
		return EmptyDigest
	}

	return stream.SendMessage(ph.EncodeDigest(dg))
}

func (p *Puller) Process(ctx context.Context) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	case ud := <-p.update:
		if ud == nil {
			return EmptyUpdate
		} else {
			return p.model.RecvUpdate(ud)
		}
	}
}

func (p *Puller) NotifyUpdate(ud Update) {
	if p == nil {
		return
	}
	p.update <- ud
}
