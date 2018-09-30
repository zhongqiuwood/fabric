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
//"invitation" of pulling)
//Pulling process including two steps:
//1. handling incoming digest and response a update
//2. optional: start a "responding" pull
//the message among these two step is fixed: that is, the updating in step 1 must
//be sent after the digest sent in step 2 (unless step 2 is omitted)
//
//
//The whole process is considered to be time-consuming: model and helper need to tailor
//the update to fit it into a suitable message size, and in a pulling process
//to decide a subset of peers in digest may require many evaluations
func AcceptPulling(p PushHelper, stream *pb.StreamHandler, model *Model, d Digest) (*Puller, error) {

	var msg proto.Message

	if d == nil {
		msg = p.EncodeUpdate(nil)
	} else {
		//suppose this is fast
		msg = p.EncodeUpdate(model.RecvPullDigest(d))
	}

	defer stream.SendMessage(msg)

	if puller := p.CanPull(); puller != nil {
		err := puller.Start(p, stream)
		if err != nil {
			return nil, err
		}

		return puller, nil
	}

	return nil, nil

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
	//TODO: we should allow empty digest in some case
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
