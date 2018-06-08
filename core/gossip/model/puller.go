package gossip_model

import (
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

type PullerHelper interface {
	EncodeDigest(map[string]Digest) proto.Message
}

type Puller struct {
	model  *Model
	update chan Update
}

func NewPullTask(helper PullerHelper, model *Model,
	stream *pb.StreamHandler) *Puller {

	dg := model.GenPullDigest()
	stream.SendMessage(helper.EncodeDigest(dg))

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
