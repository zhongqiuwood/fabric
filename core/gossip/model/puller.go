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
	PullerHelper
	model  *Model
	update chan Update
}

func NewPullTask(helper PullerHelper, model *Model) *Puller {

	return &Puller{
		PullerHelper: helper,
		model:        model,
		update:       make(chan Update),
	}
}

func (p *Puller) NotifyUpdate(ud Update) {
	if p == nil {
		return
	}

	p.update <- ud
}

func (p *Puller) Process(ctx context.Context, stream *pb.StreamHandler) {

	dg := p.model.GenPullDigest()
	stream.SendMessage(p.EncodeDigest(dg))

	select {
	case <-ctx.Done():
	case ud := <-p.update:
		p.model.RecvUpdate(ud)
	}

	//everything done
	return
}
