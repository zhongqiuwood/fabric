package gossip_model

import (
	"fmt"
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

func NewPullTask(helper PullerHelper, stream *pb.StreamHandler,
	model *Model) (*Puller, error) {

	dg := model.GenPullDigest()
	err := stream.SendMessage(helper.EncodeDigest(dg))
	if err != nil {
		return nil, fmt.Errorf("Send digest message fail: %s", err)
	}

	puller := &Puller{
		model:  model,
		update: make(chan Update),
	}

	return puller, nil
}

func (p *Puller) NotifyUpdate(ud Update) {
	if p == nil {
		return
	}

	p.update <- ud
}

func (p *Puller) Process(ctx context.Context) {

	select {
	case <-ctx.Done():
	case ud := <-p.update:
		p.model.RecvUpdate(ud)
	}

	//everything done
	return
}
