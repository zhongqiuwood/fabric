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

type puller struct {
	PullerHelper
	target *NeighbourPeer
	update chan Update
}

func NewPullTask(helper PullerHelper, stream *pb.StreamHandler,
	nP *NeighbourPeer) (*puller, error) {

	if nP.workPuller != nil {
		return nil, fmt.Errorf("Have working puller")
	}

	dg := nP.global.GenPullDigest()
	err := stream.SendMessage(helper.EncodeDigest(dg))
	if err != nil {
		return nil, fmt.Errorf("Send digest message fail: %s", err)
	}

	puller := &puller{
		target: nP,
		update: make(chan Update),
	}

	nP.workPuller = puller

	return puller, nil
}

func (p *puller) NotifyUpdate(ud Update) {
	if p == nil {
		return
	}

	p.update <- ud
}

func (p *puller) Process(ctx context.Context) {

	select {
	case <-ctx.Done():
	case ud := <-p.update:
		p.target.global.RecvUpdate(ud)
	}

	//everything done
	return
}
