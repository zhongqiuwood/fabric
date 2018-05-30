package gossip_model

import (
	"fmt"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

type PullerHelper interface {
	EncodeDigest(map[string]Digest) proto.Message

	//Notify if we failed from a pulling
	FailPulling(string)
	//Notify we receive a update, if return NOT nil, not apply this
	OnRecvUpdate(Update) error
}

type puller struct {
	PullerHelper
	target *NeighbourPeer
	update chan Update
}

func NewPullTask(helper PullerHelper, nP *NeighbourPeer) (*puller, error) {

	if nP.workPuller != nil {
		return nil, fmt.Errorf("Have working puller")
	}

	puller := &puller{
		PullerHelper: helper,
		target:       nP,
		update:       make(chan Update),
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

func (p *puller) Process(ctx context.Context, stream *pb.StreamHandler) error {

	g := p.target.global

	dg := g.GenPullDigest()

	err := stream.SendMessage(p.EncodeDigest(dg))
	if err != nil {
		return fmt.Errorf("Send digest message fail: %s", err)
	}

	select {
	case <-ctx.Done():
		p.FailPulling(stream.GetName())
		return fmt.Errorf("User cancel gossip-pull")
	case ud := <-p.update:
		err = p.OnRecvUpdate(ud)
		if err != nil {
			return fmt.Errorf("Update is rejected: %s", err)
		}
		g.RecvUpdate(ud)
	}

	//everything done
	return nil
}
