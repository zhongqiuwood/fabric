package gossip_model

import (
	"fmt"
	"golang.org/x/net/context"
)

type PullerHelper interface {
	//Notify if we failed from a pulling
	FailPulling(string)
	//Notify we receive a update, if return NOT nil, not apply this
	OnRecvUpdate(Update) error
}

type puller struct {
	PullerHelper
	target *neighbourPeer
	update chan Update
}

func NewPullTask(helper PullerHelper, nP *neighbourPeer) (*puller, error) {

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

func (p *puller) Process(ctx context.Context) error {

	nP := p.target

	dg := nP.global.GenPullDigest()

	err := nP.stream.SendMessage(nP.helper.EncodeDigest(dg))
	if err != nil {
		return fmt.Errorf("Send digest message fail: %s", err)
	}

	select {
	case <-ctx.Done():
		p.FailPulling(nP.stream.GetName())
		return fmt.Errorf("User cancel gossip-pull")
	case ud := <-p.update:
		err = p.OnRecvUpdate(ud)
		if err != nil {
			return fmt.Errorf("Update is rejected: %s", err)
		}
		nP.global.RecvUpdate(ud)
	}

	//everything done
	return nil
}
