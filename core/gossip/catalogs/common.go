package gossip_cat

import (
	model "github.com/abchain/fabric/core/gossip/model"
)

//a standard vclock use seq
type standardVClock struct {
	oor bool
	n   uint64
}

func (a *standardVClock) Less(b_in model.VClock) bool {
	b, ok := b_in.(*standardVClock)
	if !ok {
		panic("Wrong type, not standardVClock")
	}

	if b.OutOfRange() {
		return false
	}

	return a.n < b.n
}

func (v *standardVClock) OutOfRange() bool {
	return v.oor
}
