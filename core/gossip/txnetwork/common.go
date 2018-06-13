package txnetwork

import (
	model "github.com/abchain/fabric/core/gossip/model"
	"sync"
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

type asyncEvictPeerNotifier struct {
	sync.Mutex
	evicted []string
}

func (n *asyncEvictPeerNotifier) Register(t *txNetworkGlobal) {
	t.regNotify(func(ids []string) {
		n.Lock()
		defer n.Unlock()
		n.evicted = append(n.evicted, ids...)
	})

}

func (n *asyncEvictPeerNotifier) Pop() []string {

	n.Lock()
	defer n.Unlock()

	ret := n.evicted
	n.evicted = nil

	return ret
}
