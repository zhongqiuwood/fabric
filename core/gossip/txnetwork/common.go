package txnetwork

import (
	"github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"sync"
)

type txcommonImpl struct {
	secHelper crypto.Peer
	sync.Once
}

var txcommon txcommonImpl

func InitTxNetwork(secHelperFunc func() crypto.Peer) {
	txcommon.Do(func() {
		txcommon.secHelper = secHelperFunc()
	})
}

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

func registerEvictFunc(h gossip.CatalogHandler) {
	GetNetworkStatus().regNotify(func(ids []string) {
		for _, id := range ids {
			h.RemovePeer(id)
		}
	})
}
