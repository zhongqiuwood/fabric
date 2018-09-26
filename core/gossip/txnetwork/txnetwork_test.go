package txnetwork

import (
	"golang.org/x/net/context"
	"testing"
	"time"
)

type dummyH struct {
	tx        []*PendingTransaction
	callTimes int
	testing.TB
}

func (h *dummyH) Release() {}

func (h *dummyH) HandleTxs(tx []*PendingTransaction) {

	h.Logf("Add %d txs", len(tx))
	h.tx = append(h.tx, tx...)
	h.callTimes++

}
func TestTxEntrance(t *testing.T) {

	wctx, endf := context.WithCancel(context.Background())

	entry := NewTxNetworkEntry()

	for i := 0; i < 10; i++ {

		tx, _ := buildTestTx(t)
		entry.BroadCastTransaction(tx, "aa")
	}

	h := &dummyH{nil, 0, t}

	entry.Start(wctx, h)

	time.Sleep(time.Second)

	if len(h.tx) != 10 {
		t.Fatal("Handler never receive enough txs:", h.tx)
	}

	if h.callTimes > 1 {
		t.Errorf("Call mutiple times", h.callTimes)
	}

	for i := 0; i < 5; i++ {

		tx, _ := buildTestTx(t)
		entry.BroadCastTransaction(tx, "bb")
	}

	time.Sleep(time.Second)

	if len(h.tx) != 15 {
		t.Fatal("Handler never receive enough txs:", h.tx)
	}

	if h.tx[13].endorser != "bb" || h.tx[7].endorser != "aa" {
		t.Fatal("Wrong endorser name", h.tx)
	}

	if h.callTimes > 6 {
		t.Errorf("Too many calling times", h.callTimes)
	}

	endf()

	time.Sleep(time.Second)

	ntx, _ := buildTestTx(t)
	entry.BroadCastTransaction(ntx, "cc")

	time.Sleep(time.Second)

	if len(h.tx) != 15 {
		t.Fatal("Handler accept unexpected tx")
	}

}
