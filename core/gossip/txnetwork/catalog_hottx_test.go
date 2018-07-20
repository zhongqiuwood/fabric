package txnetwork

import (
	"bytes"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
	"testing"
)

func newHotTxModel(l *ledger.Ledger) *model.Model {
	txglobal := new(txPoolGlobal)
	txglobal.ind = make(map[string]*txMemPoolItem)
	txglobal.ledger = l

	return model.NewGossipModel(model.NewScuttlebuttStatus(txglobal))
}

var genesisDigest = []byte("hottx")

func TestTxChain(t *testing.T) {
	tx1, _ := buildTestTx(t)
	tx2, _ := buildTestTx(t)
	oldNonce := tx2.GetNonce()
	oldDigest, err := tx2.Digest()
	if err != nil {
		t.Fatal("digest old tx2 fail", err)
	}

	tx1 = buildPrecededTx(genesisDigest, tx1)

	dg1, err := tx1.Digest()
	if err != nil {
		t.Fatal("digest tx1 fail", err)
	}

	tx2 = buildPrecededTx(dg1, tx2)

	if !txIsPrecede(dg1, tx2) {
		t.Fatal("tx2 is not precede of tx1")
	}

	tx2.Nonce = oldNonce

	nowDigest, err := tx2.Digest()
	if err != nil {
		t.Fatal("digest fail", err)
	}

	if bytes.Compare(nowDigest, oldDigest) != 0 {
		t.Fatalf("digest not equal: %x vs %x", nowDigest, oldDigest)
	}
}

func prolongItemChain(t *testing.T, head *txMemPoolItem, n int) *peerTxs {

	cur := head

	for i := 0; i < n; i++ {

		tx, _ := buildTestTx(t)
		tx = buildPrecededTx(cur.digest, tx)

		dg, err := tx.Digest()
		if err != nil {
			t.Fatalf("fail digest at %d, %s", n, err)
		}

		t.Logf("create tx with digest %x", dg)

		cur.next = &txMemPoolItem{
			digest:       dg,
			digestSeries: cur.digestSeries + 1,
			tx:           tx,
		}
		cur = cur.next
	}

	return &peerTxs{head, cur}
}

func populatePoolItems(t *testing.T, n int) *peerTxs {

	return prolongItemChain(t, &txMemPoolItem{digest: genesisDigest}, n)

}

func TestPeerTxs(t *testing.T) {

	txs := populatePoolItems(t, 3)

	if txs.lastSeries() != 3 {
		t.Fatalf("broken series %d", txs.last.digestSeries)
	}

	for beg := txs.head; beg != txs.last; beg = beg.next {
		if !txIsPrecede(beg.digest, beg.next.tx) {
			t.Fatalf("chain 1 broken at %d", beg.digestSeries)
		}
	}

	txs2 := prolongItemChain(t, txs.last.clone(), 4)
	txs2.head = txs2.head.next

	err := txs.concat(txs2)

	if err != nil {
		t.Fatalf("concat chain fail", err)
	}

	if txs.lastSeries() != 7 {
		t.Fatalf("broken series %d after concat", txs.last.digestSeries)
	}

	for beg := txs.head; beg != txs.last; beg = beg.next {
		if !txIsPrecede(beg.digest, beg.next.tx) {
			t.Fatalf("chain 2 broken at %d", beg.digestSeries)
		}
	}

	if txs2.inRange(1) || txs2.inRange(8) || !txs2.inRange(5) {
		t.Fatalf("wrong in range 2 in chain2")
	}

	txs3 := txs.fetch(4, nil)

	if txs3.head.digestSeries != 4 {
		t.Fatalf("wrong chain 3: %d", txs3.head.digestSeries)
	}

	if txs3.lastSeries() != 7 {
		t.Fatalf("wrong chain 3 tail: %d", txs3.lastSeries())
	}

	if bytes.Compare(txs3.head.next.tx.Payload, txs2.head.next.tx.Payload) != 0 {
		t.Fatalf("wrong tx in identify chain 2 and 3")
	}

	if txsnil := txs3.fetch(2, nil); txsnil != nil {
		t.Fatalf("fetch ghost txs: %d", txsnil.head.digestSeries)
	}
}

func TestPeerUpdate(t *testing.T) {

	ledger := initTestLedgerWrapper(t)

	txchain := populatePoolItems(t, 10)

	var indexs []*txMemPoolItem

	//collect all items into array
	for i := txchain.head; i != nil; i = i.next {
		indexs = append(indexs, i)
	}

	genTxs := func(ii []int) (out []*pb.Transaction) {
		for _, i := range ii {
			out = append(out, indexs[i].tx)
		}
		return
	}

	commitTxs := func(ii []int, h uint64) {
		for _, i := range ii {
			indexs[i].committedH = h
		}
	}

	commitsetting := [][]int{nil, []int{2, 4, 7}, []int{3, 5}}

	//add gensis block
	ledger.BeginTxBatch(0)
	ledger.CommitTxBatch(0, nil, nil, nil)

	ledger.BeginTxBatch(1)
	ledger.TxBegin("txUuid")
	ledger.SetState("chaincode1", "key1", []byte("value1"))
	ledger.TxFinished("txUuid", true)
	ledger.CommitTxBatch(1, genTxs(commitsetting[1]), nil, []byte("proof1"))
	commitTxs(commitsetting[1], 1)

	ledger.BeginTxBatch(1)
	ledger.TxBegin("txUuid")
	ledger.SetState("chaincode1", "key2", []byte("value2"))
	ledger.TxFinished("txUuid", true)
	ledger.CommitTxBatch(1, genTxs(commitsetting[2]), nil, []byte("proof2"))
	commitTxs(commitsetting[2], 2)

	var udt1 txPeerUpdate
	udt1.fromTxs(txchain.fetch(1, nil), 0)

	if udt1.BeginSeries != 1 {
		t.Fatalf("wrong begin series in udt1", udt1.BeginSeries)
	}
}
