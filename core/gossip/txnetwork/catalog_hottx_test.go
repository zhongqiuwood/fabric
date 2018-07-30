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

func assertTxIsIdentify(tb testing.TB, tx1 *pb.Transaction, tx2 *pb.Transaction) {
	dg1, _ := tx1.Digest()
	dg2, _ := tx2.Digest()

	if bytes.Compare(dg1, dg2) != 0 {
		tb.Fatalf("tx is not same: %v vs %v", tx1, tx2)
	}
}

func formTestData(ledger *ledger.Ledger, txchain *peerTxs, commitsetting [][]int) (indexs []*txMemPoolItem) {

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

	//add gensis block
	ledger.BeginTxBatch(0)
	ledger.CommitTxBatch(0, nil, nil, nil)

	for ib := 0; ib < len(commitsetting); ib++ {
		ledger.BeginTxBatch(1)
		ledger.TxBegin("txUuid")
		ledger.SetState("chaincode1", "keybase", []byte{byte(ib)})
		ledger.TxFinished("txUuid", true)
		ledger.CommitTxBatch(1, genTxs(commitsetting[ib]), nil, []byte("proof1"))
		commitTxs(commitsetting[ib], uint64(ib))
	}

	return
}

func TestPeerUpdate(t *testing.T) {

	ledger := initTestLedgerWrapper(t)

	txchain := populatePoolItems(t, 10)

	indexs := formTestData(ledger, txchain, [][]int{nil, []int{2, 4, 7}, []int{3, 5}})

	var udt = txPeerUpdate{new(pb.HotTransactionBlock)}
	udt.fromTxs(txchain.fetch(1, nil), 0)

	if udt.BeginSeries != 1 {
		t.Fatalf("wrong begin series in udt1", udt.BeginSeries)
	}

	if len(udt.GetTransactions()) != 10 {
		t.Fatalf("wrong tx length in udt1: %d", len(udt.GetTransactions()))
	}

	assertTxIsIdentify(t, indexs[3].tx, udt.GetTransactions()[2])
	assertTxIsIdentify(t, indexs[6].tx, udt.GetTransactions()[5])
	assertTxIsIdentify(t, indexs[8].tx, udt.GetTransactions()[7])
	assertTxIsIdentify(t, indexs[9].tx, udt.GetTransactions()[8])

	udt.HotTransactionBlock = new(pb.HotTransactionBlock)
	udt.fromTxs(txchain.fetch(1, nil), 2)

	if udt.BeginSeries != 1 {
		t.Fatalf("wrong begin series in udt2", udt.BeginSeries)
	}

	if len(udt.GetTransactions()) != 10 {
		t.Fatalf("wrong tx length in udt2: %d", len(udt.GetTransactions()))
	}

	assertTxIsIdentify(t, indexs[1].tx, udt.GetTransactions()[0])
	assertTxIsIdentify(t, indexs[6].tx, udt.GetTransactions()[5])
	assertTxIsIdentify(t, indexs[8].tx, udt.GetTransactions()[7])
	assertTxIsIdentify(t, indexs[9].tx, udt.GetTransactions()[8])

	if !isLiteTx(udt.GetTransactions()[1]) {
		t.Fatalf("unexpected full-tx <2>")
	}

	if !isLiteTx(udt.GetTransactions()[4]) {
		t.Fatalf("unexpected full-tx <5>")
	}

	if !isLiteTx(udt.GetTransactions()[6]) {
		t.Fatalf("unexpected full-tx <7>")
	}

	retTxs, err := udt.toTxs(ledger, 5)
	if err != nil {
		t.Fatal("to txs fail:", err)
	}

	if retTxs.lastSeries() != 10 || retTxs.head.digestSeries != 5 {
		t.Fatalf("fail last or head: %d/%d", retTxs.lastSeries(), retTxs.head.digestSeries)
	}

	udt.HotTransactionBlock = new(pb.HotTransactionBlock)

	udt.fromTxs(retTxs.fetch(5, nil), 0)

	assertTxIsIdentify(t, indexs[5].tx, udt.GetTransactions()[0])
	assertTxIsIdentify(t, indexs[6].tx, udt.GetTransactions()[1])
	assertTxIsIdentify(t, indexs[7].tx, udt.GetTransactions()[2])

	//check less index
	retTxs, err = udt.toTxs(ledger, 3)
	if err != nil {
		t.Fatal("to txs fail:", err)
	}

	if retTxs.lastSeries() != 10 || retTxs.head.digestSeries != 5 {
		t.Fatalf("fail last or head: %d/%d", retTxs.lastSeries(), retTxs.head.digestSeries)
	}

}

func TestPeerTxPool(t *testing.T) {

	ledger := initTestLedgerWrapper(t)

	txchainBase := populatePoolItems(t, 39)

	indexs := formTestData(ledger, txchainBase, [][]int{nil, []int{8, 12, 15}, []int{23, 13}, []int{7, 38}})

	//we fill txpool from series 5, fill pool with a jumping index of 4 entries
	pool := new(peerTxMemPool)
	pool.reset(indexs[5])

	if len(pool.jlindex) != 4 {
		t.Fatal("unexpected jlindex:", pool.jlindex)
	}

	if pool.jlindex[3].digestSeries != 24 {
		t.Fatal("wrong entry in jlindex:", pool.jlindex[3])
	}

	if pool.lastSeries() != 39 || pool.head.digestSeries != 5 {
		t.Fatalf("fail last or head: %d/%d", pool.lastSeries(), pool.head.digestSeries)
	}

	//test To method
	if vi, ok := pool.To().(standardVClock); !ok {
		t.Fatalf("To vclock wrong: %v", pool.To())
	} else if uint64(vi) != 39 {
		t.Fatalf("To vclock wrong value: %v", vi)
	}

	//test pickFrom method
	ud_out, _ := pool.PickFrom(standardVClock(14), txPoolGlobalUpdateOut(2))

	ud, ok := ud_out.(txPeerUpdate)

	if !ok {
		t.Fatalf("type fail: %v", ud_out)
	}

	if ud.BeginSeries != 15 {
		t.Fatalf("unexpected begin: %d", ud.BeginSeries)
	}

	if !isLiteTx(ud.GetTransactions()[0]) {
		t.Fatalf("unexpected full-tx <15>")
	}

	if !isLiteTx(ud.GetTransactions()[8]) {
		t.Fatalf("unexpected full-tx <23>")
	}

	assertTxIsIdentify(t, indexs[16].tx, ud.GetTransactions()[1])
	assertTxIsIdentify(t, indexs[20].tx, ud.GetTransactions()[5])
	assertTxIsIdentify(t, indexs[38].tx, ud.GetTransactions()[23])

	//test out-date pick
	ud_out, _ = pool.PickFrom(standardVClock(3), txPoolGlobalUpdateOut(2))
	ud, ok = ud_out.(txPeerUpdate)

	if !ok {
		t.Fatalf("type fail: %v", ud_out)
	}

	if ud.BeginSeries != 5 || len(ud.Transactions) != 1 {
		t.Fatalf("unexpected begin: %v", ud.Transactions)
	}

	//test update
	txChainAdd := prolongItemChain(t, txchainBase.last, 20)

	txChainAdd.head = txChainAdd.head.next

	//collect more items ...
	for i := txChainAdd.head; i != nil; i = i.next {
		indexs = append(indexs, i)
	}

	txGlobal := &txPoolGlobal{
		ind:    make(map[string]*txMemPoolItem),
		ledger: ledger,
	}

	udt := txPeerUpdate{new(pb.HotTransactionBlock)}

	//all item in txChainAdd is not commited so epoch is of no use
	udt.fromTxs(txChainAdd, 0)
	if udt.BeginSeries != 40 {
		t.Fatal("unexpected begin series", udt.BeginSeries)
	}

	//must also add global state ...
	pstatus := GetNetworkStatus().addNewPeer("test")
	pstatus.beginTxDigest = txchainBase.head.digest

	pool.peerId = "anotherTest"

	//you update an unknown peer, no effect in fact
	err := pool.Update(udt, txGlobal)
	if err != nil {
		t.Fatal("update fail", err)
	}

	if len(txGlobal.ind) != 0 {
		t.Fatal("update unknown peer")
	}

	//now peerid is right
	pool.peerId = "test"
	err = pool.Update(udt, txGlobal)
	if err != nil {
		t.Fatal("update actual fail", err)
	}

	if len(txGlobal.ind) == 0 {
		t.Fatal("unexpected no update")
	}

	if pool.lastSeries() != 59 {
		t.Fatal("unexpected last", pool.lastSeries())
	}

	//now you can get tx from ledger or ind of txGlobal
	checkTx := func(pos int) {
		txid := indexs[pos].tx.GetTxid()

		if txid == "" {
			t.Fatal("unexpected empty txid")
		}

		tx, err := ledger.GetTransactionByID(txid)
		if err != nil || tx == nil {
			t.Fatalf("get tx %d in ledger fail: %s", pos, err)
		}

		assertTxIsIdentify(t, indexs[pos].tx, tx)

		txItem, ok := txGlobal.ind[txid]
		if !ok {
			t.Fatalf("get tx %d in index fail")
		}

		if txItem.digestSeries != uint64(pos) {
			t.Fatalf("tx series %d is unmatched with index [%d]", txItem.digestSeries, pos)
		}

		assertTxIsIdentify(t, indexs[pos].tx, txItem.tx)
	}

	checkTx(40)
	checkTx(42)
	checkTx(45)
	checkTx(55)

	//test purge
	pool.purge(50, txGlobal)

	if pool.head.digestSeries != 50 {
		t.Fatalf("wrong head series after purge", pool.head.digestSeries)
	}

	if len(txGlobal.ind) != 10 {
		t.Fatalf("wrong index after purge", len(txGlobal.ind))
	}

	if _, ok := txGlobal.ind[indexs[45].tx.GetTxid()]; ok {
		t.Fatalf("global still indexed tx which should be purged")
	}

	if len(pool.jlindex) != 1 {
		t.Fatalf("wrong index after purge", len(txGlobal.ind))
	}

	if _, ok := pool.jlindex[6]; ok {
		t.Fatalf("still have index in jumping list after purge")
	}

	//test pickfrom after purge
	ud_out, _ = pool.PickFrom(standardVClock(53), txPoolGlobalUpdateOut(0))

	ud, ok = ud_out.(txPeerUpdate)

	if !ok {
		t.Fatalf("type fail: %v", ud_out)
	}

	if ud.BeginSeries != 54 {
		t.Fatalf("unexpected begin: %d", ud.BeginSeries)
	}

	assertTxIsIdentify(t, indexs[55].tx, ud.GetTransactions()[1])
	assertTxIsIdentify(t, indexs[59].tx, ud.GetTransactions()[5])

}

func TestCatalogyHandler(t *testing.T) {

	l := initTestLedgerWrapper(t)

	txchainBase := populatePoolItems(t, 39)

	indexs := formTestData(l, txchainBase, [][]int{nil, []int{8, 12, 15}, []int{23, 13}, []int{7, 38}})

	const testname = "test"

	pstatus := GetNetworkStatus().addNewPeer(testname)
	pstatus.beginTxDigest = txchainBase.head.digest

	txglobal := new(txPoolGlobal)
	txglobal.ind = make(map[string]*txMemPoolItem)
	txglobal.ledger = l

	hotTx := new(hotTxCat)

	m := model.NewGossipModel(model.NewScuttlebuttStatus(txglobal))

	//try to build a proto directly
	dig_in := &pb.Gossip_Digest{Data: make(map[string]*pb.Gossip_Digest_PeerState)} //any epoch is ok

	dig_in.Data[testname] = &pb.Gossip_Digest_PeerState{State: txchainBase.head.digest}

	dig := hotTx.TransPbToDigest(dig_in)

	//now model should know peer test
	m.MakeUpdate(dig)

	dig = m.GenPullDigest()
	dig_out := hotTx.TransDigestToPb(dig)

	if _, ok := dig_out.Data[testname]; !ok {
		t.Fatal("model not known expected peer", dig_out)
	}

	t.Log(indexs)
}
