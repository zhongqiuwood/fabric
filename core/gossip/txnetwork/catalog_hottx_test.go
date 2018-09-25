package txnetwork

import (
	"bytes"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/util"
	pb "github.com/abchain/fabric/protos"
	"testing"
)

func newHotTxModel(l *ledger.Ledger) *model.Model {
	txglobal := new(txPoolGlobal)
	txglobal.ledger = l

	return model.NewGossipModel(model.NewScuttlebuttStatus(txglobal))
}

var genesisDigest = util.GenerateBytesUUID()

func buildPrecededTx(digest []byte, tx *pb.Transaction) *pb.Transaction {

	if len(digest) < TxDigestVerifyLen {
		digest = append(digest, failDig[len(digest):]...)
	}

	tx.Nonce = digest
	return tx
}

func TestTxChain(t *testing.T) {
	tx1, _ := buildTestTx(t)
	tx2, _ := buildTestTx(t)
	oldNonce := tx2.GetNonce()
	oldDigest := getTxDigest(tx2)

	tx1 = buildPrecededTx(genesisDigest, tx1)
	dg1 := getTxDigest(tx1)

	tx2 = buildPrecededTx(dg1, tx2)

	if !txIsPrecede(dg1, tx2) {
		t.Fatalf("tx2 is not precede of tx1: %x vs %x", dg1, tx2.GetNonce())
	}

	tx2.Nonce = oldNonce

	nowDigest := getTxDigest(tx2)

	if bytes.Compare(nowDigest, oldDigest) != 0 {
		t.Fatalf("digest not equal: %x vs %x", nowDigest, oldDigest)
	}
}

func prolongItemChain(t *testing.T, head *txMemPoolItem, n int) (*peerTxs, []*pb.Transaction) {

	cur := head
	txs := []*pb.Transaction{nil} //make the array index same for txChain

	for i := 0; i < n; i++ {

		tx, _ := buildTestTx(t)
		tx = buildPrecededTx(cur.digest, tx)

		dg := getTxDigest(tx)

		t.Logf("create tx with digest %x", dg)

		cur.next = &txMemPoolItem{
			digest:       dg,
			digestSeries: cur.digestSeries + 1,
			txid:         tx.GetTxid(),
		}
		cur = cur.next
		txs = append(txs, tx)
	}

	return &peerTxs{head, cur}, txs
}

func populatePoolItems(t *testing.T, n int) (*peerTxs, []*pb.Transaction) {

	return prolongItemChain(t, &txMemPoolItem{digest: genesisDigest}, n)

}

type dummyCache map[string]*pb.Transaction

func (d dummyCache) GetTx(_ uint64, txid string) (*pb.Transaction, uint64) {
	return d[txid], 0
}

func (d dummyCache) Import(txs []*pb.Transaction) {

	for _, tx := range txs {
		d[tx.GetTxid()] = tx
	}
}

func newCache() dummyCache { return dummyCache(make(map[string]*pb.Transaction)) }

func TestPeerTxs(t *testing.T) {

	initGlobalStatus()
	txs, txcache := populatePoolItems(t, 3)

	if txs.lastSeries() != 3 {
		t.Fatalf("broken series %d", txs.last.digestSeries)
	}

	cache := newCache()
	cache.Import(txcache)

	for beg := txs.head; beg != txs.last; beg = beg.next {
		if !txIsPrecede(beg.digest, cache[beg.next.txid]) {
			t.Fatalf("chain 1 broken at %d", beg.digestSeries)
		}
	}

	txs2, txcache := prolongItemChain(t, txs.last.clone(), 4)
	cache.Import(txcache)
	txs2.head = txs2.head.next

	err := txs.concat(txs2)

	if err != nil {
		t.Fatalf("concat chain fail", err)
	}

	if txs.lastSeries() != 7 {
		t.Fatalf("broken series %d after concat", txs.last.digestSeries)
	}

	for beg := txs.head; beg != txs.last; beg = beg.next {
		if !txIsPrecede(beg.digest, cache[beg.next.txid]) {
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

	if bytes.Compare(cache[txs3.head.next.txid].Payload, cache[txs2.head.next.txid].Payload) != 0 {
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

func formTestData(ledger *ledger.Ledger, txchain *peerTxs, commitsetting [][]int, cache dummyCache) (indexs []*txMemPoolItem) {

	//collect all items into array
	for i := txchain.head; i != nil; i = i.next {
		indexs = append(indexs, i)
	}

	genTxs := func(ii []int) (out []*pb.Transaction) {
		for _, i := range ii {
			tx, _ := cache[indexs[i].txid]
			out = append(out, tx)
		}
		return
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
	}

	return
}

func TestPeerUpdate(t *testing.T) {

	initGlobalStatus()
	ledger := initTestLedgerWrapper(t)

	txchain, txcache := populatePoolItems(t, 10)
	cache := newCache()
	cache.Import(txcache)

	var udt = txPeerUpdate{new(pb.HotTransactionBlock)}
	udt.fromTxs(txchain.fetch(1, nil), 0, cache)

	if udt.BeginSeries != 1 {
		t.Fatalf("wrong begin series in udt1: %d", udt.BeginSeries)
	}

	if len(udt.GetTransactions()) != 10 {
		t.Fatalf("wrong tx length in udt1: %d", len(udt.GetTransactions()))
	}

	assertTxIsIdentify(t, txcache[3], udt.GetTransactions()[2])
	assertTxIsIdentify(t, txcache[6], udt.GetTransactions()[5])
	assertTxIsIdentify(t, txcache[8], udt.GetTransactions()[7])
	assertTxIsIdentify(t, txcache[9], udt.GetTransactions()[8])

	retTxs, err := udt.toTxs(nil)
	if err != nil {
		t.Fatal("to txs fail:", err)
	}

	if retTxs.lastSeries() != 10 || retTxs.head.digestSeries != 1 {
		t.Fatalf("fail last or head: %d/%d", retTxs.lastSeries(), retTxs.head.digestSeries)
	}

	txPool := newTransactionPool(ledger)
	txPool.AcquireCache("helperCache", 0, 1).AddTxs(txcache, true)

	formTestData(ledger, txchain, [][]int{nil, []int{2, 4, 7}, []int{3, 5}}, cache)

	udt.HotTransactionBlock = new(pb.HotTransactionBlock)
	udt.fromTxs(retTxs.fetch(1, nil), 2, txPool.AcquireCache("helperCache", 0, txchain.lastSeries()+1))

	if udt.BeginSeries != 1 {
		t.Fatalf("wrong begin series in udt2", udt.BeginSeries)
	}

	if len(udt.GetTransactions()) != 10 {
		t.Fatalf("wrong tx length in udt2: %d", len(udt.GetTransactions()))
	}

	assertTxIsIdentify(t, txcache[1], udt.GetTransactions()[0])
	assertTxIsIdentify(t, txcache[6], udt.GetTransactions()[5])
	assertTxIsIdentify(t, txcache[8], udt.GetTransactions()[7])
	assertTxIsIdentify(t, txcache[9], udt.GetTransactions()[8])

	if !isLiteTx(udt.GetTransactions()[1]) {
		t.Fatalf("unexpected full-tx <2>")
	}

	if !isLiteTx(udt.GetTransactions()[6]) {
		t.Fatalf("unexpected full-tx <7>")
	}

	handledudt, err := udt.getRef(5).completeTxs(ledger, nil)
	if err != nil {
		t.Fatal("handle udt fail:", err)
	}

	retTxs, err = handledudt.toTxs(nil)
	if err != nil {
		t.Fatal("to txs fail:", err)
	}

	if retTxs.lastSeries() != 10 || retTxs.head.digestSeries != 5 {
		t.Fatalf("fail last or head: %d/%d", retTxs.lastSeries(), retTxs.head.digestSeries)
	}

	udt.HotTransactionBlock = new(pb.HotTransactionBlock)

	udt.fromTxs(retTxs.fetch(5, nil), 0, cache)

	assertTxIsIdentify(t, txcache[5], udt.GetTransactions()[0])
	assertTxIsIdentify(t, txcache[6], udt.GetTransactions()[1])
	assertTxIsIdentify(t, txcache[7], udt.GetTransactions()[2])

	//check less index
	handledudt, err = udt.getRef(3).completeTxs(ledger, nil)
	if err != nil {
		t.Fatal("handle udt fail:", err)
	}

	retTxs, err = handledudt.toTxs(nil)
	if err != nil {
		t.Fatal("to txs fail:", err)
	}

	if retTxs.lastSeries() != 10 || retTxs.head.digestSeries != 5 {
		t.Fatalf("fail last or head: %d/%d", retTxs.lastSeries(), retTxs.head.digestSeries)
	}

}

func TestPeerTxPool(t *testing.T) {

	defer func(bits uint) {
		SetPeerTxQueueLen(bits)
	}(peerTxQueueLenBit)

	SetPeerTxQueueLen(3)

	global := initGlobalStatus()
	ledger := initTestLedgerWrapper(t)
	txGlobal := &txPoolGlobal{
		network:         global,
		transactionPool: newTransactionPool(ledger),
	}

	defaultPeer := "test"
	txchainBase, txcache := populatePoolItems(t, 39)
	cache := newCache()
	cache.Import(txcache)

	indexs := formTestData(ledger, txchainBase, [][]int{nil, []int{8, 12, 15}, []int{23, 13}, []int{7, 38}}, cache)

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

	//test pickFrom method, bind a cache with out tested pool
	txGlobal.AcquireCache(defaultPeer, 0, pool.firstSeries()).AddTxs(txcache[5:], false)
	ud_out, _ := pool.PickFrom(defaultPeer, standardVClock(14), txPoolGlobalUpdateOut{txGlobal, 2})

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

	if _, h := txGlobal.AcquireCache(defaultPeer, pool.firstSeries(), pool.lastSeries()+1).GetTx(15, ud.GetTransactions()[0].Txid); h != 2 {
		t.Fatal("unexpected commit h", h)
	}

	assertTxIsIdentify(t, txcache[16], ud.GetTransactions()[1])
	assertTxIsIdentify(t, txcache[23], ud.GetTransactions()[8])
	assertTxIsIdentify(t, txcache[38], ud.GetTransactions()[23])

	//test out-date pick
	ud_out, _ = pool.PickFrom(defaultPeer, standardVClock(3), txPoolGlobalUpdateOut{txGlobal, 2})
	ud, ok = ud_out.(txPeerUpdate)

	if !ok {
		t.Fatalf("type fail: %v", ud_out)
	}

	if ud.BeginSeries != 5 || len(ud.Transactions) != 1 {
		t.Fatalf("unexpected begin: %v", ud.Transactions)
	}

	//test update
	txChainAdd, txcacheAdd := prolongItemChain(t, txchainBase.last, 20)
	cache.Import(txcacheAdd)
	txcache = append(txcache, txcacheAdd[1:]...)

	//"cut" the new chain ..., keep baseChain unchange
	newAddHead := txChainAdd.head.next
	txChainAdd.head.next = nil
	txChainAdd.head = newAddHead

	//collect more items ...
	for i := txChainAdd.head; i != nil; i = i.next {
		indexs = append(indexs, i)
	}

	udt := txPeerUpdate{new(pb.HotTransactionBlock)}

	//all item in txChainAdd is not commited so epoch is of no use
	udt.fromTxs(txChainAdd, 0, cache)
	if udt.BeginSeries != 40 {
		t.Fatal("unexpected begin series", udt.BeginSeries)
	}

	//must also add global state ...
	pstatus := global.addNewPeer(defaultPeer)
	pstatus.Digest = txchainBase.head.digest
	pstatus.Endorsement = []byte{2, 3, 3}

	//you update an unknown peer, no effect in fact
	err := pool.Update("anotherTest", udt, txGlobal)
	if err != nil {
		t.Fatal("update fail", err)
	}

	if txGlobal.AcquireCache("anotherTest", 0, 0).peerTxCache[0] != nil {
		t.Fatal("update unknown peer")
	}

	err = pool.Update(defaultPeer, udt, txGlobal)
	if err != nil {
		t.Fatal("update actual fail", err)
	}

	if pool.lastSeries() != 59 {
		t.Fatal("unexpected last", pool.lastSeries())
	}

	//now you can get tx from ledger
	checkTx := func(pos int) {
		txid := indexs[pos].txid

		if txid == "" {
			t.Fatal("unexpected empty txid")
		}

		tx := ledger.GetPooledTransaction(txid)
		if tx == nil {
			t.Fatalf("get pool tx %d in ledger fail", pos)
		}

		assertTxIsIdentify(t, txcache[pos], tx)
	}

	checkTx(40)
	checkTx(42)
	checkTx(45)
	checkTx(55)

	//"cut" the new chain again (which is concat in update of pool)
	indexs[39].next = nil
	//test update including older data
	anotherpool := new(peerTxMemPool)
	anotherpool.reset(indexs[5])

	if anotherpool.lastSeries() != 39 {
		panic("wrong resetting")
	}

	newChainArr := udt.Transactions
	udt = txPeerUpdate{new(pb.HotTransactionBlock)}
	udt.fromTxs(&peerTxs{indexs[39], indexs[39]}, 0, cache)
	if udt.BeginSeries != 39 || len(udt.Transactions) != 1 {
		panic("wrong udt")
	}
	udt.Transactions = append(udt.Transactions, newChainArr...)

	anotherpool.Update(defaultPeer, udt, txGlobal)

	if anotherpool.lastSeries() != 59 {
		t.Fatal("unexpected last for update with old data", pool.lastSeries())
	}

	if c := txGlobal.AcquireCache(defaultPeer, 0, 0).peerTxCache; c[5] == nil {
		t.Fatal("Wrong cache status", c)
	}

	//test purge
	pool.purge("test", 50, txGlobal)

	if pool.head.digestSeries != 50 {
		t.Fatalf("wrong head series after purge", pool.head.digestSeries)
	}

	if c := txGlobal.AcquireCache(defaultPeer, 0, 0).peerTxCache; c[5] != nil {
		t.Fatal("cache still have cache block which should be purged")
	}

	if len(pool.jlindex) != 1 {
		t.Fatal("wrong index after purge", pool.jlindex)
	}

	if _, ok := pool.jlindex[6]; ok {
		t.Fatal("still have index in jumping list after purge")
	}

	//test pickfrom after purge
	ud_out, _ = pool.PickFrom("test", standardVClock(53), txPoolGlobalUpdateOut{txGlobal, 0})

	ud, ok = ud_out.(txPeerUpdate)

	if !ok {
		t.Fatalf("type fail: %v", ud_out)
	}

	if ud.BeginSeries != 54 {
		t.Fatalf("unexpected begin: %d", ud.BeginSeries)
	}

	assertTxIsIdentify(t, txcache[55], ud.GetTransactions()[1])
	assertTxIsIdentify(t, txcache[59], ud.GetTransactions()[5])

}

func TestCatalogyHandler(t *testing.T) {

	defer func(bits uint) {
		SetPeerTxQueueLen(bits)
	}(peerTxQueueLenBit)

	SetPeerTxQueueLen(3)

	global := initGlobalStatus()
	l := initTestLedgerWrapper(t)
	txglobal := new(txPoolGlobal)
	txglobal.transactionPool = newTransactionPool(l)
	txglobal.network = global

	const testname = "test"

	txchainBase, txcache := populatePoolItems(t, 39)
	cache := newCache()
	cache.Import(txcache)

	pstatus := global.addNewPeer(testname)
	pstatus.Digest = txchainBase.head.digest
	pstatus.Endorsement = []byte{2, 3, 3}

	hotTx := new(hotTxCat)

	m := model.NewGossipModel(model.NewScuttlebuttStatus(txglobal))

	//try to build a proto directly
	dig_in := &pb.Gossip_Digest{Data: make(map[string]*pb.Gossip_Digest_PeerState)} //any epoch is ok

	dig_in.Data[testname] = &pb.Gossip_Digest_PeerState{}

	dig := hotTx.TransPbToDigest(dig_in)

	//now model should know peer test
	m.MakeUpdate(dig)

	dig = m.GenPullDigest()
	dig_out := hotTx.TransDigestToPb(dig)

	if _, ok := dig_out.Data[testname]; !ok {
		t.Fatal("model not known expected peer", dig_out)
	}

	var udt = txPeerUpdate{new(pb.HotTransactionBlock)}
	udt.fromTxs(txchainBase.fetch(1, nil), 0, cache)

	u_in := &pb.Gossip_Tx{map[string]*pb.HotTransactionBlock{testname: udt.HotTransactionBlock}}

	u, err := hotTx.DecodeUpdate(nil, u_in)
	if err != nil {
		t.Fatal("decode update fail", err)
	}

	err = m.RecvUpdate(u)
	if err != nil {
		t.Fatal("do update fail", err)
	}

	indexs := formTestData(l, txchainBase, [][]int{nil, []int{8, 12, 15}, []int{23, 13}, []int{7, 38}}, cache)
	defcache := txglobal.AcquireCache(testname, 0, txchainBase.lastSeries()+1)

	//now you can get tx from ledger or ind of txGlobal
	checkTx := func(pos int) {
		txid := indexs[pos].txid

		if txid == "" {
			t.Fatal("unexpected empty txid")
		}

		txItem, _ := defcache.GetTx(uint64(pos), txid)
		if txItem == nil {
			t.Fatalf("get tx %d in index fail", txid)
		}

		assertTxIsIdentify(t, txcache[pos], txItem)
	}

	checkTx(10)
	checkTx(12)
	checkTx(20)
	checkTx(23)
	checkTx(35)

	dig_in.Data[testname].Num = 20

	blk, _ := l.GetBlockByNumber(3)
	dig_in.Epoch = blk.GetStateHash()
	if len(dig_in.Epoch) == 0 {
		panic("no state hash")
	}

	dig = hotTx.TransPbToDigest(dig_in)

	u_out, ok := hotTx.EncodeUpdate(nil, m.MakeUpdate(dig), new(pb.Gossip_Tx)).(*pb.Gossip_Tx)
	if !ok {
		panic("type error, not gossip_tx")
	}

	if txs, ok := u_out.Txs[testname]; !ok {
		t.Fatal("update not include expected peer")
	} else {

		t.Log(txs.Transactions)

		if len(txs.Transactions) != 19 {
			t.Fatal("unexpected size of update:", len(txs.Transactions))
		} else if txs.BeginSeries != 21 {
			t.Fatal("unexpected begin of begin series:", txs.BeginSeries)
		}

		if !isLiteTx(txs.GetTransactions()[2]) {
			t.Fatalf("unexpected full-tx <23> (at 2)")
		}

		assertTxIsIdentify(t, txcache[21], txs.GetTransactions()[0])
		assertTxIsIdentify(t, txcache[38], txs.GetTransactions()[17])
		assertTxIsIdentify(t, txcache[27], txs.GetTransactions()[6])
		assertTxIsIdentify(t, txcache[39], txs.GetTransactions()[18])

	}
	//commit more tx
	l.BeginTxBatch(1)
	err = l.CommitTxBatch(1, []*pb.Transaction{txcache[21], txcache[27], txcache[39]}, nil, []byte("proof2"))
	if err != nil {
		t.Fatal("commit more block fail", err)
	}

	checkTx(21)
	if _, committedH := defcache.GetTx(21, indexs[21].txid); committedH != 5 {
		t.Fatal("commit update fail:", committedH)
	}

	del_commit := model.NewscuttlebuttUpdate(nil)
	del_commit.RemovePeers([]string{testname})

	err = m.RecvUpdate(del_commit)
	if err != nil {
		t.Fatal("do update fail", err)
	}

	if len(txglobal.AcquireCache(testname, 0, 0).peerTxCache[0]) > 0 {
		t.Fatal("status still have ghost index")
	}
}
