package txnetwork

import (
	pb "github.com/abchain/fabric/protos"
	"testing"
)

func TestBaseSettings(t *testing.T) {

	defer func(bits uint) {
		SetPeerTxQueueLen(bits)

		if r := recover(); r == nil {
			t.Fatal("No panic in last test")
		}

	}(peerTxQueueLenBit)

	SetPeerTxQueueLen(3)
	if peerTxQueueMask != 7 {
		t.Fatal("Wrong tx queue len", peerTxQueueMask+1)
	}

	SetPeerTxQueueLen(8)
	if peerTxQueueMask != 255 {
		t.Fatal("Wrong tx queue len", peerTxQueueMask+1)
	}

	SetPeerTxQueueLen(0)
}

func TestTxCache(t *testing.T) {

	queueLen := peerTxQueueMask + 1
	queueLenPart := queueLen / 4
	if queueLenPart <= 3 {
		t.Fatal("We have a too small queue len", queueLen)
	}

	cycleLen := PeerTxQueueLimit()

	txcache := new(peerTxCache)
	tx, id := buildTestTx(t)

	//ensure we have a single row array
	ret1 := txcache.append(0, 3)

	if len(ret1) != 1 || len(ret1[0]) != 3 {
		t.Fatal("wrong append space", ret1)
	}

	ret1[0][2] = cachedTx{tx, 0}

	if txcache[0][2].Transaction == nil || txcache[0][2].Transaction.GetTxid() != id {
		t.Fatal("wrong position in append space", txcache)
	}

	//a bit longer array
	ret2 := txcache.append(3, queueLenPart)

	if len(ret2) != 1 || len(ret2[0]) != queueLenPart {
		t.Fatal("wrong append space", ret2)
	}

	ret2[0][0] = cachedTx{tx, 1}

	if txcache[0][3].commitedH != 1 {
		t.Fatal("wrong position in append space", txcache)
	}

	//an array cross two queues
	ret3 := txcache.append(3, queueLen)

	if len(ret3) != 2 || len(ret3[0]) != queueLen-3 || len(ret3[1]) != 3 {
		t.Fatal("wrong append space", ret3)
	}

	ret3[1][0] = cachedTx{tx, 2}

	if txcache[1][0].commitedH != 2 {
		t.Fatal("wrong position in append space", txcache)
	}

	//an array cross 3 queues
	ret4 := txcache.append(3, queueLen*2)

	if len(ret4) != 3 || len(ret4[0]) != queueLen-3 || len(ret4[1]) != queueLen || len(ret4[2]) != 3 {
		t.Fatal("wrong append space", ret4)
	}

	ret4[0][queueLen-4] = cachedTx{tx, 3}

	if txcache[0][peerTxQueueMask].commitedH != 3 {
		t.Fatal("wrong position in append space", txcache)
	}

	//an array just fit the tail
	ret5 := txcache.append(3, queueLen*2-3)

	if len(ret5) != 2 || len(ret5[0]) != queueLen-3 || len(ret5[1]) != queueLen {
		t.Fatal("wrong append space", ret5)
	}

	ret5[1][peerTxQueueMask] = cachedTx{tx, 4}

	if txcache[1][peerTxQueueMask].commitedH != 4 {
		t.Fatal("wrong position in append space", txcache)
	}

	//an array just fit one row
	ret6 := txcache.append(uint64(3+queueLen*2), queueLen-3)

	if len(ret6) != 1 || len(ret6[0]) != queueLen-3 {
		t.Fatal("wrong append space", ret6)
	}

	ret6[0][0] = cachedTx{tx, 5}

	if txcache[2][3].commitedH != 5 {
		t.Fatal("wrong position in append space", txcache)
	}

	//cycling
	ret7 := txcache.append(uint64(cycleLen-queueLenPart+3), queueLenPart)

	if len(ret7) != 2 || len(ret7[1]) != 3 {
		t.Fatal("wrong append space", ret7)
	}

	ret7[1][0] = cachedTx{tx, 6}

	if txcache[0][0].commitedH != 6 {
		t.Fatal("wrong position in append space", txcache)
	}

	ret7[0][0] = cachedTx{tx, 7}

	if txcache[peerTxMask][queueLen-queueLenPart+3].commitedH != 7 {
		t.Fatal("wrong position in append space", txcache)
	}

	//start prune ...
	if txcache[0] == nil || txcache[1] == nil || txcache[2] == nil {
		t.Fatal("wrong initial status")
	}

	//second row
	txcache.prune(uint64(queueLen+3), uint64(queueLen*2+3))
	if txcache[1] != nil || txcache[2] == nil {
		t.Fatal("wrong prune")
	}

	//nothing
	txcache.prune(uint64(queueLen+3), uint64(queueLen*2+queueLenPart))
	if txcache[2] == nil {
		t.Fatal("wrong prune")
	}

	//cycling
	txcache.prune(uint64(cycleLen-queueLenPart+3), uint64(cycleLen+3))
	if txcache[peerTxMask] != nil || txcache[0] == nil {
		t.Fatal("wrong prune")
	}
}

func TestCache(t *testing.T) {

	initGlobalStatus()
	ledger := initTestLedgerWrapper(t)
	txpool := newTransactionPool(ledger)

	queueLen := peerTxQueueMask + 1
	queueLenPart := queueLen / 4
	if queueLenPart < 3 {
		t.Fatal("We have a too small queue len", queueLen)
	}

	cache := txpool.AcquireCache("any", 0, 0).peerTxCache

	//generate a collection of txs large enough...
	txcollection := make([]*pb.Transaction, queueLen*3+queueLenPart)
	for i, _ := range txcollection {
		txcollection[i], _ = buildTestTx(t)
	}

	//and commit part of it
	// ledger.BeginTxBatch(1)
	// ledger.TxBegin("txUuid")
	// ledger.SetState("chaincode1", "keybase", []byte{byte(ib)})
	// ledger.TxFinished("txUuid", true)
	// ledger.CommitTxBatch(1, genTxs(commitsetting[ib]), nil, []byte("proof1"))

	last := uint64(1)
	txpool.AcquireCache("any", 0, last).AddTxs(txcollection[1:queueLenPart], true)

	last = last + queueLenPart - 1
	rcache := txpool.AcquireCache("any", 0, last)
	if txcollection[3].Txid != rcache.GetTx(3, txcollection[3].Txid) {
		t.Fatal("Wrong cache", cache)
	}
}
