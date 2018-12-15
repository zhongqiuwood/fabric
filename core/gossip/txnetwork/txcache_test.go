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

func TestData(t *testing.T) {

	queueLen := peerTxQueueLen
	queueLenPart := queueLen / 4
	if queueLenPart <= 3 {
		t.Fatal("We have a too small queue len", queueLen)
	}

	cycleLen := PeerTxQueueLimit()

	data := new(commitData)

	//ensure we have a single row array
	ret1 := data.append(0, 3)

	if len(ret1) != 1 || len(ret1[0]) != 3 {
		t.Fatal("wrong append space", ret1)
	}

	ret1[0][2] = 1

	if data[0][2] != 1 {
		t.Fatal("wrong position in append space", data)
	}

	//a bit longer array
	ret2 := data.append(3, queueLenPart)

	if len(ret2) != 1 || len(ret2[0]) != queueLenPart {
		t.Fatal("wrong append space", ret2)
	}

	ret2[0][0] = 2

	if data[0][3] != 2 || data[0][2] != 1 {
		t.Fatal("wrong position in append space", data)
	}

	//an array cross two queues
	ret3 := data.append(3, queueLen)

	if len(ret3) != 2 || len(ret3[0]) != queueLen-3 || len(ret3[1]) != 3 {
		t.Fatal("wrong append space", ret3)
	}

	ret3[1][0] = 3

	if data[1][0] != 3 {
		t.Fatal("wrong position in append space", data)
	}

	//an array cross 3 queues
	ret4 := data.append(3, queueLen*2)

	if len(ret4) != 3 || len(ret4[0]) != queueLen-3 || len(ret4[1]) != queueLen || len(ret4[2]) != 3 {
		t.Fatal("wrong append space", ret4)
	}

	ret4[0][queueLen-4] = 4

	if data[0][peerTxQueueMask] != 4 {
		t.Fatal("wrong position in append space", data)
	}

	//an array just fit the tail
	ret5 := data.append(3, queueLen*2-3)

	if len(ret5) != 2 || len(ret5[0]) != queueLen-3 || len(ret5[1]) != queueLen {
		t.Fatal("wrong append space", ret5)
	}

	ret5[1][peerTxQueueMask] = 5

	if data[1][peerTxQueueMask] != 5 {
		t.Fatal("wrong position in append space", data)
	}

	//an array just fit one row
	ret6 := data.append(uint64(3+queueLen*2), queueLen-3)

	if len(ret6) != 1 || len(ret6[0]) != queueLen-3 {
		t.Fatal("wrong append space", ret6)
	}

	ret6[0][0] = 6

	if data[2][3] != 6 {
		t.Fatal("wrong position in append space", data)
	}

	//cycling
	ret7 := data.append(uint64(cycleLen-queueLenPart+3), queueLenPart)

	if len(ret7) != 2 || len(ret7[1]) != 3 {
		t.Fatal("wrong append space", ret7)
	}

	ret7[1][0] = 7

	if data[0][0] != 7 {
		t.Fatal("wrong position in append space", data)
	}

	ret7[0][0] = 71

	if data[peerTxMask][queueLen-queueLenPart+3] != 71 {
		t.Fatal("wrong position in append space", data)
	}

	//start prune ...
	if data[0] == nil || data[1] == nil || data[2] == nil {
		t.Fatal("wrong initial status")
	}

	//second row
	data.pruning(uint64(queueLen+3), uint64(queueLen*2+3))
	if data[1] != nil || data[2] == nil {
		t.Fatal("wrong prune")
	}

	//nothing
	data.pruning(uint64(queueLen+3), uint64(queueLen*2+queueLenPart))
	if data[2] == nil {
		t.Fatal("wrong prune")
	}

	//cycling
	data.pruning(uint64(cycleLen-queueLenPart+3), uint64(cycleLen+3))
	if data[peerTxMask] != nil || data[0] == nil {
		t.Fatal("wrong prune")
	}
}

func TestCommitting(t *testing.T) {

	ledger := initTestLedgerWrapper(t)

	//add gensis block
	ledger.BeginTxBatch(0)
	ledger.CommitTxBatch(0, nil, nil, nil)

	txpool := newTransactionPool(ledger)

	queueLen := peerTxQueueLen
	queueLenPart := queueLen / 4
	if queueLenPart < 3 {
		t.Fatal("We have a too small queue len", queueLen)
	}

	cache := txpool.AcquireCaches("any").commitData

	//generate a collection of txs large enough...
	txcollection := make([]*pb.Transaction, queueLen*3+queueLenPart)
	for i, _ := range txcollection {
		tx, _ := buildTestTx(t)
		if i > 0 {
			d, _ := txcollection[i-1].Digest()
			tx = buildPrecededTx(d, tx)
		}
		txcollection[i] = tx
	}

	last := uint64(1)
	txpool.AcquireCaches("any").AddTxs(last, txcollection[1:queueLenPart])
	last = last + uint64(queueLenPart-1)

	//commit part of it
	ledger.BeginTxBatch(1)
	err := ledger.CommitTxBatch(1, []*pb.Transaction{txcollection[1], txcollection[2], txcollection[4], txcollection[5]}, nil, []byte("proof1"))
	if err != nil {
		t.Fatal("commit fail", err)
	}

	if cache[0][2] != 0 || cache[0][3] != 0 || cache[0][4] != 0 {
		t.Fatal("has wrong commitH", cache[0][:5])
	}

	rcache := txpool.AcquireCaches("any")
	ch := rcache.GetCommit(3, txcollection[3])
	if ch != 0 {
		t.Fatal("get uncommit tx fail", ch, txcollection[3])
	}
	ch = rcache.GetCommit(2, txcollection[2])
	if ch != 1 {
		t.Fatal("get commited tx fail", ch, txcollection[2])
	}
	ch = rcache.GetCommit(4, txcollection[4])
	if ch != 1 {
		t.Fatal("get commited tx fail", ch, txcollection[4])
	}
	if cache[0][2] != 1 || cache[0][3] != 0 || cache[0][4] != 1 {
		t.Fatal("has wrong commitH after update", cache[0][:5])
	}

	//pre commit another block
	ledger.BeginTxBatch(1)
	err = ledger.CommitTxBatch(1, []*pb.Transaction{txcollection[queueLen]}, nil, []byte("proof1"))
	if err != nil {
		t.Fatal("commit fail", err)
	}

	err = txpool.AcquireCaches("any").AddTxs(last, txcollection[queueLenPart:queueLen+2*queueLenPart])
	if err != nil {
		t.Fatal("add txs fail", err)
	}
	last = last + uint64(queueLen+queueLenPart)

	ch = rcache.GetCommit(uint64(queueLen), txcollection[queueLen])
	if ch != 2 || cache[1][0] != 2 {
		t.Fatal("Wrong commit status ", cache[1][:5], ch)
	}

	ch = rcache.GetCommit(uint64(queueLen-1), txcollection[queueLen-1])
	if ch != 0 {
		t.Fatal("get uncommit tx fail", ch)
	}
	ch = rcache.GetCommit(uint64(queueLen+1), txcollection[queueLen+1])
	if ch != 0 {
		t.Fatal("get uncommit tx fail", ch)
	}

	//repooling
	txpool.onCommit([]string{txcollection[3].GetTxid()}, 255)
	if _, ok := txpool.cPendingTxs[txcollection[3].GetTxid()]; ok {
		t.Fatal("tx is not pruned")
	}

	rcache.GetCommit(3, txcollection[3])
	if _, ok := txpool.cPendingTxs[txcollection[3].GetTxid()]; !ok {
		t.Fatal("tx is not repooled")
	}

}
