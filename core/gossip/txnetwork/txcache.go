package txnetwork

import (
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
)

//a deque struct mapped the tx chain into arrays

const peerTxQueues = 16
const peerTxMask = uint(peerTxQueues) - 1

var peerTxQueueLenBit = uint(7) //2^7=128
var peerTxQueueMask uint
var peerTxQueueLen int

func init() {
	peerTxQueueMask = (1 << (peerTxQueueLenBit)) - 1
	peerTxQueueLen = int(peerTxQueueMask) + 1
}

func PeerTxQueueLimit() int {
	return peerTxQueues * peerTxQueueLen
}

func PeerTxQueueLen() int {
	return peerTxQueueLen
}

func SetPeerTxQueueLen(bits uint) {
	if bits == 0 || bits > 10 {
		panic("invalid bit count")
	}
	peerTxQueueLenBit = bits
	peerTxQueueMask = (1 << (bits)) - 1
	peerTxQueueLen = int(peerTxQueueMask) + 1
}

type commitData [peerTxQueues][]uint64

func dequeueLoc(pos uint64) (loc int, qpos int) {
	loc = int(uint(pos>>peerTxQueueLenBit) & peerTxMask)
	qpos = int(uint(pos) & peerTxQueueMask)
	return
}

func (c *commitData) pick(pos uint64) *uint64 {

	loc, qpos := dequeueLoc(pos)
	return &c[loc][qpos]
}

//return a dequeue of append space (it maybe overlapped if the size is exceed
//the limit, that is, peerTxQueues * peerTxQueueLen)
func (c *commitData) append(from uint64, size int) (ret [][]uint64) {

	loc, qpos := dequeueLoc(from)
	if c[loc] == nil {
		c[loc] = make([]uint64, peerTxQueueLen)
	}

	for size > 0 {

		if qpos+size > peerTxQueueLen {
			ret = append(ret, c[loc][qpos:peerTxQueueLen])
			size = size - (peerTxQueueLen - qpos)
			qpos = 0
			loc = int(uint(loc+1) & peerTxMask)
			c[loc] = make([]uint64, peerTxQueueLen)
		} else {
			ret = append(ret, c[loc][qpos:qpos+size])
			size = 0
		}
	}
	return
}

func (c *commitData) pruning(from uint64, to uint64) {

	locBeg := from >> peerTxQueueLenBit
	logTo := to >> peerTxQueueLenBit
	for i := locBeg; i < logTo; i++ {
		//drop the whole array
		c[int(uint(i)&peerTxMask)] = nil
	}

}

type TxCache interface {
	GetCommit(series uint64, tx *pb.Transaction) uint64
	AddTxs(from uint64, txs []*pb.Transaction, nocheck bool) error
	Pruning(from uint64, to uint64)
}

//the cache is supposed to be handled only by single goroutine
type txCache struct {
	*commitData
	id     string
	parent *transactionPool
}

func getTxCommitHeight(l *ledger.Ledger, txid string) uint64 {

	if l.GetPooledTransaction(txid) != nil {
		return 0
	}

	h, _, err := l.GetBlockNumberByTxid(txid)
	if err != nil {
		logger.Errorf("Can not find index of Tx %s from ledger", txid)
		//TODO: should we still consider it is pending?
		return 0
	}

	return h

}

func (c *txCache) GetCommit(series uint64, tx *pb.Transaction) uint64 {

	pos := c.commitData.pick(series)
	if *pos == 0 {
		if tx := c.parent.ledger.GetPooledTransaction(tx.GetTxid()); tx != nil {
			//tx is still being pooled
			return 0
		}
		//or tx is commited, we need to update the commitH

		if h := getTxCommitHeight(c.parent.ledger, tx.GetTxid()); h == 0 {
			//tx can be re pooling here if it was lost before, but we should not encourage
			//this behavoir
			logger.Infof("Repool Tx {%s} [series %d] to ledger again", tx.GetTxid(), series)
			c.parent.ledger.PoolTransactions([]*pb.Transaction{tx})
		} else {
			*pos = h
		}
	}

	return *pos
}

func (c *txCache) AddTxs(from uint64, txs []*pb.Transaction, nocheck bool) error {

	udt := txPeerUpdate{&pb.HotTransactionBlock{Transactions: txs}}
	var err error
	if !nocheck {
		if c.parent.txHandler != nil {
			preHandler, err := c.parent.txHandler.GetPreHandler(c.id)
			if err != nil {
				return err
			}
			_, err = udt.completeTxs(c.parent.ledger, preHandler)
		} else {
			_, err = udt.completeTxs(c.parent.ledger, nil)
		}

	}

	if err != nil {
		return err
	}
	c.commitData.append(from, len(txs))
	/* 	var txspos int
	   	_ := c.commitData.append(from, len(txs))

	   	for _, q := range added {
	   		for i := 0; i < len(q); i++ {
	   			tx := txs[txspos]
	   			var commitedH uint64
	   			if !nocheck {
	   				commitedH, _, _ = c.parent.ledger.GetBlockNumberByTxid(tx.GetTxid())
	   			}

	   			if commitedH == 0 {
	   				pooltxs = append(pooltxs, tx)
	   			}

	   			q[i] = commitedH
	   			txspos++
	   		}
	   	}

	   	//sanity check
	   	if txspos != len(txs) {
	   		panic("AddTxs encounter wrong subscript")
	   	} */

	c.parent.ledger.PoolTransactions(txs)
	return nil
}

func (c *txCache) Pruning(from uint64, to uint64) {
	c.commitData.pruning(from, to)
}
