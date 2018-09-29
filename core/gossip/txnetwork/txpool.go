package txnetwork

import (
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
	"sync"
)

type TxPreHandler interface {
	TransactionPreValidation(tx *pb.Transaction) (*pb.Transaction, error)
}

type cachedTx struct {
	*pb.Transaction
	commitedH uint64
}

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

func SetPeerTxQueueLen(bits uint) {
	if bits == 0 || bits > 10 {
		panic("invalid bit count")
	}
	peerTxQueueLenBit = bits
	peerTxQueueMask = (1 << (bits)) - 1
	peerTxQueueLen = int(peerTxQueueMask) + 1
}

type peerTxCache [peerTxQueues][]cachedTx

func dequeueLoc(pos uint64) (loc int, qpos int) {
	loc = int(uint(pos>>peerTxQueueLenBit) & peerTxMask)
	qpos = int(uint(pos) & peerTxQueueMask)
	return
}

func (c *peerTxCache) pick(pos uint64) *cachedTx {

	loc, qpos := dequeueLoc(pos)
	return &c[loc][qpos]
}

//return a dequeue of append space (it maybe overlapped if the size is exceed
//the limit, that is, peerTxQueues * peerTxQueueLen)
func (c *peerTxCache) append(from uint64, size int) (ret [][]cachedTx) {

	loc, qpos := dequeueLoc(from)
	if c[loc] == nil {
		c[loc] = make([]cachedTx, peerTxQueueLen)
	}

	for size > 0 {

		if qpos+size > peerTxQueueLen {
			ret = append(ret, c[loc][qpos:peerTxQueueLen])
			size = size - (peerTxQueueLen - qpos)
			qpos = 0
			loc = int(uint(loc+1) & peerTxMask)
			c[loc] = make([]cachedTx, peerTxQueueLen)
		} else {
			ret = append(ret, c[loc][qpos:qpos+size])
			size = 0
		}
	}
	return
}

func (c *peerTxCache) prune(from uint64, to uint64) {

	locBeg := from >> peerTxQueueLenBit
	logTo := to >> peerTxQueueLenBit
	for i := locBeg; i < logTo; i++ {
		//drop the whole array
		c[int(uint(i)&peerTxMask)] = nil
	}

}

type transactionPool struct {
	sync.RWMutex
	ledger    *ledger.Ledger
	preH      TxPreHandler
	peerCache map[string]*peerTxCache
}

func newTransactionPool(ledger *ledger.Ledger) *transactionPool {
	ret := new(transactionPool)
	ret.peerCache = make(map[string]*peerTxCache)
	ret.ledger = ledger

	return ret
}

type txCache interface {
	GetTx(uint64, string) (*pb.Transaction, uint64)
}

//the cache is supposed to be handled only by single goroutine
type peerCache struct {
	*peerTxCache
	beg    uint64
	last   uint64
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

func (c *peerCache) GetTx(series uint64, txid string) (*pb.Transaction, uint64) {

	if series < c.beg || series >= c.last {
		//can we declaim this?
		logger.Fatalf("Accept outbound series: [%d], code is wrong", series)
		return nil, 0
	}

	pos := c.pick(series)
	if pos.commitedH == 0 {
		tx := c.parent.ledger.GetPooledTransaction(txid)
		//tx is still pooled
		if tx != nil {
			return tx, 0
		}
	}

	if pos.Transaction == nil {

		//cache is erased, we try to recover it first
		tx, err := c.parent.ledger.GetTransactionByID(txid)
		if tx == nil {
			logger.Errorf("Can not find Tx %s from ledger again: [%s]", err)
			return nil, 0
		}

		//why tx is missed? (because of block reversed, or just commited?) we check it
		pos.commitedH = getTxCommitHeight(c.parent.ledger, txid)
		if pos.commitedH != 0 {
			//so tx has been commited, can cache again
			pos.Transaction = tx
		} else {
			//tx can be re pooling here if it was lost before, but we should not encourage
			//this behavoir
			logger.Infof("Repool Tx %s [series %d] to ledger again", txid, series)
			c.parent.ledger.PoolTransactions([]*pb.Transaction{tx})
		}

		return tx, pos.commitedH
	}

	return pos.Transaction, pos.commitedH
}

func (c *peerCache) AddTxs(txs []*pb.Transaction, nocheck bool) {

	pooltxs := make([]*pb.Transaction, 0, len(txs))
	added := c.append(c.last, len(txs))
	var txspos int

	for _, q := range added {
		for i := 0; i < len(q); i++ {
			tx := txs[txspos]
			var commitedH uint64
			if !nocheck {
				commitedH, _, _ = c.parent.ledger.GetBlockNumberByTxid(tx.GetTxid())
			}

			if commitedH == 0 {
				pooltxs = append(pooltxs, tx)
			} else {
				//NOTICE: we only put "commited" tx into cache, so if the block is
				//reversed, we can simply erase all cache and recheck every tx
				//we touched later
				q[i] = cachedTx{tx, commitedH}
			}
			txspos++
		}
	}

	//sanity check
	if txspos != len(txs) {
		panic("AddTxs encounter wrong subscript")
	}

	c.parent.ledger.PoolTransactions(pooltxs)
}

func (c *peerCache) PurneCache(to uint64) {
	c.prune(c.beg, to)
}

func (tp *transactionPool) AcquireCache(peer string, beg uint64, last uint64) *peerCache {

	tp.RLock()

	c, ok := tp.peerCache[peer]

	if !ok {
		tp.RUnlock()

		c = new(peerTxCache)
		tp.Lock()
		defer tp.Unlock()
		tp.peerCache[peer] = c
		return &peerCache{c, beg, last, tp}
	}

	tp.RUnlock()
	return &peerCache{c, beg, last, tp}
}

func (tp *transactionPool) RemoveCache(peer string) {
	tp.Lock()
	defer tp.Unlock()

	delete(tp.peerCache, peer)
}
