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
var peerTxQueueMask int

func init() {
	peerTxQueueMask = int(1<<(peerTxQueueLenBit-1)) - 1
}

func PeerTxQueueLimit() int {
	return peerTxQueues * (peerTxQueueMask + 1)
}

func SetPeerTxQueueLen(bits uint) {
	if bits == 0 || bits > 10 {
		panic("invalid bit count")
	}
	peerTxQueueLenBit = bits
	peerTxQueueMask = int(1<<(bits-1)) - 1
}

type peerTxCache struct {
	q    [peerTxQueues][]cachedTx
	beg  uint64
	last uint64
}

func (c *peerTxCache) checkRange(pos uint64) bool {
	return pos >= c.beg && pos < c.last
}

func dequeueLoc(pos uint64) (loc int, qpos int) {
	loc = int(uint(pos>>peerTxQueueLenBit) & peerTxMask)
	qpos = int(pos & uint64(peerTxQueueMask))
}

func (c *peerTxCache) pick(pos uint64) (*pb.Transaction, uint64) {

	loc, qpos := dequeueLoc(pos)

	ret := c.q[loc][qpos]
	return ret.Transaction, ret.commitedH
}

//return a dequeue of append space (it maybe overlapped if the size is exceed
//the limit, that is, peerTxQueues * peerTxQueueLen)
func (c *peerTxCache) append(size int) (ret [][]cachedTx) {

	loc, qpos := dequeueLoc(c.last)

	for size > 0 {

		if qpos+size > peerTxQueueMask+1 {
			ret = append(ret, c.q[loc][qpos:peerTxQueueMask])
			size = size - (peerTxQueueMask + 1 - qpos)
			qpos = 0
			loc++
			c.q[loc] = make([]cachedTx, peerTxQueueMask+1)
		} else {
			ret = append(ret, c.q[loc][qpos:qpos+size-1])
			size = 0
		}
	}
	c.last = c.last + size
	return
}

func (c *peerTxCache) prune(size int) {

	pruneTo := c.beg + size
	if pruneTo > c.last {
		pruneTo = c.last
	}

	locBeg := c.beg >> peerTxQueueLenBit
	logTo := pruneTo >> peerTxQueueLenBit
	for i := locBeg; i < logTo; i++ {
		//drop the whole array
		c.q[int(uint(i)&peerTxMask)] = nil
	}

	c.beg = pruneTo

}

type transactionPool struct {
	sync.RWMutex
	ledger    *ledger.Ledger
	preH      TxPreHandler
	peerCache map[string]*peerTxCache
}

func newTransactionPool(ledger *ledger.Ledger) *transactionPool {
	ret := new(transactionPool)
	ret.peerCache = make(map[string]map[string]cachedTx)
	ret.ledger = ledger

	return ret
}

type txCache interface {
	GetTx(uint64, string) (*pb.Transaction, uint64)
}

//the cache is supposed to be handled only by single goroutine
type peerCache struct {
	*peerTxCache
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

	if i, ok := c.pick(series); ok {

		if i.commitedH == 0 {
			//we must always double check the pooling status
			i.commitedH = getTxCommitHeight(c.parent.ledger, txid)
		}

		return i.Transaction, i.commitedH
	} else {

		//cache is erased, we try to recover it
		tx, err := c.parent.ledger.GetTransactionByID(txid)
		if tx == nil {
			logger.Errorf("Can not find Tx %s from ledger again: [%s]", err)
			return nil, 0
		}

		commitedH := getTxCommitHeight(c.parent.ledger, txid)

		c.c[txid] = cachedTx{tx, commitedH}
		return tx, commitedH
	}
}

func (c *peerCache) AddTxs(txs []*pb.Transaction, nocheck bool) {

	pooltxs := make([]*pb.Transaction, 0, len(txs))
	added := c.append(len(txs))
	var txspos int

	for _, q := range added {
		for i := 0; i < len(q); i++ {
			tx := txs[txspos]
			var commitedH uint64
			if !nocheck {
				commitedH, _, _ = c.parent.ledger.GetBlockNumberByTxid(tx.GetTxid())
			}

			q[i] = cachedTx{tx, commitedH}

			if commitedH == 0 {
				pooltxs = append(pooltxs, tx)
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

func (tp *transactionPool) NewCache() {
	tp.peerCache = make(map[string]map[string]cachedTx)
}

func (tp *transactionPool) AcquireCache(peer string) *peerCache {

	tp.RLock()

	c, ok := tp.peerCache[peer]

	if !ok {
		tp.RUnlock()

		c = new(peerTxCache)
		tp.Lock()
		defer tp.Unlock()
		tp.peerCache[peer] = c
		return &peerCache{c, tp}
	}

	tp.RUnlock()
	return &peerCache{c, tp}
}

func (tp *transactionPool) RemoveCache(peer string) {
	tp.Lock()
	defer tp.Unlock()

	delete(tp.peerCache, peer)
}
