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

type transactionPool struct {
	sync.RWMutex
	ledger    *ledger.Ledger
	preH      TxPreHandler
	peerCache map[string]map[string]cachedTx
}

func newTransactionPool(ledger *ledger.Ledger) *transactionPool {
	ret := new(transactionPool)
	ret.peerCache = make(map[string]map[string]cachedTx)
	ret.ledger = ledger

	return ret
}

//the cache is supposed to be handled only by single goroutine
type peerCache struct {
	c      map[string]cachedTx
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

func (c *peerCache) GetTx(txid string) (*pb.Transaction, uint64) {

	if i, ok := c.c[txid]; ok {

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

	for _, tx := range txs {
		var commitedH uint64
		if !nocheck {
			commitedH, _, _ = c.parent.ledger.GetBlockNumberByTxid(tx.GetTxid())
		}

		c.c[tx.GetTxid()] = cachedTx{tx, commitedH}

		if commitedH == 0 {
			pooltxs = append(pooltxs, tx)
		}
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

		c = make(map[string]cachedTx)
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
