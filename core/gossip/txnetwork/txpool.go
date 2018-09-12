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
	txPool    map[string]*pb.Transaction
	txPoolCnt int
	peerCache map[string]map[string]cachedTx
}

func newTransactionPool() *transactionPool {
	ret := new(transactionPool)
	ret.txPool = make(map[string]*pb.Transaction)
	ret.peerCache = make(map[string]map[string]cachedTx)

	return ret
}

type peerCache struct {
	c      map[string]cachedTx
	parent *transactionPool
}

func (c *peerCache) GetTx(txid string) (*pb.Transaction, uint64) {

	if i, ok := c.c[txid]; ok {
		return i.Transaction, i.commitedH
	} else {
		tx, err := c.parent.ledger.GetTransactionByID(txid)
		if err != nil {
			logger.Errorf("Query Tx %s from ledger fail", err)
			return nil, 0
		} else if tx == nil {
			logger.Errorf("Can not find Tx %s from ledger again")
			return nil, 0
		}

		commitedH := c.parent.repoolTx(tx)

		c.c[txid] = cachedTx{tx, commitedH}
		return tx, commitedH
	}
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

func (tp *transactionPool) repoolTx(tx *pb.Transaction) uint64 {

	if h, _, err := tp.ledger.GetBlockNumberByTxid(tx.GetTxid()); err == nil && h > 0 {
		return h
	} else {
		//this tx is pooled
		tp.Lock()
		defer tp.Unlock()
		tp.txPool[tx.GetTxid()] = tx
		tp.txPoolCnt++
		return 0
	}
}

func (tp *transactionPool) GetPooledTxCnt() int {
	tp.RLock()
	defer tp.RUnlock()
	return tp.txPoolCnt
}

func (tp *transactionPool) HandlePooledTx() map[string]*pb.Transaction {
	tp.Lock()
	defer tp.Unlock()
	ret := tp.txPool
	tp.txPool = make(map[string]*pb.Transaction)
	return ret
}

//use this method to allow parallel updating and querying of tx pool
func (tp *transactionPool) FinishHandlingPooledTx(poolback map[string]*pb.Transaction) {

	var mergeT, merger map[string]*pb.Transaction

	tp.Lock()
	defer tp.Unlock()

	//in most case, the count of PoolTx is small and the merging cost is trival
	if len(poolback) < len(tp.txPool) {
		mergeT = poolback
		merger = tp.txPool
	} else {
		merger = poolback
		mergeT = tp.txPool
	}

	for id, tx := range mergeT {
		merger[id] = tx
	}

	tp.txPool = merger
	//remember to update pool count here
	tp.txPoolCnt = len(tp.txPool)

}

func (tp *transactionPool) PoolTx(tx *pb.Transaction) (uint64, error) {

	var err error
	if tp.preH != nil {
		tx, err = tp.preH.TransactionPreValidation(tx)
		if err != nil {
			return 0, err
		}
	}

	committedH := tp.repoolTx(tx)
	if committedH != 0 {
		logger.Debugf("Tx %s has been commited into block %d", tx.GetTxid(), committedH)
	}

	return committedH, nil
}
