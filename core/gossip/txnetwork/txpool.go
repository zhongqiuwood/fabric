package txnetwork

import (
	"fmt"
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
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
	ledger *ledger.Ledger
	preH   TxPreHandler
	txPool map[string]*pb.Transaction
	//use for temporary writing when an long journey of reading is on the way
	txPoolWrite map[string]*pb.Transaction
	peerCache   map[string]map[string]cachedTx
}

func newTransactionPool(ledger *ledger.Ledger) *transactionPool {
	ret := new(transactionPool)
	ret.txPool = make(map[string]*pb.Transaction)
	ret.peerCache = make(map[string]map[string]cachedTx)
	ret.ledger = ledger

	return ret
}

//the cache is supposed to be handled only by single goroutine
type peerCache struct {
	c      map[string]cachedTx
	parent *transactionPool
}

func (c *peerCache) GetTx(txid string) (*pb.Transaction, uint64) {

	if i, ok := c.c[txid]; ok {

		if i.commitedH == 0 {
			//we must always double check the pooling status
			if !c.parent.checkTxPoolStatus(txid) {
				//if tx is not pooled yet we must update ...
				i.commitedH = c.parent.repoolTx(i.Transaction)
			}
		}

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

func (tp *transactionPool) RemoveCache(peer string) {
	tp.Lock()
	defer tp.Unlock()

	delete(tp.peerCache, peer)
}

//check if tx is still pooled
func (tp *transactionPool) checkTxPoolStatus(txid string) (ok bool) {

	tp.RLock()
	defer tp.RUnlock()
	_, ok = tp.txPool[txid]
	if ok {
		return
	}

	//also check updating set
	if tp.txPoolWrite != nil {
		_, ok = tp.txPoolWrite[txid]
		if ok {
			return
		}
	}

	return
}

func (tp *transactionPool) simplePoolTx(tx *pb.Transaction) {

	tp.Lock()
	defer tp.Unlock()

	if tp.txPoolWrite != nil {
		tp.txPoolWrite[tx.GetTxid()] = tx
	} else {
		tp.txPool[tx.GetTxid()] = tx
	}
}

func (tp *transactionPool) repoolTx(tx *pb.Transaction) uint64 {

	if h, _, err := tp.ledger.GetBlockNumberByTxid(tx.GetTxid()); err == nil && h > 0 {
		return h
	} else {
		tp.simplePoolTx(tx)
		return 0
	}
}

//wrapping a read-only version of the current pooling (the snapshot)
type poolStatus struct {
	TxPool map[string]*pb.Transaction
	parent *transactionPool
}

//only one long-journey edition is allowed once
func (tp *transactionPool) OpenEdit(ctx context.Context) (chan *pb.Transaction, error) {

	tp.Lock()
	defer tp.Unlock()

	if tp.txPoolWrite != nil {
		return nil, fmt.Errorf("Long-read request duplicated")
	}

	tp.txPoolWrite = make(map[string]*pb.Transaction)

	out := make(chan *pb.Transaction)
	go func(pool map[string]*pb.Transaction) {
		defer close(out)
		for _, tx := range pool {
			select {
			case out <- tx:
			case <-ctx.Done():
				return
			}
		}
	}(tp.txPool)

	return out, nil
}

func (tp *transactionPool) EditPool(content map[string]*pb.Transaction) {
	tp.Lock()
	defer tp.Unlock()

	if tp.txPoolWrite == nil {
		panic("Edit pool without entering edit mode, wrong code")
	}

	tp.txPool = content
}

func (tp *transactionPool) EndEdit() {

	var mergeT, merger map[string]*pb.Transaction

	tp.Lock()
	defer tp.Unlock()

	//in most case, the count of PoolTx is small and the merging cost is trival
	if len(tp.txPoolWrite) < len(tp.txPool) {
		mergeT = tp.txPoolWrite
		merger = tp.txPool
	} else {
		merger = tp.txPoolWrite
		mergeT = tp.txPool
	}

	for id, tx := range mergeT {
		merger[id] = tx
	}

	tp.txPool = merger
	tp.txPoolWrite = nil

}

func (tp *transactionPool) GetPooledTxCnt() int {
	tp.RLock()
	defer tp.RUnlock()
	return len(tp.txPool) + len(tp.txPoolWrite)
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
