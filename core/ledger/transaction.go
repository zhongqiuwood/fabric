package ledger

import (
	"errors"
	"github.com/abchain/fabric/core/db"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"sync"
)

// Transaction work for get/put transaction and holds "unconfirmed" transaction
type transactionPool struct {
	sync.RWMutex
	txPool map[string]*pb.Transaction
	//use for temporary reading in a long-journey
	txPoolSnapshot map[string]*pb.Transaction

	commitHooks []func([]string)
}

func newTxPool() (*transactionPool, error) {

	txpool := &transactionPool{}

	txpool.txPool = make(map[string]*pb.Transaction)

	return txpool, nil
}

func (tp *transactionPool) clearPool(txs []*pb.Transaction) {

	tp.Lock()
	defer tp.Unlock()
	if tp.txPoolSnapshot != nil {
		for _, tx := range txs {
			if _, ok := tp.txPoolSnapshot[tx.GetTxid()]; ok {
				tp.txPool[tx.GetTxid()] = nil
			} else {
				delete(tp.txPool, tx.GetTxid())
			}
		}
	} else {
		for _, tx := range txs {
			delete(tp.txPool, tx.GetTxid())
		}
	}
}

func (tp *transactionPool) poolTransaction(txs []*pb.Transaction) {

	tp.Lock()
	defer tp.Unlock()

	for _, tx := range txs {
		tp.txPool[tx.GetTxid()] = tx
	}
}

func (tp *transactionPool) putTransaction(txs []*pb.Transaction) error {

	err := db.GetGlobalDBHandle().PutTransactions(txs)
	if err != nil {
		return err
	}

	return nil
}

func (tp *transactionPool) commitTransaction(txids []string) error {

	pendingTxs := make([]*pb.Transaction, 0, len(txids))

	tp.RLock()
	if tp.txPoolSnapshot == nil {
		for _, id := range txids {
			if tx, ok := tp.txPool[id]; ok {
				pendingTxs = append(pendingTxs, tx)
			}
		}
	} else {
		for _, id := range txids {
			tx, ok := tp.txPool[id]
			if !ok {
				tx, ok = tp.txPoolSnapshot[id]
			}
			if ok {
				pendingTxs = append(pendingTxs, tx)
			}
		}
	}

	tp.RUnlock()

	defer tp.clearPool(pendingTxs)
	return tp.putTransaction(pendingTxs)
}

func (tp *transactionPool) getConfirmedTransaction(txID string) (*pb.Transaction, error) {
	return fetchTxFromDB(txID)
}

func (tp *transactionPool) getTransaction(txID string) (*pb.Transaction, error) {

	tp.RLock()
	tx, ok := tp.txPool[txID]
	if !ok && tp.txPoolSnapshot != nil {
		tx, ok = tp.txPoolSnapshot[txID]
	}
	tp.RUnlock()

	if !ok {

		tx, err := fetchTxFromDB(txID)
		if err != nil {
			return nil, err
		}

		if tx != nil {
			return tx, nil
		}

		return nil, ErrResourceNotFound
	}

	return tx, nil
}

func (tp *transactionPool) finishIteration(out chan *pb.Transaction) {
	defer close(out)
	tp.Lock()
	defer tp.Unlock()

	//we suppose the snapshot is larger than the (temporary generated) txPool
	for id, tx := range tp.txPool {
		if tx != nil {
			tp.txPoolSnapshot[id] = tx
		} else {
			delete(tp.txPoolSnapshot, id)
		}
	}

	tp.txPool = tp.txPoolSnapshot
	tp.txPoolSnapshot = nil
}

func (tp *transactionPool) getPooledTxCount() int {
	tp.RLock()
	defer tp.RUnlock()

	return len(tp.txPool) + len(tp.txPoolSnapshot)
}

func (tp *transactionPool) getPooledTx(txid string) *pb.Transaction {

	tp.RLock()
	defer tp.RUnlock()

	tx, ok := tp.txPool[txid]
	if !ok && tp.txPoolSnapshot != nil {
		tx = tp.txPoolSnapshot[txid]
	}

	return tx
}

//only one long-journey read is allowed once
func (tp *transactionPool) iteratePooledTx(ctx context.Context) (chan *pb.Transaction, error) {

	tp.Lock()
	defer tp.Unlock()

	if tp.txPoolSnapshot != nil {
		return nil, errors.New("Iterate-read request duplicated")
	}

	tp.txPoolSnapshot = tp.txPool
	tp.txPool = make(map[string]*pb.Transaction)

	out := make(chan *pb.Transaction)
	go func(pool map[string]*pb.Transaction) {
		defer tp.finishIteration(out)
		for _, tx := range pool {
			select {
			case out <- tx:
			case <-ctx.Done():
				return
			}
		}
	}(tp.txPoolSnapshot)

	return out, nil
}

//shared by block struct's access
func fetchTxFromDB(txID string) (*pb.Transaction, error) {
	return db.GetGlobalDBHandle().GetTransaction(txID)
}

func fetchTxsFromDB(txIDs []string) []*pb.Transaction {
	return db.GetGlobalDBHandle().GetTransactions(txIDs)
}
