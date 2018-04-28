package ledger

import (
	"github.com/abchain/fabric/core/db"
	pb "github.com/abchain/fabric/protos"
	"sync"
)

// Transaction work for get/put transaction and holds "unconfirmed" transaction
type transactionPool struct {
	sync.RWMutex
	txPool map[string]*pb.Transaction
}

func newTxPool() (*transactionPool, error) {

	txpool := &transactionPool{}

	txpool.txPool = make(map[string]*pb.Transaction)

	return txpool, nil
}

func (tp *transactionPool) clearPool(txs []*pb.Transaction) {

	tp.Lock()
	defer tp.Unlock()
	for _, tx := range txs {
		delete(tp.txPool, tx.Txid)
	}
}

func (tp *transactionPool) putTransaction(txs []*pb.Transaction) error {

	err := db.GetGlobalDBHandle().PutTransactions(txs)
	if err != nil {
		return err
	}

	tp.clearPool(txs)

	return nil
}

func (tp *transactionPool) commitTransaction(txids []string) error {

	pendingTxs := make([]*pb.Transaction, 0, len(txids))

	tp.RLock()
	for _, id := range txids {
		tx, ok := tp.txPool[id]
		if ok {
			pendingTxs = append(pendingTxs, tx)
		}
	}
	tp.RUnlock()

	return tp.putTransaction(pendingTxs)
}

func (tp *transactionPool) getConfirmedTransaction(txID string) (*pb.Transaction, error) {
	return fetchTxFromDB(txID)
}

func (tp *transactionPool) getTransaction(txID string) (*pb.Transaction, error) {
	tx, err := fetchTxFromDB(txID)
	if err != nil {
		return nil, err
	}

	if tx != nil {
		return tx, nil
	}

	tp.RLock()
	defer tp.RUnlock()
	tx, ok := tp.txPool[txID]

	if !ok {
		return nil, ErrResourceNotFound
	}

	return tx, nil
}

//shared by block struct's access
func fetchTxFromDB(txID string) (*pb.Transaction, error) {
	return db.GetGlobalDBHandle().GetTransaction(txID)
}

func fetchTxsFromDB(txIDs []string) []*pb.Transaction {
	return db.GetGlobalDBHandle().GetTransactions(txIDs)
}
