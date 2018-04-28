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
