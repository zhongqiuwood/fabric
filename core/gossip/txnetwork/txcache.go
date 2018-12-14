package txnetwork

import (
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

func PeerTxQueueSoftLimit() int {
	return peerTxQueues * peerTxQueueLen * 3 / 4
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
	AddTxs(from uint64, txs []*pb.Transaction) error
	Pruning(from uint64, to uint64)
}

//the cache is supposed to be handled only by single goroutine
type txCache struct {
	*commitData
	id     string
	parent *transactionPool
}

func (c *txCache) GetCommit(series uint64, tx *pb.Transaction) uint64 {

	pos := c.commitData.pick(series)
	if *pos == 0 {

		if existed := c.parent.txIsPending(tx.GetTxid()); existed {
			//tx is still being pooled
			return 0
		}
		//or tx is commited, we need to update the commitH

		if h := c.parent.getTxCommitHeight(tx.GetTxid()); h == 0 {
			//tx can be re pooling here if it was lost before, but we should not encourage
			//this behavoir
			logger.Infof("Repool Tx {%s} [series %d] to ledger again", tx.GetTxid(), series)
			c.parent.addPendingTx([]string{tx.GetTxid()})
		} else {
			*pos = h
		}
	}

	return *pos
}

func completeTxs(txsin []*pb.Transaction, tp *transactionPool) ([]*pb.Transaction, error) {

	var err error
	for i, tx := range txsin {
		tx, err = tp.completeTx(tx)
		if err != nil {
			return txsin[:i], err
		}
	}

	return txsin, nil
}

//send tx to the target (prehandler), each tx sent successfully is also added to cache and return
//for record in txnetwork
func (c *txCache) AddTxsToTarget(from uint64, txs []*pb.Transaction, preHandler pb.TxPreHandler) ([]*pb.Transaction, error) {

	var txspos int
	added := c.commitData.append(from, len(txs))
	pooltxs := make([]string, 0, len(txs))

	defer c.parent.addPendingTx(pooltxs)

	for _, q := range added {
		for i := 0; i < len(q); i++ {
			tx := txs[txspos]

			txe := pb.NewTransactionHandlingContext(tx)
			if _, err := preHandler.Handle(txe); err != nil {
				return txs[:txspos], err
			}

			txspos++
			//commitedH, _, _ := c.parent.ledger.GetBlockNumberByTxid(tx.GetTxid())

			if commitedH == 0 {
				pooltxs = append(pooltxs, tx.GetTxid())
			}

			q[i] = commitedH

		}
	}

	return txs, nil
}

//only for test purpose
func (c *txCache) AddTxs(from uint64, txs []*pb.Transaction) error {

	var err error
	logger.Debugf("cache [%s] add %d txs from series %d", c.id, len(txs), from)
	txs, err = completeTxs(txs, c.parent)

	if err != nil {
		return err
	}

	var txspos int
	added := c.commitData.append(from, len(txs))
	pooltxs := make([]string, 0, len(txs))

	for _, q := range added {
		for i := 0; i < len(q); i++ {
			tx := txs[txspos]
			commitedH, _, _ := c.parent.ledger.GetBlockNumberByTxid(tx.GetTxid())

			if commitedH == 0 {
				pooltxs = append(pooltxs, tx.GetTxid())
			}

			q[i] = commitedH
			txspos++
		}
	}

	//sanity check
	if txspos != len(txs) {
		panic("wrong code")
	}

	c.parent.addPendingTx(pooltxs)
	return nil
}

func (c *txCache) Pruning(from uint64, to uint64) {
	c.commitData.pruning(from, to)
}
