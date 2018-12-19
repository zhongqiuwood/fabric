package txnetwork

import (
	pb "github.com/abchain/fabric/protos"
	"math"
	"sync/atomic"
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

//we use uint32 for the commit height, save space and avoid 64 bit atomic ops
type commitData [][]uint32

func dequeueLoc(pos uint64) (loc int, qpos int) {
	loc = int(uint(pos>>peerTxQueueLenBit) & peerTxMask)
	qpos = int(uint(pos) & peerTxQueueMask)
	return
}

func (c commitData) pick(pos uint64) *uint32 {

	loc, qpos := dequeueLoc(pos)
	return &c[loc][qpos]
}

func (c commitData) pruning(from uint64, to uint64) {

	locBeg := from >> peerTxQueueLenBit
	logTo := to >> peerTxQueueLenBit
	for i := locBeg; i < logTo; i++ {
		//drop the whole array
		c[int(uint(i)&peerTxMask)] = nil
	}

}

//return a dequeue of append space (it maybe overlapped if the size is exceed
//the limit, that is, peerTxQueues * peerTxQueueLen)
func (cp *commitData) append(from uint64, size int) (ret [][]uint32) {

	c := *cp
	loc, qpos := dequeueLoc(from)
	if len(c) < peerTxQueues {
		requirelen, _ := dequeueLoc(from + uint64(size))
		if loc > requirelen { //wrapping
			requirelen = peerTxQueues
		} else {
			requirelen = requirelen + 1
		}
		if len(c) < requirelen {
			c = append(c, make([][]uint32, requirelen-len(c))...)
			*cp = c
		}
	}

	if c[loc] == nil {
		c[loc] = make([]uint32, peerTxQueueLen)
	}

	for size > 0 {

		if qpos+size > peerTxQueueLen {
			ret = append(ret, c[loc][qpos:peerTxQueueLen])
			size = size - (peerTxQueueLen - qpos)
			qpos = 0
			loc = int(uint(loc+1) & peerTxMask)
			c[loc] = make([]uint32, peerTxQueueLen)
		} else {
			ret = append(ret, c[loc][qpos:qpos+size])
			size = 0
		}
	}
	return
}

type txCacheRead struct {
	commitData
	parent *transactionPool
}

func (c txCacheRead) PruneTxs(epochH uint64, txs *pb.HotTransactionBlock) {

	if c.commitData == nil {
		return
	}

	for i, tx := range txs.Transactions {
		h := c.GetCommit(txs.BeginSeries+uint64(i), tx)
		if h != 0 && (h <= uint32(epochH) || h-uint32(epochH) > (math.MaxUint32>>1)) {
			logger.Debugf("prune tx <%s> to lite (%d vs %d)", tx.GetTxid(), h, epochH)
			txs.Transactions[i] = getLiteTx(tx)
		}
	}
}

func (c txCacheRead) GetCommit(series uint64, tx *pb.Transaction) uint32 {

	pos := c.pick(series)
	if h := atomic.LoadUint32(pos); h == 0 {

		h = uint32(c.parent.confirmCommit(tx.GetTxid()))
		atomic.StoreUint32(pos, h)
		//logger.Debugf("test and confirm commit h of tx <%s> is %d", tx.GetTxid(), h)
		return h
	} else {
		//logger.Debugf("test commit h of tx <%s> is %d", tx.GetTxid(), h)
		return h
	}

}

type TxCache interface {
	GetCommit(series uint64, tx *pb.Transaction) uint32
	AddTxs(from uint64, txs []*pb.Transaction) error
	Pruning(from uint64, to uint64)
}

//the cache is supposed to be handled only by single goroutine
type txCache struct {
	txCacheRead
	id          string
	txHeightRet <-chan uint64
}

//send tx to the target (prehandler), each tx sent successfully is also added to cache and return
//for record in txnetwork
func (c *txCache) AddTxsToTarget(from uint64, txs []*pb.Transaction, preHandler pb.TxPreHandler) ([]*pb.Transaction, error) {

	var txspos int
	var err error
	added := c.append(from, len(txs))
	logger.Debugf("Append cache space from %d for %d txs", from, len(txs))

	dataPipe := make(chan *pb.TransactionHandlingContext)
	dataRet := make(chan error)
	defer func() {
		close(dataPipe)
		if txspos > 0 {
			c.parent.setCommitData(c.id, c.commitData)
		}
	}()

	go func(in <-chan *pb.TransactionHandlingContext, out chan<- error) {
		for txe := range in {
			_, err := preHandler.Handle(txe)
			out <- err
		}
	}(dataPipe, dataRet)

	for _, q := range added {
		for i := 0; i < len(q); i++ {
			tx := txs[txspos]

			txe := pb.NewTransactionHandlingContext(tx)
			dataPipe <- txe

			var commitedH uint64
			select {
			case err = <-dataRet:
				select {
				case commitedH = <-c.txHeightRet:
				default:
					if err == nil {
						panic("No commitH filter is included in the prehandler, wrong code")
					}
				}
			case commitedH = <-c.txHeightRet:
				err = <-dataRet
			}

			if err != nil {
				return txs[:txspos], err
			}

			txs[txspos] = txe.Transaction
			logger.Debugf("Tx {%s} [nonce %x, commitH %d] is added to cache", tx.GetTxid(), tx.GetNonce(), commitedH)

			q[i] = uint32(commitedH)
			if commitedH == 0 {
				c.parent.addPendingTx(tx.GetTxid())
			}
			txspos++
		}
	}

	//sanity check
	if txspos != len(txs) {
		panic("wrong code")
	}

	return txs, nil
}

//only for test purpose
func (c *txCache) AddTxs(from uint64, txs []*pb.Transaction) error {

	heightChan := make(chan uint64)
	c.txHeightRet = heightChan

	_, err := c.AddTxsToTarget(from, txs,
		pb.MutipleTxHandler(c.parent.buildCompleteTxHandler(), c.parent.buildGetCommitHandler(heightChan)))
	return err
}

func (c *txCache) Pruning(from uint64, to uint64) {
	c.commitData.pruning(from, to)
}
