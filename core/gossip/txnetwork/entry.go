package txnetwork

import (
	"fmt"
	"github.com/abchain/fabric/core/gossip"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"sync"
)

type entryIndexs struct {
	sync.Mutex
	ind map[*pb.StreamStub]TxNetworkEntry
}

type TxNetworkEntry struct {
	*txNetworkEntry
	stub *gossip.GossipStub
}

func (e TxNetworkEntry) GetNetwork() *txNetworkGlobal {
	return global.GetNetwork(e.stub)
}

func (e TxNetworkEntry) GetEntry() TxNetwork {
	return e.txNetworkEntry
}

func (e TxNetworkEntry) UpdateLocalEpoch(series uint64, digest []byte) error {
	return UpdateLocalEpoch(e.stub, series, digest)
}

func (e TxNetworkEntry) UpdateLocalPeer() (*pb.PeerTxState, error) {
	return UpdateLocalPeer(e.stub)
}

func (e TxNetworkEntry) UpdateLocalHotTx(txs *pb.HotTransactionBlock) error {
	return UpdateLocalHotTx(e.stub, txs)
}

var entryglobal = entryIndexs{ind: make(map[*pb.StreamStub]TxNetworkEntry)}

func GetNetworkEntry(sstub *pb.StreamStub) (TxNetworkEntry, bool) {

	entryglobal.Lock()
	defer entryglobal.Unlock()
	e, ok := entryglobal.ind[sstub]
	return e, ok
}

func init() {
	gossip.RegisterCat = append(gossip.RegisterCat, initTxnetworkEntrance)
}

func initTxnetworkEntrance(stub *gossip.GossipStub) {

	entryglobal.Lock()
	defer entryglobal.Unlock()

	entryglobal.ind[stub.GetSStub()] = TxNetworkEntry{
		NewTxNetworkEntry(),
		stub,
	}
}

type TxNetworkHandler interface {
	HandleTxs(tx []*PendingTransaction)
	Release()
}

type TxNetwork interface {
	BroadCastTransaction(*pb.Transaction, string, ...string) error
	BroadCastTransactionDefault(*pb.Transaction, ...string) error
	ExecuteTransaction(context.Context, *pb.Transaction, string, ...string) *pb.Response
}

type TxNetworkUpdate interface {
	UpdateLocalEpoch(series uint64, digest []byte) error
	UpdateLocalPeer() (*pb.PeerTxState, error)
	UpdateLocalHotTx(*pb.HotTransactionBlock) error
}

type txNetworkEntry struct {
	source chan *PendingTransaction
}

const (
	maxOutputBatch = 16
)

func NewTxNetworkEntry() *txNetworkEntry {

	return &txNetworkEntry{
		source: make(chan *PendingTransaction, maxOutputBatch*3),
	}
}

func (e *txNetworkEntry) worker(ictx context.Context, h TxNetworkHandler) {

	var txBuffer [maxOutputBatch]*PendingTransaction
	txs := txBuffer[:0]
	ctx, endf := context.WithCancel(ictx)

	logger.Infof("Start a worker for txnetwork entry with handler %x", h)
	defer func() {
		h.Release()
		logger.Infof("End the worker for txnetwork entry with handler %x", h)
		endf()
	}()
	for {

		if len(txs) == 0 {
			//wait for an item
			select {
			case item := <-e.source:
				txs = append(txs, item)
			case <-ctx.Done():
				return
			}
		} else if len(txs) >= maxOutputBatch {
			h.HandleTxs(txs)
			txs = txBuffer[:0]
		} else {
			//wait more item or handle we have
			select {
			case item := <-e.source:
				txs = append(txs, item)
			case <-ctx.Done():
				return
			default:
				h.HandleTxs(txs)
				txs = txBuffer[:0]
			}
		}

	}
}

func (e *txNetworkEntry) Start(ctx context.Context, h TxNetworkHandler) {

	go e.worker(ctx, h)
}

type PendingTransaction struct {
	*pb.Transaction
	endorser string
	attrs    []string
	resp     chan *pb.Response
}

func (t *PendingTransaction) GetEndorser() string {
	if t == nil {
		return ""
	}

	return t.endorser
}

func (t *PendingTransaction) GetAttrs() []string {
	if t == nil {
		return nil
	}

	return t.attrs
}

func (t *PendingTransaction) Respond(resp *pb.Response) {
	if t != nil {
		t.resp <- resp
	}
}

func (e *txNetworkEntry) broadcast(ptx *PendingTransaction) error {
	if e == nil {
		return fmt.Errorf("txNetwork not init yet")
	}

	select {
	case e.source <- ptx:
		return nil
	default:
		return fmt.Errorf("Buffer full, can not write more")
	}
}

func (e *txNetworkEntry) BroadCastTransactionDefault(tx *pb.Transaction, attrs ...string) error {
	return e.BroadCastTransaction(tx, "", attrs...)
}

func (e *txNetworkEntry) BroadCastTransaction(tx *pb.Transaction, client string, attrs ...string) error {
	return e.broadcast(&PendingTransaction{tx, client, attrs, nil})
}

func (e *txNetworkEntry) ExecuteTransaction(ctx context.Context, tx *pb.Transaction, client string, attrs ...string) *pb.Response {

	resp := make(chan *pb.Response)
	ret := e.broadcast(&PendingTransaction{tx, client, attrs, resp})

	if ret == nil {
		select {
		case ret := <-resp:
			return ret
		case <-ctx.Done():
			return &pb.Response{pb.Response_FAILURE, []byte(fmt.Sprintf("%s", ctx.Err()))}
		}
	} else {
		return &pb.Response{pb.Response_FAILURE, []byte(fmt.Sprintf("Exec transaction fail: %s", ret))}
	}
}
