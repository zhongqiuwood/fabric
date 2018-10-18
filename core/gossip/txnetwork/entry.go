package txnetwork

import (
	"fmt"
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"sync"
)

var entryglobal struct {
	sync.Mutex
	ind map[*pb.StreamStub]*TxNetworkEntry
}

type TxNetworkEntry struct {
	*txNetworkEntry
	*txNetworkGlobal
	stub *gossip.GossipStub
}

//Init method can be only executed before gossip network is running, to override
//the default settings
func (e *TxNetworkEntry) InitLedger(l *ledger.Ledger) {
	e.txPool.ledger = l
}

func (e *TxNetworkEntry) InitCred(v cred.TxHandlerFactory) {
	e.credvalidator = v
	e.txPool.txHandler = v
}

func (e *TxNetworkEntry) catalogHandlerUpdateLocal(catName string, u model.ScuttlebuttPeerUpdate, ug model.Update) error {
	cat := e.stub.GetCatalogHandler(catName)
	if cat == nil {
		return fmt.Errorf("Can't not found corresponding cataloghandler [%s] catalogHandler", catName)
	}

	selfUpdate := model.NewscuttlebuttUpdate(ug)
	selfUpdate.UpdateLocal(u)

	if err := cat.Model().RecvUpdate(selfUpdate); err != nil {
		return err
	} else {
		//notify our peer is updated
		cat.SelfUpdate()
		return nil
	}
}

func (e *TxNetworkEntry) UpdateLocalPeer(s *pb.PeerTxState) error {
	return e.catalogHandlerUpdateLocal(globalCatName, peerStatus{s}, nil)
}

func (e *TxNetworkEntry) UpdateLocalHotTx(txs *pb.HotTransactionBlock) error {
	return e.catalogHandlerUpdateLocal(hotTxCatName, txPeerUpdate{txs}, nil)
}

func (e *TxNetworkEntry) ResetSelfPeer(id string, state *pb.PeerTxState) error {

	if err := e.txNetworkGlobal.peers.ChangeSelf(id, state); err != nil {
		return err
	}

	e.txNetworkGlobal.handleSetSelf(id, state)
	return nil
}

func (e *TxNetworkEntry) GetPeerStatus() *pb.PeerTxState {
	return e.peers.QuerySelf()
}

func GetNetworkEntry(sstub *pb.StreamStub) (*TxNetworkEntry, bool) {

	entryglobal.Lock()
	defer entryglobal.Unlock()
	e, ok := entryglobal.ind[sstub]
	return e, ok
}

func init() {
	entryglobal.ind = make(map[*pb.StreamStub]*TxNetworkEntry)
	gossip.RegisterCat = append(gossip.RegisterCat, initTxnetworkEntrance)
}

func initTxnetworkEntrance(stub *gossip.GossipStub) {

	entryglobal.Lock()
	defer entryglobal.Unlock()

	entryglobal.ind[stub.GetSStub()] = &TxNetworkEntry{
		newTxNetworkEntry(),
		getTxNetwork(stub),
		stub,
	}
}

type TxNetworkHandler interface {
	HandleTxs(tx []*PendingTransaction)
}

type TxNetwork interface {
	BroadCastTransaction(*pb.Transaction, cred.TxEndorser) error
	ExecuteTransaction(context.Context, *pb.Transaction, cred.TxEndorser) *pb.Response
}

type txNetworkEntry struct {
	source chan *PendingTransaction
}

const (
	maxOutputBatch = 16
)

func newTxNetworkEntry() *txNetworkEntry {

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
	endorser cred.TxEndorser
	resp     chan *pb.Response
}

func (t *PendingTransaction) GetEndorser() cred.TxEndorser {
	return t.endorser
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

func (e *txNetworkEntry) BroadCastTransaction(tx *pb.Transaction, endorser cred.TxEndorser) error {
	return e.broadcast(&PendingTransaction{tx, endorser, nil})
}

func (e *txNetworkEntry) ExecuteTransaction(ctx context.Context, tx *pb.Transaction, endorser cred.TxEndorser) *pb.Response {

	resp := make(chan *pb.Response)
	ret := e.broadcast(&PendingTransaction{tx, endorser, resp})

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
