package txnetwork

import (
	"fmt"
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
)

type TxNetworkEntry struct {
	*txNetworkEntry
	net  *txNetworkGlobal
	stub *gossip.GossipStub
}

//Init method can be only executed before gossip network is running, to override
//the default settings
func (e *TxNetworkEntry) InitLedger(l *ledger.Ledger) {
	e.net.txPool.ledger = l
}

func (e *TxNetworkEntry) InitCred(v cred.TxHandlerFactory) {
	e.net.peers.peerHandler = v
	e.net.txPool.txHandler = v
}

func (e *TxNetworkEntry) ResetPeerSimple(id []byte) error {

	if err := e.net.peers.ChangeSelf(id); err != nil {
		return err
	}

	selfState, selfId := e.net.peers.QuerySelf()
	e.net.handleSetSelf(selfId, selfState)
	//add a mark to indicate the peer is endorsered
	selfState.Endorsement = []byte{1}
	e.catalogHandlerUpdateLocal(globalCatName, peerStatus{selfState}, nil)

	return nil
}

func (e *TxNetworkEntry) ResetPeer(endorser cred.TxEndorserFactory) error {

	id := endorser.EndorserId()
	err := e.net.peers.ChangeSelf(id)

	var selfState *pb.PeerTxState
	if err == nil {
		//self id is changed

		//we have a inconsistent status here (the self is updated in global network
		//while is not yet in each scuttlebutt model), but this should be all right
		//as long as the model is not updated self status, which is controllable
		//by the caller of SetSelf

		var selfId string
		//obtain self state after chaning
		selfState, selfId = e.net.peers.QuerySelf()

		//sanity check
		if selfState == nil {
			panic("state must be obtained succefully after a calling of change self")
		}
		//we also update scuttlebutt model before the state is endorsed, now we have
		//a new self-peer, but not endorsed so do not propagate it on the network
		e.net.handleSetSelf(selfId, selfState)

	} else if err != SelfIDNotChange {
		return err
	} else {
		selfState, _ = e.net.peers.QuerySelf()
	}

	//give a new endorsement for the selfState, so it was ready for broadcasting
	selfState, err = endorser.EndorsePeerState(selfState)
	if err != nil {
		return err
	}

	//everything is done, now disclose our new peer to the whole network
	e.catalogHandlerUpdateLocal(globalCatName, peerStatus{selfState}, nil)

	return nil
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

func (e *TxNetworkEntry) GetPeerStatus() (*pb.PeerTxState, string) {
	return e.net.peers.QuerySelf()
}

func GetNetworkEntry(stub *gossip.GossipStub) *TxNetworkEntry {

	global.Lock()
	defer global.Unlock()
	e, ok := global.entries[stub]
	if !ok {
		panic("It was impossible that entry is not existed: use a gossipstub not created by stub?")
	}
	return e
}

func init() {
	stub.RegisterCat = append(stub.RegisterCat, initTxnetworkEntrance)
}

func initTxnetworkEntrance(stub *gossip.GossipStub) {

	self := &TxNetworkEntry{
		newTxNetworkEntry(),
		getTxNetwork(stub),
		stub,
	}

	global.Lock()
	defer global.Unlock()
	global.entries[stub] = self
}

type TxNetworkHandler interface {
	HandleTxs(tx []*PendingTransaction) error
	Release()
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

func (e *txNetworkEntry) worker(ctx context.Context, h TxNetworkHandler) {

	var txBuffer [maxOutputBatch]*PendingTransaction
	var err error
	txs := txBuffer[:0]

	logger.Infof("Start a worker for txnetwork entry with handler %x", h)
	defer func() {
		logger.Infof("End the worker for txnetwork entry with handler %x", h)
		h.Release()
	}()
	for {

		if err != nil {
			logger.Error("Tx handling encounter failure:", err)
			return
		}

		if len(txs) == 0 {
			//wait for an item
			select {
			case item := <-e.source:
				txs = append(txs, item)
			case <-ctx.Done():
				return
			}
		} else if len(txs) >= maxOutputBatch {
			err = h.HandleTxs(txs)
			txs = txBuffer[:0]
		} else {
			//wait more item or handle we have
			select {
			case item := <-e.source:
				txs = append(txs, item)
			case <-ctx.Done():
				return
			default:
				err = h.HandleTxs(txs)
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
	if t.resp != nil {
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
