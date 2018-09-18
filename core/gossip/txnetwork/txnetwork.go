package txnetwork

import (
	"fmt"
	"github.com/abchain/fabric/core/gossip"
	"github.com/abchain/fabric/events/litekfk"
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
		NewTxNetworkEntry(stub.GetStubContext()),
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
	topic     litekfk.Topic
	newTxCond *sync.Cond
	context.Context
	TxNetworkHandler
}

const (
	maxOutputBatch = 16
)

func NewTxNetworkEntry(ctx context.Context) *txNetworkEntry {

	conf := litekfk.NewDefaultConfig()
	topic := litekfk.CreateTopic(conf)

	return &txNetworkEntry{
		topic:     topic,
		newTxCond: sync.NewCond(topic),
		Context:   ctx,
	}
}

func (e *txNetworkEntry) waitForTx(ctx context.Context, cond func() bool) {

	e.topic.Lock()
	defer e.topic.Unlock()

	select {
	case <-ctx.Done():
		logger.Info("Worker for txnetwork is quit")
		return
	default:
	}

	if cond() {
		e.newTxCond.Wait()
	}
}

func (e *txNetworkEntry) worker(ctx context.Context, h TxNetworkHandler) {
	cli := e.topic.NewClient()
	defer cli.UnReg()

	watcher := e.topic.Watcher()

	rd, rerr := cli.Read(litekfk.ReadPos_Default)
	if rerr != nil {
		panic(fmt.Errorf("Worker is failed when inited: %v", rerr))
	}

	rd.AutoReset(true)

	gctx, endf := context.WithCancel(ctx)

	//start the guard
	go func() {
		<-ctx.Done()
		logger.Info("Guard will make worker for txnetwork quitting")

		e.topic.Lock()
		endf()
		e.topic.Unlock()

		e.newTxCond.Broadcast()

	}()

	var txBuffer [maxOutputBatch]*PendingTransaction

	txs := txBuffer[:0]
	waitCond := func() bool {
		return rd.CurrentEnd().Equal(watcher.GetTail())
	}

	for {

		if item, err := rd.ReadOne(); err == litekfk.ErrEOF {

			if len(txs) > 0 {
				h.HandleTxs(txs)
				txs = txBuffer[:0]
			} else {

				e.waitForTx(gctx, waitCond)
			}

		} else if err != nil {
			//we have autoreset so we won't be drop out
			//if we still fail it should be a panic
			panic(fmt.Errorf("Worker has unknown failure: %v", err))
		} else {
			txs = append(txs, item.(*PendingTransaction))
			if len(txs) >= maxOutputBatch {
				h.HandleTxs(txs)
				txs = txBuffer[:0]
			}
		}

		select {
		case <-gctx.Done():
			logger.Info("Worker for txnetwork is quit")
			return
		default:
		}

	}
}

func (e *txNetworkEntry) Start(h TxNetworkHandler) {

	go e.worker(e, h)
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

	err := e.topic.Write(ptx)
	if err != nil {
		logger.Error("Write tx fail", err)
		return err
	}

	e.newTxCond.Broadcast()
	return nil
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
