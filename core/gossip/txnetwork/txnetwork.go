package txnetwork

import (
	"fmt"
	"github.com/abchain/fabric/core/gossip"
	"github.com/abchain/fabric/events/litekfk"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"sync"
)

type entryGlobal struct {
	sync.Mutex
	entries map[*gossip.GossipStub]*txNetworkEntry
}

var entryglobal entryGlobal

func GetNetworkEntry(stub *gossip.GossipStub) *txNetworkEntry {
	entryglobal.Lock()
	defer entryglobal.Unlock()

	return entryglobal.entries[stub]
}

func init() {

	gossip.RegisterCat = append(gossip.RegisterCat, initTxnetworkEntrance)
}

func initTxnetworkEntrance(stub *gossip.GossipStub) {

	entryglobal.Lock()
	defer entryglobal.Unlock()

	entryglobal.entries[stub] = NewTxNetworkEntry(stub.GetStubContext())
}

type TxNetworkHandler interface {
	HandleTxs(tx []*PendingTransaction)
	Release()
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

	ctx, _ := context.WithCancel(e)

	go e.worker(ctx, h)
}

type PendingTransaction struct {
	*pb.Transaction
	endorser string
	attrs    []string
}

func (e *txNetworkEntry) BroadCastTransaction(tx *pb.Transaction, client string, attrs ...string) error {

	if e == nil {
		return fmt.Errorf("txNetwork not init yet")
	}

	err := e.topic.Write(&PendingTransaction{tx, client, attrs})
	if err != nil {
		logger.Error("Write tx fail", err)
		return err
	}

	e.newTxCond.Broadcast()
	return nil
}
