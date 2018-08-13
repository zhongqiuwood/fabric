package txnetwork

import (
	"fmt"
	"github.com/abchain/fabric/core/gossip"
	_ "github.com/abchain/fabric/core/gossip/model"
	_ "github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/events/litekfk"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"sync"
)

type txNetworkEntry struct {
	topic     litekfk.Topic
	txOut     chan *pb.Transaction
	newTxCond *sync.Cond
	onEnd     context.CancelFunc
	context.Context
}

var (
	maxOutputBatch = 20
)

func NewTxNetworkEntry(ctx context.Context, thread int) *txNetworkEntry {

	conf := litekfk.NewDefaultConfig()
	topic := litekfk.CreateTopic(conf)

	return &txNetworkEntry{
		topic:     topic,
		txOut:     make(chan *pb.Transaction, thread*maxOutputBatch),
		newTxCond: sync.NewCond(topic),
		Context:   ctx,
	}
}

func NewDefaultTxNetworkEntry(ctx context.Context) *txNetworkEntry {
	return NewTxNetworkEntry(ctx, 1)
}

func (e *txNetworkEntry) pumper(ctx context.Context, cli *litekfk.Client) {
	defer cli.UnReg()

	watcher := e.topic.Watcher()

	rd, rerr := cli.Read(litekfk.ReadPos_Default)
	if rerr != nil {
		panic(fmt.Errorf("Pumper is failed when inited: %v", rerr))
	}

	rd.AutoReset(true)

	for {

		if item, err := rd.ReadOne(); err == litekfk.ErrEOF {
			e.topic.Lock()
			if rd.CurrentEnd().Equal(watcher.GetTail()) {
				e.newTxCond.Wait()
			}
			e.topic.Unlock()

			select {
			case <-ctx.Done():
				logger.Info("Pumper for txnetwork is quit")
				return
			default:
			}
		} else if err != nil {
			//we have autoreset so we won't be drop out
			//if we still fail it should be a panic
			panic(fmt.Errorf("Pumper has unknown failure: %v", err))
		} else {
			select {
			case <-ctx.Done():
				logger.Info("Pumper for txnetwork is quit")
				return
			case e.txOut <- item.(*pb.Transaction):
			}
		}

	}
}

func (e *txNetworkEntry) Start() {

	if e.onEnd != nil {
		e.Finalize()
	}

	ctx, ef := context.WithCancel(e)
	e.onEnd = ef
	cli := e.topic.NewClient()

	go e.pumper(ctx, cli)
}

func (e *txNetworkEntry) Finalize() {

	if e.onEnd == nil {
		return
	}

	e.onEnd()
	e.newTxCond.Broadcast()
}

func (e *txNetworkEntry) BroadCastTransaction(tx *pb.Transaction) error {

	if e == nil {
		return fmt.Errorf("txNetwork not init yet")
	}

	err := e.topic.Write(tx)
	if err != nil {
		logger.Error("Write tx fail", err)
		return err
	}

	e.newTxCond.Broadcast()
	return nil
}

type txNetworkUpdater struct {
	stub *gossip.GossipStub
}

func (e *txNetworkEntry) handleTxTask(tx *pb.Transaction) error {
	return nil
}
