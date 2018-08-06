package txnetwork

import (
	"fmt"
	"github.com/abchain/events/litekfk"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"sync"
)

type txNetworkEntry struct {
	topic     litekfk.Topic
	stub      *gossip.GossipStub
	newTxCond *sync.Cond
	context.Context
}

func (e *txNetworkEntry) worker(cli *litekfk.Client) error {
	defer cli.UnReg()

	watcher := e.topic.Watcher()

	rd := cli.Read(litekfk.ReadPos_Default)

	for {
		topic.Lock()
		if rd.CurrentEnd().Equal(watcher.GetTail()) {
			e.newTxCond.Wait()
		}
		topic.Unlock()
	}
}

func (e *txNetworkEntry) handleTxTask(tx *pb.Transaction) error {

}

func (e *txNetworkEntry) BroadCastTransaction(tx *pb.Transaction) error {

}
