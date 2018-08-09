package txnetwork

import (
	_ "fmt"
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
	stub      *gossip.GossipStub
	newTxCond *sync.Cond
	context.Context
}

// func (e *txNetworkEntry) worker(cli *litekfk.Client) error {
// 	defer cli.UnReg()

// 	watcher := e.topic.Watcher()

// 	rd := cli.Read(litekfk.ReadPos_Default)

// 	for {
// 		topic.Lock()
// 		if rd.CurrentEnd().Equal(watcher.GetTail()) {
// 			e.newTxCond.Wait()
// 		}
// 		topic.Unlock()
// 	}
// }

func (e *txNetworkEntry) handleTxTask(tx *pb.Transaction) error {
	return nil
}

func (e *txNetworkEntry) BroadCastTransaction(tx *pb.Transaction) error {
	return nil
}
