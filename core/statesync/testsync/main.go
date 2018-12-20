package main

import (
	"context"
	"fmt"
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/core/statesync"
	"github.com/abchain/fabric/examples/chaincode/embedded/simple_chaincode"
	"github.com/abchain/fabric/node/legacy"
	node "github.com/abchain/fabric/node/start"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"time"
)

var logger = logging.MustGetLogger("synctest")

func main() {

	adapter := &legacynode.LegacyEngineAdapter{}

	runtest := func() error {

		err := api.RegisterECC(&api.EmbeddedChaincode{"example02",
			new(simple_chaincode.SimpleChaincode)})

		if err != nil {
			return err
		}

		if err = adapter.Init(); err != nil {
			return err
		}

		syncTarget := viper.GetString("peer.syncTarget")
		if len(syncTarget) > 0 {

			logger.Infof("Start state sync test after 10s. Sync target: %s", syncTarget)
			time.Sleep(10 * time.Second)

			syncStub := statesync.NewStateSyncStubWithPeer(nil, node.GetNode().DefaultLedger())

			err = syncStub.SyncToStateByPeer(context.TODO(), nil, nil, &pb.PeerID{syncTarget})

			if err != nil {
				return fmt.Errorf("Stop running with sync result: %s", err)
			}
		}
		return err
	}

	node.RunNode(&node.NodeConfig{PostRun: runtest, Schemes: adapter.Scheme})

}
