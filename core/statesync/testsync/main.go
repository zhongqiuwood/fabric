package main

import (
	"fmt"
	"github.com/abchain/fabric/core/statesync"
	"github.com/abchain/fabric/peerex/node"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"time"
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/examples/chaincode/go/embedded/simple_chaincode"
)

var logger = logging.MustGetLogger("synctest")


func main() {

	runtest := func() error {

		err := api.RegisterECC(&api.EmbeddedChaincode{"example02", new(simple_chaincode.SimpleChaincode)})

		if err != nil {
			return err
		}

		syncTarget := viper.GetString("peer.syncTarget")
		if len(syncTarget) > 0 {

			logger.Infof("Start state sync test after 10s. Sync target: %s", syncTarget)
			time.Sleep(10 * time.Second)

			sync, _ := statesync.GetStateSync()
			err = sync.SyncToState(nil, nil, nil, &pb.PeerID{syncTarget})

			if err != nil {
				return fmt.Errorf("Stop running with sync result: %s", err)
			}
		}
		return err
	}

	node.RunNode(&node.NodeConfig{PostRun: runtest})

}



