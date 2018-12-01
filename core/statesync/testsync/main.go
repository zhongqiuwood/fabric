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
	"github.com/abchain/fabric/core/peer"
	"context"
)

var logger = logging.MustGetLogger("synctest")


func main() {

	runtest := func(peerServer interface{}) error {

		err := api.RegisterECC(&api.EmbeddedChaincode{"example02",
		new(simple_chaincode.SimpleChaincode)})

		if err != nil {
			return err
		}

		syncTarget := viper.GetString("peer.syncTarget")
		if len(syncTarget) > 0 {

			logger.Infof("Start state sync test after 10s. Sync target: %s", syncTarget)
			time.Sleep(10 * time.Second)


			v, ok := peerServer.(*peer.Peer)
			if !ok {
				return fmt.Errorf("Incorrect post run arg")
			}

			syncStub := statesync.NewStateSyncStubWithPeer(v)

			err = syncStub.SyncToStateByPeer(context.TODO(), nil, nil, &pb.PeerID{syncTarget})

			if err != nil {
				return fmt.Errorf("Stop running with sync result: %s", err)
			}
		}
		return err
	}

	node.RunNode(&node.NodeConfig{PostRun: runtest})

}



