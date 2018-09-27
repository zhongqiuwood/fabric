package main

import (
	"fmt"
	"github.com/abchain/fabric/core/statesync"
	"github.com/abchain/fabric/peerex/node"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"time"
)

var logger = logging.MustGetLogger("synctest")

func main() {

	runtest := func() error {
		syncTarget := viper.GetString("peer.syncTarget")
		if len(syncTarget) == 0 {
			return fmt.Errorf("Sync test is not specified, end with doing nothing")
		}
		logger.Infof("Start state sync test after 10s. Sync target: %s", syncTarget)
		time.Sleep(10 * time.Second)

		sync, _ := statesync.GetStateSync()
		err := sync.SyncToState(nil, nil, nil, &pb.PeerID{syncTarget})
		return fmt.Errorf("Stop running with sync result: %s", err)
	}

	node.RunNode(&node.NodeConfig{PostRun: runtest})

}
