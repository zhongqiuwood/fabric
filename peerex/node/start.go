package node

import (
	"fmt"
	"github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/peer/node"
	"github.com/abchain/fabric/peerex"
	"github.com/abchain/fabric/peerex/logging"
	"github.com/spf13/viper"
	"runtime"
)

type NodeConfig struct {
	Settings map[string]interface{}
	PostRun  func() error
}

var logger = logging.InitLogger("node")

//mimic peer.main()
func RunNode(cfg *NodeConfig) {

	peerConfig := &peerex.GlobalConfig{
		LogRole: "node",
	}

	err := peerConfig.InitGlobalWrapper(true, cfg.Settings)

	if err != nil {
		panic(fmt.Errorf("Init fail:", err))
	}

	runtime.GOMAXPROCS(viper.GetInt("peer.gomaxprocs"))

	// Init the crypto layer
	err = crypto.Init()
	if err != nil {
		panic(fmt.Errorf("Failed to initialize the crypto layer: %s", err))
	}

	// run serve
	err, ret := node.StartNode()
	if err == nil {

		if cfg.PostRun != nil {
			err = cfg.PostRun()
		}

		if err != nil {
			logger.Error("Post run fail, exit ...", err)
		} else {
			err = <-ret
			logger.Info("Normal exiting ....", err)
		}

		node.ClearNodeService()
	}
}
