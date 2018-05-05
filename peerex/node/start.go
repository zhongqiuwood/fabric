package node

import (
	"fmt"
	"github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/flogging"
	"github.com/abchain/fabric/peer/node"
	"github.com/abchain/fabric/peerex"
	"github.com/spf13/viper"
	"runtime"
)

type NodeConfig struct {
	Settings map[string]interface{}
	PostRun  func() error
}

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

	flogging.LoggingInit("node")

	// run serve
	node.StartNode(cfg.PostRun)

}
