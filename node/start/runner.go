package startnode

import (
	"fmt"
	"github.com/abchain/fabric/core/crypto"
)

type NodeConfig struct {
	Settings map[string]interface{}
	PostRun  func() error
}

//mimic peer.main()
func RunNode(ncfg *NodeConfig) {

	cfg := new(GlobalConfig)
	cfg.LogRole = "node"
	cfg.DefaultSetting = ncfg.Settings

	if err := cfg.Apply(); err != nil {
		panic(fmt.Errorf("Init fail:", err))
	}

	// Init the crypto layer
	if err := crypto.Init(); err != nil {
		panic(fmt.Errorf("Failed to initialize the crypto layer: %s", err))
	}

	PreInitFabricNode("Default")
	if err := InitFabricNode(); err != nil {
		panic(fmt.Errorf("Failed to running node: %s", err))
	}

	if ncfg.PostRun != nil {
		if err := ncfg.PostRun(); err != nil {
			logger.Errorf("post run fail: %s, exit immediately", err)
			return
		}
	}
}
