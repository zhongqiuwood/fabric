package startnode

import (
	"fmt"
	"github.com/abchain/fabric/core/crypto"
	"golang.org/x/net/context"
	"os"
	"os/signal"
)

type NodeConfig struct {
	Settings map[string]interface{}
	PostRun  func() error
}

//mimic peer.main()
func RunNode(ncfg *NodeConfig) {

	cfg := new(GlobalConfig)
	cfg.LogRole = "node"
	cfg.NotUseSourceConfig = true
	cfg.DefaultSetting = ncfg.Settings

	if err := cfg.Apply(); err != nil {
		panic(fmt.Errorf("Init fail: %s", err))
	}

	// Init the crypto layer
	if err := crypto.Init(); err != nil {
		panic(fmt.Errorf("Failed to initialize the crypto layer: %s", err))
	}

	PreInitFabricNode("Default")
	if err := InitFabricNode(); err != nil {
		panic(fmt.Errorf("Failed to init node: %s", err))
	}

	if ncfg.PostRun != nil {
		if err := ncfg.PostRun(); err != nil {
			logger.Errorf("post run fail: %s, exit immediately", err)
			return
		}
	}

	if err := RunFabricNode(); err != nil {
		panic(fmt.Errorf("Fail to run node: %s", err))
	}

	defer Final()

	gctx, endf := context.WithCancel(context.Background())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {

		<-c
		endf()
		logger.Info("Get ctrl-c and exit")

	}()

	//block here
	TheGuard(gctx)
}
