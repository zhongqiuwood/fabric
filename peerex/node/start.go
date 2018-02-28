package node

import (
	"fmt"
	"github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/peer/node"
	"github.com/abchain/fabric/peerex"
	"runtime"
)

//mimic peer.main()
func main() {

	peerConfig := &peerex.GlobalConfig{
		LogRole: "node",
	}

	err := peerConfig.InitGlobalWrapper(true, settings)

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

		//start embedded chaincode

		node.ClearNodeService()
	}
}
