package main

import (
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/examples/chaincode/embedded/simple_chaincode"
	"github.com/abchain/fabric/node/legacy"
	node "github.com/abchain/fabric/node/start"
)

func main() {

	adapter := &legacynode.LegacyEngineAdapter{}

	reg := func() error {
		if err := api.RegisterECC(&api.EmbeddedChaincode{"example02", new(simple_chaincode.SimpleChaincode)}); err != nil {
			return err
		}

		if err := adapter.Init(); err != nil {
			return err
		}

		return nil
	}

	node.RunNode(&node.NodeConfig{PostRun: reg, Schemes: adapter.Scheme})

}
