package main

import (
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/examples/chaincode/go/embedded/simple_chaincode"
	"github.com/abchain/fabric/peerex/node"
)

func main() {

	reg := func() error {
		return api.RegisterECC(&api.EmbeddedChaincode{"example02", new(simple_chaincode.SimpleChaincode)})
	}

	node.RunNode(&node.NodeConfig{PostRun: reg})

}
