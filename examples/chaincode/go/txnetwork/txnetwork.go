package main

import (
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	cc "github.com/abchain/fabric/examples/chaincode/go/txnetwork/chaincode"
	"github.com/abchain/fabric/peerex/node"
)

var ccConf = &api.SystemChaincode{
	Enabled:   true,
	Name:      "txnetwork",
	Chaincode: new(cc.SimpleChaincode),
}

func main() {

	api.SystemChaincodes = append(api.SystemChaincodes, ccConf)
	nullf := func() error {
		return nil
	}

	node.RunNode(&node.NodeConfig{PostRun: nullf})

}