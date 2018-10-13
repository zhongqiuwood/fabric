/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

//WARNING - this chaincode's ID is hard-coded in chaincode_example04 to illustrate one way of
//calling chaincode from a chaincode. If this example is modified, chaincode_example04.go has
//to be modified as well with the new ID of chaincode_example02.
//chaincode_example05 show's how chaincode ID can be passed in as a parameter instead of
//hard-coding.

import (
	"github.com/abchain/fabric/examples/chaincode/go/embedded/simple_chaincode"
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/peerex/node"
)


func main() {

	reg := func() error {
		return api.RegisterECC(&api.EmbeddedChaincode{"example02", new(simple_chaincode.SimpleChaincode)})
	}

	node.RunNode(&node.NodeConfig{PostRun: reg})

}
