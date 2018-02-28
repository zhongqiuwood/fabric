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

package embedded_chaincode

import (
	"github.com/abchain/fabric/core/embedded_chaincode/api"
)

//we devided embedded chaincode into system and embedded

var systemChaincodes = []*api.SystemChaincode{}

// {
// 	Enabled:  true,
// 	Name:     "noop",
// 	Path:     "github.com/abchain/fabric/bddtests/syschaincode/noop",
// 	InitArgs: [][]byte{},
// 	//		Chaincode: &noop.SystemChaincode{},
// }

//RegisterSysCCs is the hook for system chaincodes where system chaincodes are registered with the fabric
//note the chaincode must still be deployed and launched like a user chaincode will be
func RegisterSysCCs() {
	for _, sysCC := range systemChaincodes {
		deploySysCC(sysCC)
	}
}
