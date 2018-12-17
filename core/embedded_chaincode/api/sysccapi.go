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

package api

import (
	"github.com/abchain/fabric/core/chaincode/shim"
)

//we devided embedded chaincode into system and embedded
var systemChaincodes = []*SystemChaincode{}

// SystemChaincode defines the metadata needed to initialize system chaincode
// when the fabric comes up. SystemChaincodes are installed by adding an
// entry in importsysccs.go
type SystemChaincode struct {
	// Enabled a convenient switch to enable/disable system chaincode without
	// having to remove entry
	Enabled bool

	// if syscc is enforced, register process throw error when this code is not
	// allowed on whitelist
	Enforced bool

	//Unique name of the system chaincode
	Name string

	//Path to the system chaincode; currently not used
	Path string

	//InitArgs initialization arguments to startup the system chaincode
	InitArgs [][]byte

	// Chaincode is the actual chaincode object
	Chaincode shim.Chaincode
}

func ListSysCC() []*SystemChaincode { return systemChaincodes }

// RegisterSysCC registers the given system chaincode with the peer
func RegisterSysCC(syscc *SystemChaincode) {

	//just silently exit
	if !syscc.Enabled {
		return
	}

	systemChaincodes = append(systemChaincodes, syscc)
}
