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

package benchmark

//WARNING - this chaincode's ID is hard-coded in chaincode_example04 to illustrate one way of
//calling chaincode from a chaincode. If this example is modified, chaincode_example04.go has
//to be modified as well with the new ID of chaincode_example02.
//chaincode_example05 show's how chaincode ID can be passed in as a parameter instead of
//hard-coding.

import (
	"errors"
	"fmt"
	"github.com/abchain/fabric/core/chaincode/shim"
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/peerex/node"
	"strconv"
)

const ERROR_SYSTEM = "{\"code\":300, \"reason\": \"system error: %s\"}"
const ERROR_WRONG_FORMAT = "{\"code\":301, \"reason\": \"command format is wrong\"}"
const ERROR_ACCOUNT_EXISTING = "{\"code\":302, \"reason\": \"account already exists\"}"
const ERROR_ACCOUT_ABNORMAL = "{\"code\":303, \"reason\": \"abnormal account\"}"
const ERROR_MONEY_NOT_ENOUGH = "{\"code\":304, \"reason\": \"account's money is not enough\"}"


// SimpleChaincode example simple Chaincode implementation
type SimpleChaincode struct {
}

var logger = shim.NewLogger("ecc")

func (t *SimpleChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	
	return nil, nil
}

// Transaction makes payment of X units from A to B
func (t *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	if function == "open" {
		return t.Open(stub, args)
	}
	if function == "delete" {
		return t.Delete(stub, args)
	}
	if function == "query" {
		return t.Query(stub, "", args)
	}
	if function == "transfer" {
		return t.Transfer(stub, args)
	}
	
	return nil, nil
}


// open an account, should be [open account money]
func (t *SimpleChaincode) Open(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	if len(args) != 2 {
		return nil, errors.New(ERROR_WRONG_FORMAT)

	}

	account  := args[0]
	money,err := stub.GetState(account)
	if money != nil {
		return nil, errors.New(ERROR_ACCOUNT_EXISTING)
	}

	_,err = strconv.Atoi(args[1])
	if err != nil {		
		return nil, errors.New(ERROR_WRONG_FORMAT)
	}

	err = stub.PutState(account, []byte(args[1]))
	if err != nil {
		s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
		return nil, errors.New(s)
	}

	return nil, nil
}

// delete an account, should be [delete account]
func (t *SimpleChaincode) Delete(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New(ERROR_WRONG_FORMAT)
	}

	err := stub.DelState(args[0])
	if err != nil {
		s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
		return nil, errors.New(s)
	}

	return nil, nil
}
// query current money of the account,should be [query accout]
func (t *SimpleChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New(ERROR_WRONG_FORMAT)
	}

	money, err := stub.GetState(args[0])
	if err != nil {
		s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
		return nil, errors.New(s)
	}

	if money == nil {
		return nil, errors.New(ERROR_ACCOUT_ABNORMAL)
	}

	return money, nil
}

// transfer money from account1 to account2, should be [transfer accout1 accout2 money]
func (t *SimpleChaincode) Transfer(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	if len(args) != 3 {
		return nil, errors.New(ERROR_WRONG_FORMAT)
	}
	money, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New(ERROR_WRONG_FORMAT)
	}

	moneyBytes1, err1 := stub.GetState(args[0])
	moneyBytes2, err2 := stub.GetState(args[0])
	if err1 != nil || err2 != nil {
		s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
		return nil, errors.New(s)
	}
	if moneyBytes1 == nil || moneyBytes2 == nil {
		return nil, errors.New(ERROR_ACCOUT_ABNORMAL)
	}

	money1, _ := strconv.Atoi(string(moneyBytes1))
	money2, _ := strconv.Atoi(string(moneyBytes1))
	if money1 < money {
		return nil, errors.New(ERROR_MONEY_NOT_ENOUGH)
	}

	money1 -= money
	money2 += money

	err = stub.PutState(args[0], []byte(strconv.Itoa(money1)))
	if err != nil {
		s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
		return nil, errors.New(s)
	}

	err = stub.PutState(args[1], []byte(strconv.Itoa(money2)))
	if err != nil {
		stub.PutState(args[0], []byte(strconv.Itoa(money1+money)))
		s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
		return nil, errors.New(s)
	}

	return nil, nil
}


func main() {

	reg := func() error {
		return api.RegisterECC(&api.EmbeddedChaincode{"mycc", new(SimpleChaincode)})
	}

	node.RunNode(&node.NodeConfig{PostRun: reg})

}
