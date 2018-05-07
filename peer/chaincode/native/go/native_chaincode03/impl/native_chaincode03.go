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

package native_chaincode03

//WARNING - this chaincode's ID is hard-coded in native_chaincode01 to illustrate one way of
//calling chaincode from a chaincode. If this example is modified, chaincode_example04.go has
//to be modified as well with the new ID of chaincode_example02.
//chaincode_example05 show's how chaincode ID can be passed in as a parameter instead of
//hard-coding.

import (
	"errors"
	"fmt"
	"strconv"
	"github.com/abchain/wood/fabric/core/chaincode/shim"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("native_chaincode03")

// SimpleChaincode example simple Chaincode implementation
type SimpleChaincode struct {
}

func (t *SimpleChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	var A, B string    // Entities
	var Aval, Bval int // Asset holdings
	var err error

	if len(args) != 4 {
		return nil, errors.New("Incorrect number of arguments. Expecting 4")
	}

	// Initialize the chaincode
	A = args[0]
	Aval, err = strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("Expecting integer value for asset holding")
	}
	B = args[2]
	Bval, err = strconv.Atoi(args[3])
	if err != nil {
		return nil, errors.New("Expecting integer value for asset holding")
	}
	fmt.Printf("Aval = %d, Bval = %d\n", Aval, Bval)
	logger.Infof("Aval = %d, Bval = %d\n", Aval, Bval)

	// Write the state to the ledger
	err = stub.PutState(A, []byte(strconv.Itoa(Aval)))
	if err != nil {
		return nil, err
	}

	err = stub.PutState(B, []byte(strconv.Itoa(Bval)))
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// Transaction makes payment of X units from A to B
func (t *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if function == "delete" {
		// Deletes an entity from its state
		return t.delete(stub, args)
	}

	attr, errattr := stub.ReadCertAttribute("position")
	if errattr != nil{
		fmt.Println("attr fail", errattr)
	}else{
		fmt.Println("attr position is", string(attr))
	}

	var A, B string    // Entities
	var Aval, Bval int // Asset holdings
	var X int          // Transaction value
	var err error

	if len(args) != 3 {
		return nil, errors.New("Incorrect number of arguments. Expecting 3")
	}

	A = args[0]
	B = args[1]

	// Get the state from the ledger
	// TODO: will be nice to have a GetAllState call to ledger
	Avalbytes, err := stub.GetState(A)
	if err != nil {
		return nil, errors.New("Failed to get state")
	}
	if Avalbytes == nil {
		return nil, errors.New("Entity not found")
	}
	Aval, _ = strconv.Atoi(string(Avalbytes))

	Bvalbytes, err := stub.GetState(B)
	if err != nil {
		return nil, errors.New("Failed to get state")
	}
	if Bvalbytes == nil {
		return nil, errors.New("Entity not found")
	}
	Bval, _ = strconv.Atoi(string(Bvalbytes))

	// Perform the execution
	X, err = strconv.Atoi(args[2])
	if err != nil {
		return nil, errors.New("Invalid transaction amount, expecting a integer value")
	}
	Aval = Aval - X
	Bval = Bval + X
	fmt.Printf("Aval = %d, Bval = %d\n", Aval, Bval)
	logger.Infof("Aval = %d, Bval = %d\n", Aval, Bval)


	// Write the state back to the ledger
	err = stub.PutState(A, []byte(strconv.Itoa(Aval)))
	if err != nil {
		return nil, err
	}

	err = stub.PutState(B, []byte(strconv.Itoa(Bval)))
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// Deletes an entity from state
func (t *SimpleChaincode) delete(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	A := args[0]

	// Delete the key from the state in ledger
	err := stub.DelState(A)
	if err != nil {
		return nil, errors.New("Failed to delete state")
	}

	return nil, nil
}

// Query callback representing the query of a chaincode
func (t *SimpleChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if function != "query" {
		return nil, errors.New("Invalid query function name. Expecting \"query\"")
	}
	var A string // Entities
	var err error

	attr, errattr := stub.ReadCertAttribute("position")
	if errattr != nil{
		fmt.Println("attr fail", errattr)
	}else{
		fmt.Println("attr position is", string(attr))
	}

	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting name of the person to query")
	}

	A = args[0]

	// Get the state from the ledger
	Avalbytes, err := stub.GetState(A)
	if err != nil {
		jsonResp := "{\"Error\":\"Failed to get state for " + A + "\"}"
		return nil, errors.New(jsonResp)
	}

	if Avalbytes == nil {
		jsonResp := "{\"Error\":\"Nil amount for " + A + "\"}"
		return nil, errors.New(jsonResp)
	}

	jsonResp := "{\"Name\":\"" + A + "\",\"Amount\":\"" + string(Avalbytes) + "\"}"
	fmt.Printf("Query Response:%s\n", jsonResp)
	logger.Infof("Query Response:%s\n", jsonResp)

	return Avalbytes, nil
}


type StateSyncSample struct {
}


func (t *StateSyncSample) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	var A, B string    // Entities
	var Aval, Bval int // Asset holdings
	var err error
	var b []byte

	b, err = stub.GetCallerCertificate()
	if err != nil {
		logger.Warning("Security fail", err)
	} else if b == nil {
		logger.Info("No security")
	} else {
		logger.Info("Get security", len(b))
	}

	if len(args) != 4 {
		return nil, errors.New("Incorrect number of arguments. Expecting 4")
	}

	// Initialize the chaincode
	A = args[0]
	Aval, err = strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("Expecting integer value for asset holding")
	}
	B = args[2]
	Bval, err = strconv.Atoi(args[3])
	if err != nil {
		return nil, errors.New("Expecting integer value for asset holding")
	}
	fmt.Printf("Aval = %d, Bval = %d\n", Aval, Bval)

	// Write the state to the ledger
	err = stub.PutState(A, []byte(strconv.Itoa(Aval)))
	if err != nil {
		return nil, err
	}

	err = stub.PutState(B, []byte(strconv.Itoa(Bval)))
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// Transaction makes payment of X units from A to B
func (t *StateSyncSample) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if function == "delete" {
		// Deletes an entity from its state
		return t.delete(stub, args)
	}

	attr, errattr := stub.ReadCertAttribute("position")
	if errattr != nil {
		fmt.Println("attr fail", errattr)
	} else {
		fmt.Println("attr position is", string(attr))
	}

	var A, B string    // Entities
	var Aval, Bval int // Asset holdings
	var X int          // Transaction value
	var err error

	if len(args) != 3 {
		return nil, errors.New("Incorrect number of arguments. Expecting 3")
	}

	A = args[0]
	B = args[1]

	// Get the state from the ledger
	// TODO: will be nice to have a GetAllState call to ledger
	Avalbytes, err := stub.GetState(A)
	if err != nil {
		return nil, errors.New("Failed to get state")
	}
	if Avalbytes == nil {
		return nil, errors.New("Entity not found")
	}
	Aval, _ = strconv.Atoi(string(Avalbytes))

	Bvalbytes, err := stub.GetState(B)
	if err != nil {
		return nil, errors.New("Failed to get state")
	}
	if Bvalbytes == nil {
		return nil, errors.New("Entity not found")
	}
	Bval, _ = strconv.Atoi(string(Bvalbytes))

	// Perform the execution
	X, err = strconv.Atoi(args[2])
	if err != nil {
		return nil, errors.New("Invalid transaction amount, expecting a integer value")
	}
	Aval = Aval - X
	Bval = Bval + X

	//fmt.Printf("<<<-----------Aval = %d, Bval = %d\n", Aval, Bval)

	// Write the state back to the ledger
	err = stub.PutState(A, []byte(strconv.Itoa(Aval)))
	if err != nil {
		return nil, err
	}

	err = stub.PutState(B, []byte(strconv.Itoa(Bval)))
	if err != nil {
		return nil, err
	}

	keyNum := 10

	if Bval % 100 == 1 {
		t.batchAdd(stub, "k", 0, keyNum)
		t.batchRemove(stub, "t", 0, keyNum)
	} else if Bval % 100 == 51 {
		t.batchAdd(stub, "t", 0, keyNum)
		t.batchRemove(stub, "k", 0, keyNum)
	}

	t.batchIncrease(stub, "k", 0, keyNum, 1)
	t.batchIncrease(stub, "t", 0, keyNum, 1)

	kres := t.batchDump(stub, "k", 0, keyNum)
	tres := t.batchDump(stub, "t", 0, keyNum)

	fmt.Printf("<<<--------  Aval=%d, Bval=%d, %s, %s\n", Aval, Bval, kres, tres)

	return nil, nil
}

// Deletes an entity from state
func (t *StateSyncSample) delete(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	A := args[0]

	// Delete the key from the state in ledger
	err := stub.DelState(A)
	if err != nil {
		return nil, errors.New("Failed to delete state")
	}

	return nil, nil
}

// Deletes an entity from state
func (t *StateSyncSample) batchAdd(stub shim.ChaincodeStubInterface, key string, start int, end int) {

	for i := start; i < end; i++ {
		k := key + strconv.Itoa(i)
		t.add(stub, k, 1)
	}
}

// Deletes an entity from state
func (t *StateSyncSample) batchRemove(stub shim.ChaincodeStubInterface, key string, start int, end int) {

	for i := start; i < end; i++ {
		k := key + strconv.Itoa(i)
		t.remove(stub, k)
	}
}


// Deletes an entity from state
func (t *StateSyncSample) batchIncrease(stub shim.ChaincodeStubInterface, key string, start int, end int, delta int) {

	for i := start; i < end; i++ {
		k := key + strconv.Itoa(i)
		t.increase(stub, k, delta)
	}
}

// Deletes an entity from state
func (t *StateSyncSample) batchDump(stub shim.ChaincodeStubInterface, key string, start int, end int) string{

	res := "<<<--- [" + key + "],"
	for i := start; i < end; i++ {
		k := key + strconv.Itoa(i)

		valbytes, err := stub.GetState(k)

		if err != nil {
			continue
		}

		if valbytes == nil {
			continue
		}

		res = res + k + "=" + string(valbytes) + ","
	}
	return res
}

// Deletes an entity from state
func (t *StateSyncSample) increase(stub shim.ChaincodeStubInterface, key string, delta int) {

	Avalbytes, err := stub.GetState(key)
	if err != nil {
		return
	}

	if Avalbytes == nil {
		return
	}

	val, _ := strconv.Atoi(string(Avalbytes))

	err = stub.PutState(key, []byte(strconv.Itoa(val + delta)))
	if err != nil {
		fmt.Printf("Failed to increase %s\n", key)
	} else {
		//fmt.Printf("Succeeded to increase %s=%d!\n", key, val + delta)
	}
}


// Deletes an entity from state
func (t *StateSyncSample) add(stub shim.ChaincodeStubInterface, key string, val int) {

	err := stub.PutState(key, []byte(strconv.Itoa(val)))
	if err != nil {
		fmt.Printf("Failed to add %s\n", key)
	} else {
		//fmt.Printf("Succeeded to put %s=%d!\n", key, val)
	}
}



// Deletes an entity from state
func (t *StateSyncSample) dump(stub shim.ChaincodeStubInterface, key string) {

	Avalbytes, err := stub.GetState(key)
	if err != nil {
		return
	}

	if Avalbytes == nil {
		return
	}
	val, _ := strconv.Atoi(string(Avalbytes))

	fmt.Printf("<<<--- %s: %d, ", key, val)
}

// Deletes an entity from state
func (t *StateSyncSample) remove(stub shim.ChaincodeStubInterface, key string) {

	_, err := stub.GetState(key)
	if err != nil {
		return
	}

	err = stub.DelState(key)
	if err != nil {
		fmt.Printf("Failed to delete %s\n", key)
	} else {
		//fmt.Printf("Succeeded to delete %s!\n", key)
	}
}

// Query callback representing the query of a chaincode
func (t *StateSyncSample) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if function != "query" {
		return nil, errors.New("Invalid query function name. Expecting \"query\"")
	}
	var A string // Entities
	var err error

	attr, errattr := stub.ReadCertAttribute("position")
	if errattr != nil {
		fmt.Println("attr fail", errattr)
	} else {
		fmt.Println("attr position is", string(attr))
	}

	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting name of the person to query")
	}

	A = args[0]

	// Get the state from the ledger
	Avalbytes, err := stub.GetState(A)
	if err != nil {
		jsonResp := "{\"Error\":\"Failed to get state for " + A + "\"}"
		return nil, errors.New(jsonResp)
	}

	if Avalbytes == nil {
		jsonResp := "{\"Error\":\"Nil amount for " + A + "\"}"
		return nil, errors.New(jsonResp)
	}

	jsonResp := "{\"Name\":\"" + A + "\",\"Amount\":\"" + string(Avalbytes) + "\"}"
	fmt.Printf("Query Response:%s\n", jsonResp)
	return Avalbytes, nil
}



