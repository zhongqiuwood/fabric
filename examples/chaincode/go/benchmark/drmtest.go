/**
* Copyright 2017 HUAWEI. All Rights Reserved.
*
* SPDX-License-Identifier: Apache-2.0
*
*/

package benchmark

import (
	"encoding/json"
	"encoding/base64"
	"crypto/sha256"

	"errors"
	"fmt"
	"github.com/abchain/fabric/core/chaincode/shim"
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/peerex/node"
)

type DrmChaincode struct {

}

type PublishRequest struct {
	Author     string
	CreateTime string
	Info       string
	Item       string
}

type DigitalItem struct {
	Author     string
	CreateTime string
	Info       string
	Identity   string
}

type QueryResponse struct {
	Result     string		// "Published" or "Unknown"
	Author     string
	CreateTime string
	Info       string
}

func (t *DrmChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	// nothing to do
	return nil, nil
}

// query the drm info by item's identity
func (t *DrmChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string)  ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("Wrong format, should be 'query id'")
	}

	stat, err := stub.GetState(args[0])
	if err != nil {
		notFound := QueryResponse{"Unknown","","",""}
		r,_ := json.Marshal(notFound)
		return []byte(r), nil		// query a unpublished item , return success
	}

	var item DigitalItem
	err = json.Unmarshal(stat, &item)
	if err != nil {
		fmt.Printf("unknown state value %v \n", string(stat))
		return nil, errors.New(err.Error())
	}

	resp := QueryResponse{"Published", item.Author, item.CreateTime, item.Info}
	r,_  := json.Marshal(resp)
	return r, nil
}

// publish a digital item, store the hash and drm information into ledger
func (t * DrmChaincode) Publish(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	var wrongFmt = "Wrong format, should be 'publish {author:string, createtime:string, info:string, item:string'"
	if len(args) != 1 {
		fmt.Println(wrongFmt)
		return nil, errors.New(wrongFmt)
	}

	var req PublishRequest
	err := json.Unmarshal([]byte(args[0]), &req)
	if err != nil {
		return nil, errors.New(wrongFmt)
	}

	// calculate the hash value as identity
	hash := sha256.Sum256([]byte(req.Item))
	id   := base64.URLEncoding.EncodeToString(hash[:])
	item := DigitalItem{req.Author, req.CreateTime, req.Info, id}
	i,_  := json.Marshal(item)

	// check if item already exists
	state, err := stub.GetState(id)
	if state != nil && err == nil {
		return nil, errors.New("Item already published");
	}

	err = stub.PutState(id, []byte(i))
	if err != nil {
		fmt.Printf("PutState error, %v\n", err.Error())
		return nil, errors.New(err.Error())
	}

	return []byte(id), nil
}

// check if a digital item is already published
func (t * DrmChaincode) Check(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	var wrongFmt = "Wrong format, should be 'check item'"
	if len(args) != 1 {
		fmt.Println(wrongFmt)
		return nil, errors.New(wrongFmt)
	}

	hash  := sha256.Sum256([]byte(args[0]))
	id    := base64.URLEncoding.EncodeToString(hash[:])
	bytes,err := stub.GetState(id)
	if bytes != nil && err == nil {
		return []byte("Published"), nil
	} else{
		return []byte("Not Published"), nil
	}


}

func (t * DrmChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	if function == "query" {
		return t.Query(stub, "", args)
	}
	if function == "publish" {
		return t.Publish(stub, args)
	}
	if function == "check" {
		return t.Check(stub, args)
	}

	return nil, errors.New("Unknown action, should be 'query', 'publish' or 'check'")
}

func  main()  {

	reg := func() error {
		return api.RegisterECC(&api.EmbeddedChaincode{"mycc", new(DrmChaincode)})
	}

	node.RunNode(&node.NodeConfig{PostRun: reg})

}