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

package protos

import (
	"encoding/json"
	"fmt"
	"hash"

	bin "encoding/binary"
	"github.com/abchain/fabric/core/util"
	"github.com/golang/protobuf/proto"
	"golang.org/x/crypto/sha3"
)

type conWriter interface {
	Write([]byte) conWriter
	Error() error
}

type failConWriter struct {
	Err error
}

func (w *failConWriter) Write([]byte) conWriter {
	return w
}

func (w *failConWriter) Error() error {
	return w.Err
}

type hashConWriter struct {
	H hash.Hash
}

func (w hashConWriter) Write(p []byte) conWriter {
	i, err := w.H.Write(p)

	if err != nil {
		return &failConWriter{err}
	} else if i < len(p) {
		return &failConWriter{fmt.Errorf("Write %d for %d bytes", i, len(p))}
	} else {
		return w
	}
}

func (w hashConWriter) Error() error {
	return nil
}

//taken from core/util/utils.go
const defaultAlg = "sha3"

type alg struct {
	hashFactory func() hash.Hash
}

var availableIDgenAlgs = map[string]alg{
	defaultAlg: alg{sha3.New256},
}

func (t *Transaction) Digest() ([]byte, error) {
	return t.digest(sha3.New256())
}

func (t *Transaction) DigestWithAlg(customIDgenAlg string) ([]byte, error) {

	if customIDgenAlg == "" {
		customIDgenAlg = defaultAlg
	}
	alg, ok := availableIDgenAlgs[customIDgenAlg]
	if ok {
		return t.digest(alg.hashFactory())
	} else {
		return nil, fmt.Errorf("Wrong ID generation algorithm was given: %s", customIDgenAlg)
	}
}

// Digest generate a solid digest for transaction contents,
// It ensure a bitwise equality to txs regardless the versions or extends in future
// and the cost should be light on both memory and computation
func (t *Transaction) digest(h hash.Hash) ([]byte, error) {

	hash := hashConWriter{h}

	err := hash.Write(t.ChaincodeID).Write(t.Payload).Write(t.Metadata).Write(t.Nonce).Error()

	if err != nil {
		return nil, err
	}

	//so we do not digest the nano part in ts ...
	err = bin.Write(hash.H, bin.BigEndian, t.Timestamp.Seconds)
	if err != nil {
		return nil, err
	}

	err = bin.Write(hash.H, bin.BigEndian, int32(t.ConfidentialityLevel))
	if err != nil {
		return nil, err
	}

	//we can add more items for tx in future
	return hash.H.Sum(nil), nil
}

// Bytes returns this transaction as an array of bytes.
func (transaction *Transaction) Bytes() ([]byte, error) {
	data, err := proto.Marshal(transaction)
	if err != nil {
		logger.Errorf("Error marshalling transaction: %s", err)
		return nil, fmt.Errorf("Could not marshal transaction: %s", err)
	}
	return data, nil
}

func UnmarshallTransaction(transaction []byte) (*Transaction, error) {
	tx := &Transaction{}
	err := proto.Unmarshal(transaction, tx)
	if err != nil {
		logger.Errorf("Error unmarshalling Transaction: %s", err)
		return nil, fmt.Errorf("Could not unmarshal Transaction: %s", err)
	}
	return tx, nil
}

// NewTransaction creates a new transaction. It defines the function to call,
// the chaincodeID on which the function should be called, and the arguments
// string. The arguments could be a string of JSON, but there is no strict
// requirement.
func NewTransaction(chaincodeID ChaincodeID, uuid string, function string, arguments []string) (*Transaction, error) {
	data, err := proto.Marshal(&chaincodeID)
	if err != nil {
		return nil, fmt.Errorf("Could not marshal chaincode : %s", err)
	}
	transaction := new(Transaction)
	transaction.ChaincodeID = data
	transaction.Txid = uuid
	transaction.Timestamp = util.CreateUtcTimestamp()
	/*
		// Build the spec
		spec := &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_GOLANG,
			ChaincodeID: chaincodeID, ChaincodeInput: &pb.ChaincodeInput{Function: function, Args: arguments}}

		// Build the ChaincodeInvocationSpec message
		invocation := &pb.ChaincodeInvocationSpec{ChaincodeSpec: spec}

		data, err := proto.Marshal(invocation)
		if err != nil {
			return nil, fmt.Errorf("Could not marshal payload for chaincode invocation: %s", err)
		}
		transaction.Payload = data
	*/
	return transaction, nil
}

// NewChaincodeDeployTransaction is used to deploy chaincode.
func NewChaincodeDeployTransaction(chaincodeDeploymentSpec *ChaincodeDeploymentSpec, uuid string) (*Transaction, error) {
	transaction := new(Transaction)
	transaction.Type = Transaction_CHAINCODE_DEPLOY
	transaction.Txid = uuid
	transaction.Timestamp = util.CreateUtcTimestamp()
	cID := chaincodeDeploymentSpec.ChaincodeSpec.GetChaincodeID()
	if cID != nil {
		data, err := proto.Marshal(cID)
		if err != nil {
			return nil, fmt.Errorf("Could not marshal chaincode : %s", err)
		}
		transaction.ChaincodeID = data
	}
	//if chaincodeDeploymentSpec.ChaincodeSpec.GetCtorMsg() != nil {
	//	transaction.Function = chaincodeDeploymentSpec.ChaincodeSpec.GetCtorMsg().Function
	//	transaction.Args = chaincodeDeploymentSpec.ChaincodeSpec.GetCtorMsg().Args
	//}
	data, err := proto.Marshal(chaincodeDeploymentSpec)
	if err != nil {
		logger.Errorf("Error mashalling payload for chaincode deployment: %s", err)
		return nil, fmt.Errorf("Could not marshal payload for chaincode deployment: %s", err)
	}
	transaction.Payload = data
	return transaction, nil
}

// NewChaincodeExecute is used to invoke chaincode.
func NewChaincodeExecute(chaincodeInvocationSpec *ChaincodeInvocationSpec, uuid string, typ Transaction_Type) (*Transaction, error) {
	transaction := new(Transaction)
	transaction.Type = typ
	transaction.Txid = uuid
	transaction.Timestamp = util.CreateUtcTimestamp()
	cID := chaincodeInvocationSpec.ChaincodeSpec.GetChaincodeID()
	if cID != nil {
		data, err := proto.Marshal(cID)
		if err != nil {
			return nil, fmt.Errorf("Could not marshal chaincode : %s", err)
		}
		transaction.ChaincodeID = data
	}
	data, err := proto.Marshal(chaincodeInvocationSpec)
	if err != nil {
		return nil, fmt.Errorf("Could not marshal payload for chaincode invocation: %s", err)
	}
	transaction.Payload = data
	return transaction, nil
}

type strArgs struct {
	Function string
	Args     []string
}

// UnmarshalJSON converts the string-based REST/JSON input to
// the []byte-based current ChaincodeInput structure.
func (c *ChaincodeInput) UnmarshalJSON(b []byte) error {
	sa := &strArgs{}
	err := json.Unmarshal(b, sa)
	if err != nil {
		return err
	}
	allArgs := sa.Args
	if sa.Function != "" {
		allArgs = append([]string{sa.Function}, sa.Args...)
	}
	c.Args = util.ToChaincodeArgs(allArgs...)
	return nil
}
