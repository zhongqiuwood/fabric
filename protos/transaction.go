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
)

func TxidFromDigest(digest []byte) string {

	//truncate the digest which is too long

	if len(digest) > 32 {
		digest = digest[:32]
	}

	return fmt.Sprintf("%x", digest)
}

func (t *Transaction) Digest() ([]byte, error) {
	return t.digest(util.DefaultCryptoHash())
}

func (t *Transaction) DigestWithAlg(customIDgenAlg string) ([]byte, error) {

	h := util.CryptoHashByAlg(customIDgenAlg)
	if h == nil {
		return nil, fmt.Errorf("Wrong hash algorithm was given: %s", customIDgenAlg)
	} else {
		return t.digest(h)
	}
}

// Digest generate a solid digest for transaction contents,
// It ensure a bitwise equality to txs regardless the versions or extends in future
// and the cost should be light on both memory and computation
func (t *Transaction) digest(h hash.Hash) ([]byte, error) {

	hash := util.NewHashWriter(h)

	err := hash.Write(t.ChaincodeID).Write(t.Payload).Write(t.Metadata).Write(t.Nonce).Error()

	if err != nil {
		return nil, err
	}

	//so we do not digest the nano part in ts ...
	err = bin.Write(h, bin.BigEndian, t.Timestamp.Seconds)
	if err != nil {
		return nil, err
	}

	err = bin.Write(h, bin.BigEndian, int32(t.ConfidentialityLevel))
	if err != nil {
		return nil, err
	}

	//we can add more items for tx in future
	return h.Sum(nil), nil
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

func toArgs(arguments []string) (ret [][]byte) {

	for _, arg := range arguments {
		ret = append(ret, []byte(arg))
	}

	return
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

	// Build the spec
	spec := &ChaincodeSpec{Type: ChaincodeSpec_GOLANG,
		ChaincodeID: &chaincodeID, CtorMsg: &ChaincodeInput{Args: toArgs(arguments)}}

	// Build the ChaincodeInvocationSpec message
	invocation := &ChaincodeInvocationSpec{ChaincodeSpec: spec}

	payloaddata, err := proto.Marshal(invocation)
	if err != nil {
		return nil, fmt.Errorf("Could not marshal payload for chaincode invocation: %s", err)
	}

	transaction.Payload = payloaddata

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
	transaction.Metadata = chaincodeDeploymentSpec.ChaincodeSpec.Metadata
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
	transaction.Metadata = chaincodeInvocationSpec.ChaincodeSpec.Metadata
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

/*
  YA-fabric 0.9：
  We define a struct for transaction which is passed in a pipeline
  handling it, the data can be completed progressively along the
  pipeline and finally be delivered for executing. It mainly contain
  fidentiality-releated contents currently and may add more or customed
  fields
*/
type TransactionHandlingContext struct {
	*Transaction //the original transaction
	//every fields can be readout from transaction (may covered by the confidentiality)
	ChaincodeName       string //the decrypted part of Name field in chaincodeID
	ChaincodeSpec       *ChaincodeSpec
	ChaincodeDeploySpec *ChaincodeDeploymentSpec
	//fields will be tagged from outside
	NetworkID, PeerID string
	CustomFields      map[string]interface{}
}

func NewTransactionHandlingContext(t *Transaction) *TransactionHandlingContext {
	ret := new(TransactionHandlingContext)
	ret.Transaction = t
	return ret
}

/*
   read a unencrypted tx and fill possible fields
*/
func parsePlainTx(tx *TransactionHandlingContext) (ret *TransactionHandlingContext, err error) {
	ret = tx

	switch tx.Type {
	case Transaction_CHAINCODE_DEPLOY:
		cds := &ChaincodeDeploymentSpec{}
		err = proto.Unmarshal(tx.Payload, cds)
		if err != nil {
			return
		}
		ret.ChaincodeDeploySpec = cds
		ret.ChaincodeName = cds.GetChaincodeSpec().GetChaincodeID().GetName()
	case Transaction_CHAINCODE_INVOKE, Transaction_CHAINCODE_QUERY:
		ci := &ChaincodeInvocationSpec{}
		err = proto.Unmarshal(tx.Payload, ci)
		if err != nil {
			return
		}
		ret.ChaincodeSpec = ci.GetChaincodeSpec()
		ret.ChaincodeName = ci.GetChaincodeSpec().GetChaincodeID().GetName()
	default:
		err = fmt.Errorf("invalid transaction type: %d", tx.Type)
	}

	return
}

func NewPlainTxHandlingContext(tx *Transaction) (*TransactionHandlingContext, error) {
	ret := NewTransactionHandlingContext(tx)

	return parsePlainTx(ret)
}

/*
  Also define the handling pipeline interface
*/

type TxPreHandler interface {
	Handle(*TransactionHandlingContext) (*TransactionHandlingContext, error)
}

//convert a function to a prehandler interface
type FuncAsTxPreHandler func(tx *TransactionHandlingContext) (*TransactionHandlingContext, error)

func (f FuncAsTxPreHandler) Handle(tx *TransactionHandlingContext) (*TransactionHandlingContext, error) {
	return f(tx)
}

var PlainTxHandler = FuncAsTxPreHandler(parsePlainTx)

/*
  Mutiple handler
*/

type mutiTxPreHandler []TxPreHandler

type interruptErr struct{}

func (interruptErr) Error() string {
	return "prehandling interrupted"
}

//allowing interrupt among a prehandler array and set the whold result as correct
var ValidateInterrupt = interruptErr{}

func MutipleTxHandler(m ...TxPreHandler) TxPreHandler {
	var flattedM []TxPreHandler
	//"flat" the recursive mutiple txhandler
	for _, mh := range m {
		if mmh, ok := mh.(mutiTxPreHandler); ok {
			flattedM = append(flattedM, mmh...)
		} else {
			flattedM = append(flattedM, mh)
		}
	}

	switch len(flattedM) {
	case 0:
		return nil
	case 1:
		return flattedM[0]
	default:
		return mutiTxPreHandler(flattedM)
	}
}

func (m mutiTxPreHandler) Handle(tx *TransactionHandlingContext) (*TransactionHandlingContext, error) {
	var err error
	for _, h := range m {
		tx, err = h.Handle(tx)
		if err == ValidateInterrupt {
			return tx, nil
		} else if err != nil {
			return tx, err
		}
	}
	return tx, nil
}
