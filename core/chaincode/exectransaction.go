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

package chaincode

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
)

//Execute2 - like legacy execute, but not relay the global ledger object and supposed the transaction has
//be make pre-exec
func Execute2(ctxt context.Context, ledger *ledger.Ledger, chain *ChaincodeSupport, t *pb.Transaction) ([]byte, *pb.ChaincodeEvent, error) {
	cID, cMsg, cds, err := chain.Preapre(t)
	if nil != err {
		return nil, nil, err
	}

	if t.Type == pb.Transaction_CHAINCODE_DEPLOY {
		if err := chain.Deploy(ctxt, cds); err != nil {
			return nil, nil, fmt.Errorf("Failed to deploy chaincode spec(%s)", err)
		}
	}

	//will launch if necessary (and wait for ready)
	err, chrte := chain.Launch(ctxt, ledger, cID, cds, t)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to launch chaincode spec(%s)", err)
	}

	//this should work because it worked above...
	chaincode := cID.Name
	markTxBegin(ledger, t)
	resp, err := chain.Execute(ctxt, chrte, cMsg, t)
	txSuccess := false
	defer func() {
		// **** for deploy, we add final checking ****
		if t.Type == pb.Transaction_CHAINCODE_DEPLOY && chain.FinalDeploy(ctxt, txSuccess, cds, t, ledger) != nil {
			txSuccess = false
		}
		markTxFinish(ledger, t, txSuccess)
	}()

	if err != nil {
		return nil, nil, fmt.Errorf("Failed to execute transaction or query(%s)", err)
	} else if resp == nil {
		return nil, nil, fmt.Errorf("Failed to receive a response for (%s)", t.Txid)
	}

	if resp.ChaincodeEvent != nil {
		resp.ChaincodeEvent.ChaincodeID = chaincode
		resp.ChaincodeEvent.TxID = t.Txid
	}

	if resp.Type == pb.ChaincodeMessage_COMPLETED || resp.Type == pb.ChaincodeMessage_QUERY_COMPLETED {
		// Success
		txSuccess = true
		//		chaincodeLogger.Debugf("tx %s exec done: %x, %v", shorttxid(t.Txid), resp.Payload, resp.ChaincodeEvent)
		return resp.Payload, resp.ChaincodeEvent, nil
	} else if resp.Type == pb.ChaincodeMessage_ERROR || resp.Type == pb.ChaincodeMessage_QUERY_ERROR {
		// Rollback transaction
		return nil, resp.ChaincodeEvent, fmt.Errorf("Transaction or query returned with failure: %s", string(resp.Payload))
	}
	return resp.Payload, nil, fmt.Errorf("receive a response for (%s) but in invalid state(%d)", t.Txid, resp.Type)

}

//Execute - execute transaction or a query
func Execute(ctxt context.Context, chain *ChaincodeSupport, t *pb.Transaction) ([]byte, *pb.ChaincodeEvent, error) {

	// get a handle to ledger to mark the begin/finish of a tx
	ledger, ledgerErr := ledger.GetLedger()
	if ledgerErr != nil {
		return nil, nil, fmt.Errorf("Failed to get handle to ledger (%s)", ledgerErr)
	}

	if secHelper := chain.getSecHelper(); nil != secHelper {
		var err error
		t, err = secHelper.TransactionPreExecution(t)
		// Note that t is now decrypted and is a deep clone of the original input t
		if nil != err {
			return nil, nil, err
		}
	}

	return Execute2(ctxt, ledger, chain, t)
}

//ExecuteTransactions - will execute transactions on the array one by one
//will return an array of errors one for each transaction. If the execution
//succeeded, array element will be nil. returns []byte of state hash or
//error
func ExecuteTransactions(ctxt context.Context, cname ChainName, xacts []*pb.Transaction) (succeededTXs []*pb.Transaction, stateHash []byte, ccevents []*pb.ChaincodeEvent, txerrs []error, err error) {
	var chain = GetChain(cname)
	if chain == nil {
		// TODO: We should never get here, but otherwise a good reminder to better handle
		panic(fmt.Sprintf("[ExecuteTransactions]Chain %s not found\n", cname))
	}

	txerrs = make([]error, len(xacts))
	ccevents = make([]*pb.ChaincodeEvent, len(xacts))
	var succeededTxs = make([]*pb.Transaction, 0)
	for i, t := range xacts {
		_, ccevents[i], txerrs[i] = Execute(ctxt, chain, t)
		if txerrs[i] == nil {
			succeededTxs = append(succeededTxs, t)
		}
	}

	var lgr *ledger.Ledger
	lgr, err = ledger.GetLedger()
	if err == nil {
		stateHash, err = lgr.GetTempStateHash()
	}

	return succeededTxs, stateHash, ccevents, txerrs, err
}

// GetSecureContext returns the security context from the context object or error
// Security context is nil if security is off from core.yaml file
// func GetSecureContext(ctxt context.Context) (crypto.Peer, error) {
// 	var err error
// 	temp := ctxt.Value("security")
// 	if nil != temp {
// 		if secCxt, ok := temp.(crypto.Peer); ok {
// 			return secCxt, nil
// 		}
// 		err = errors.New("Failed to convert security context type")
// 	}
// 	return nil, err
// }

var errFailedToGetChainCodeSpecForTransaction = errors.New("Failed to get ChainCodeSpec from Transaction")

func getTimeout(cID *pb.ChaincodeID) (time.Duration, error) {
	ledger, err := ledger.GetLedger()
	if err == nil {
		chaincodeID := cID.Name
		txID, err := ledger.GetState(chaincodeID, "github.com_openblockchain_obc-peer_chaincode_id", true)
		if err == nil {
			tx, err := ledger.GetTransactionByID(string(txID))
			if err == nil {
				chaincodeDeploymentSpec := &pb.ChaincodeDeploymentSpec{}
				proto.Unmarshal(tx.Payload, chaincodeDeploymentSpec)
				chaincodeSpec := chaincodeDeploymentSpec.GetChaincodeSpec()
				timeout := time.Duration(time.Duration(chaincodeSpec.Timeout) * time.Millisecond)
				return timeout, nil
			}
		}
	}

	return -1, errFailedToGetChainCodeSpecForTransaction
}

func markTxBegin(ledger *ledger.Ledger, t *pb.Transaction) {
	if t.Type == pb.Transaction_CHAINCODE_QUERY {
		return
	}
	ledger.TxBegin(t.Txid)
}

func markTxFinish(ledger *ledger.Ledger, t *pb.Transaction, successful bool) {
	if t.Type == pb.Transaction_CHAINCODE_QUERY {
		return
	}
	ledger.TxFinished(t.Txid, successful)
}
