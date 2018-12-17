package chaincode

import (
	"errors"
	"fmt"
	"github.com/abchain/fabric/core/chaincode/shim"
	"github.com/abchain/fabric/core/gossip/txnetwork"
	"github.com/abchain/fabric/core/ledger"
	logging "github.com/op/go-logging"
	"golang.org/x/net/context"
)

// SimpleChaincode example simple Chaincode implementation
type SimpleChaincode struct {
	defLogLevel logging.Level
}

func (t *SimpleChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	t.defLogLevel = logging.GetLevel("gossip_cat")
	return nil, nil
}

// Transaction makes payment of X units from A to B
func (t *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if function == "delete" {
		// Deletes an entity from its state
		return t.delete(stub, args)
	} else if function != "invoke" {
		return nil, errors.New("Invalid invoke function name. Expecting \"invoke\"")
	}

	if len(args) != 3 {
		return nil, errors.New("Incorrect number of arguments. Expecting 3")
	}

	key := args[0]
	value := args[1]

	err := stub.PutState(key, []byte(value))
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
	if function == "list" {

		l, err := ledger.GetLedger()
		if err != nil {
			return nil, fmt.Errorf("acquire ledger fail: %s", err)
		}
		i, err := l.IteratePooledTransactions(context.Background())
		if err != nil {
			return nil, fmt.Errorf("can not iterator pooled transaction: %s", err)
		}

		resp := "pool lists -----\n"
		for tx := range i {
			resp = resp + fmt.Sprintf("* %v\n", tx)
		}
		return []byte(resp), nil

	} else if function == "count" {
		l, err := ledger.GetLedger()
		if err != nil {
			return nil, fmt.Errorf("acquire ledger fail: %s", err)
		}
		return []byte(fmt.Sprintf("%d", l.GetPooledTxCount())), nil
	} else if function == "status" {

		dump := txnetwork.DumpNetwork()
		if len(dump) == 0 {
			return nil, fmt.Errorf("no available network")
		}

		dump2 := txnetwork.DumpTxNetwork()

		var resp string
		for netid, outf := range dump {
			resp = resp + fmt.Sprintf("dumping network %s ----\n", netid)
			out := outf()
			out2 := map[string]uint64{}
			out2f := dump2[netid]
			if out2f == nil {
				resp = resp + "  (No vclock found)\n"
			} else {
				out2 = out2f()
			}

			for id, s := range out {
				resp = resp + fmt.Sprintf("*     %s: [%d/%d]:%x\n", id, s.GetNum(), out2[id], s.GetDigest())
			}
			resp = resp + fmt.Sprintf("dumping network %s end ----\n", netid)
		}
		return []byte(resp), nil

	} else if function == "debug" {
		if len(args) != 1 {
			return nil, errors.New("Incorrect number of arguments. Expecting a switch")
		}
		if args[0] == "on" {
			logging.SetLevel(logging.DEBUG, "gossip_cat")
			return []byte("Gossip log is set to debug mode"), nil
		} else {
			//set log flag to default
			logging.SetLevel(t.defLogLevel, "gossip_cat")
			return []byte("Log is resume"), nil
		}

	} else if function != "query" {
		return nil, errors.New("Invalid query function name. Expecting \"query\"")
	}

	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting name of the person to query")
	}

	key := args[0]

	// Get the state from the ledger
	Avalbytes, err := stub.GetState(key)
	if err != nil {
		jsonResp := "{\"Error\":\"Failed to get state for " + key + "\"}"
		return nil, errors.New(jsonResp)
	}

	if Avalbytes == nil {
		jsonResp := "{\"Error\":\"Nil amount for " + key + "\"}"
		return nil, errors.New(jsonResp)
	}

	jsonResp := "{\"Key\":\"" + key + "\",\"Data\":\"" + string(Avalbytes) + "\"}"
	return []byte(jsonResp), nil
}
