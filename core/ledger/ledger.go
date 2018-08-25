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

package ledger

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"

	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/core/ledger/statemgmt/state"
	"github.com/abchain/fabric/events/producer"
	"github.com/golang/protobuf/proto"
	"github.com/op/go-logging"

	"github.com/abchain/fabric/flogging"
	"github.com/abchain/fabric/protos"
)

var ledgerLogger = logging.MustGetLogger("ledger")
var printGID = flogging.GoRDef

//ErrorType represents the type of a ledger error
type ErrorType string

const (
	//ErrorTypeInvalidArgument used to indicate the invalid input to ledger method
	ErrorTypeInvalidArgument = ErrorType("InvalidArgument")
	//ErrorTypeOutOfBounds used to indicate that a request is out of bounds
	ErrorTypeOutOfBounds = ErrorType("OutOfBounds")
	//ErrorTypeResourceNotFound used to indicate if a resource is not found
	ErrorTypeResourceNotFound = ErrorType("ResourceNotFound")
	//ErrorTypeBlockNotFound used to indicate if a block is not found when looked up by it's hash
	ErrorTypeBlockNotFound = ErrorType("ErrorTypeBlockNotFound")
)

//Error can be used for throwing an error from ledger code.
type Error struct {
	errType ErrorType
	msg     string
}

func (ledgerError *Error) Error() string {
	return fmt.Sprintf("LedgerError - %s: %s", ledgerError.errType, ledgerError.msg)
}

//Type returns the type of the error
func (ledgerError *Error) Type() ErrorType {
	return ledgerError.errType
}

func newLedgerError(errType ErrorType, msg string) *Error {
	return &Error{errType, msg}
}

var (
	// ErrOutOfBounds is returned if a request is out of bounds
	ErrOutOfBounds = newLedgerError(ErrorTypeOutOfBounds, "ledger: out of bounds")

	// ErrResourceNotFound is returned if a resource is not found
	ErrResourceNotFound = newLedgerError(ErrorTypeResourceNotFound, "ledger: resource not found")
)

// Ledger - the struct for openchain ledger
type Ledger struct {
	blockchain       *blockchain
	txpool           *transactionPool
	state            *state.State
	currentID        interface{}
	currentStateHash []byte
	snapshotMap      *dbsnapshotMap
}

var ledger *Ledger
var ledgerError error
var once sync.Once

// GetLedger - gives a reference to a 'singleton' ledger
func GetLedger() (*Ledger, error) {
	once.Do(func() {
		//check version before obtain ledger ...
		ledgerError = UpgradeLedger(true)
		if ledgerError == nil {
			ledger, ledgerError = GetNewLedger(db.GetDBHandle())
		}
	})
	return ledger, ledgerError
}

// GetNewLedger - gives a reference to a new ledger TODO need better approach
func GetNewLedger(db *db.OpenchainDB) (*Ledger, error) {

	blockchain, err := newBlockchain(db)
	if err != nil {
		return nil, err
	}

	txpool, err := newTxPool()
	if err != nil {
		return nil, err
	}

	state := state.NewState(db)
	snapshotMap := newSnapshotMap()

	err = sanityCheck(db)
	if err != nil {
		return nil, err
	}

	return &Ledger{blockchain, txpool, state,
		nil, nil, snapshotMap}, nil
}

func sanityCheck(odb *db.OpenchainDB) error {

	size, err := fetchBlockchainSizeFromDB(odb)
	if err != nil {
		return fmt.Errorf("Fetch size fail: %s", err)
	}

	if size == 0 {
		return nil
	}

	noneExistingList := make([][]byte, 0)
	var lastExisting []byte

	for n := size - 1; n != 0; n-- {

		block, err := fetchRawBlockFromDB(odb, n)
		if err != nil {
			return fmt.Errorf("Fetch block fail: %s", err)
		}
		if block == nil {
			return fmt.Errorf("Block %d is not exist yet", n)
		}

		gs := db.GetGlobalDBHandle().GetGlobalState(block.StateHash)
		if gs != nil {
			lastExisting = block.StateHash
			ledgerLogger.Infof("Block number [%d] state exists in txdb, statehash: [%x]",
				n, block.StateHash)
			break
		} else {
			ledgerLogger.Warningf("Block number [%d] state does not exist in txdb, statehash: [%x]",
				n, block.StateHash)
			noneExistingList = append(noneExistingList, block.StateHash)
		}
	}

	//so we have all blocks missed in global state (except for the gensis)
	//so we just start from a new gensis state
	if lastExisting == nil {
		block, err := fetchRawBlockFromDB(odb, 0)
		if err != nil {
			return fmt.Errorf("Fetch gensis block fail: %s", err)
		}
		err = db.GetGlobalDBHandle().PutGenesisGlobalState(block.StateHash)
		if err != nil {
			return fmt.Errorf("Put genesis state fail: %s", err)
		}
	}

	for len(noneExistingList) > 0 {

		pos := len(noneExistingList) - 1
		stateHash := noneExistingList[pos]
		err = db.GetGlobalDBHandle().AddGlobalState(lastExisting, stateHash)
		if err != nil {
			return fmt.Errorf("Put global state fail: %s", err)
		}
		lastExisting = stateHash
		noneExistingList = noneExistingList[:pos]
		ledgerLogger.Infof("Sanity check add Missed GlobalState [%x]:", stateHash)
	}

	return nil
}

func (ledger *Ledger) AddGlobalState(parent []byte, state []byte) error {

	s := db.GetGlobalDBHandle().GetGlobalState(parent)

	if s == nil {
		ledgerLogger.Warningf("Try to add state in unexist global state [%x], we execute a sanity check",
			parent)

		//sanityCheck should be thread safe
		err := sanityCheck(ledger.blockchain.OpenchainDB)
		if err != nil {
			ledgerLogger.Errorf("Sanity check fail in add globalstate: %s", err)
			return err
		}
	}

	err := db.GetGlobalDBHandle().AddGlobalState(parent, state)

	if err != nil {
		//should this the correct way to omit StateDuplicatedError?
		if _, ok := err.(db.StateDuplicatedError); !ok {
			ledgerLogger.Errorf("Add globalstate fail: %s", err)
			return err
		}

		ledgerLogger.Warningf("Try to add existed globalstate: %x", state)
	}

	ledgerLogger.Infof("Add globalstate [%x]", state)
	ledgerLogger.Infof("      on parent [%x]", parent)
	return nil
}

func (ledger *Ledger) GetBlockByNumberBySnapshot(syncid string, blockNumber uint64) (*protos.Block, error) {
	return ledger.snapshotMap.GetBlockByNumber(syncid, blockNumber)
}

func (ledger *Ledger) GetBlockchainSizeBySnapshot(syncid string) (uint64, error) {
	return ledger.snapshotMap.GetBlockchainSize(syncid)
}

func (ledger *Ledger) ReleaseSnapshot(syncid string) {
	ledger.snapshotMap.ReleaseSnapshot(syncid)
}

func (ledger *Ledger) GetStateDeltaBySnapshot(syncid string, blockNumber uint64) (*statemgmt.StateDelta, error) {

	size, err := ledger.snapshotMap.GetBlockchainSize(syncid)

	if err != nil {
		return nil, err
	}

	if blockNumber >= size {
		return nil, ErrOutOfBounds
	}

	return ledger.snapshotMap.GetStateDelta(syncid, blockNumber)
}

/////////////////// Transaction-batch related methods ///////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

// BeginTxBatch - gets invoked when next round of transaction-batch execution begins
func (ledger *Ledger) BeginTxBatch(id interface{}) error {
	err := ledger.checkValidIDBegin()
	if err != nil {
		return err
	}

	ledger.currentStateHash, err = ledger.state.GetHash()
	if err != nil {
		return err
	}

	ledger.currentID = id
	return nil
}

// GetTXBatchPreviewBlockInfo returns a preview block info that will
// contain the same information as GetBlockchainInfo will return after
// ledger.CommitTxBatch is called with the same parameters. If the
// state is modified by a transaction between these two calls, the
// contained hash will be different.
func (ledger *Ledger) GetTXBatchPreviewBlockInfo(id interface{},
	transactions []*protos.Transaction, metadata []byte) (*protos.BlockchainInfo, error) {
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return nil, err
	}
	stateHash, err := ledger.state.GetHash()
	if err != nil {
		return nil, err
	}
	block := ledger.blockchain.buildBlock(protos.NewBlock(transactions, metadata), stateHash)
	info := ledger.blockchain.getBlockchainInfoForBlock(ledger.blockchain.getSize()+1, block)
	return info, nil
}

// CommitTxBatch - gets invoked when the current transaction-batch needs to be committed
// This function returns successfully iff the transactions details and state changes (that
// may have happened during execution of this transaction-batch) have been committed to permanent storage
func (ledger *Ledger) CommitTxBatch(id interface{}, transactions []*protos.Transaction, transactionResults []*protos.TransactionResult, metadata []byte) error {
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return err
	}

	stateHash, err := ledger.state.GetHash()
	commitDone := false
	defer func() {
		if !commitDone {
			ledger.resetForNextTxGroup(false)
			ledger.blockchain.blockPersistenceStatus(false)
		}
	}()

	if err != nil {
		return err
	}

	//commit transactions first
	err = ledger.PutTransactions(transactions)
	if err != nil {
		return err
	}

	writeBatch := ledger.blockchain.NewWriteBatch()
	defer writeBatch.Destroy()
	block := protos.NewBlock(transactions, metadata)

	ccEvents := []*protos.ChaincodeEvent{}

	if transactionResults != nil {
		ccEvents = make([]*protos.ChaincodeEvent, len(transactionResults))
		for i := 0; i < len(transactionResults); i++ {
			if transactionResults[i].ChaincodeEvent != nil {
				ccEvents[i] = transactionResults[i].ChaincodeEvent
			} else {
				//We need the index so we can map the chaincode
				//event to the transaction that generated it.
				//Hence need an entry for cc event even if one
				//wasn't generated for the transaction. We cannot
				//use a nil cc event as protobuf does not like
				//elements of a repeated array to be nil.
				//
				//We should discard empty events without chaincode
				//ID when sending out events.
				ccEvents[i] = &protos.ChaincodeEvent{}
			}
		}
	}

	//store chaincode events directly in NonHashData. This will likely change in New Consensus where we can move them to Transaction
	block.NonHashData = &protos.NonHashData{ChaincodeEvents: ccEvents}
	block = ledger.blockchain.buildBlock(block, stateHash)

	newBlockNumber := ledger.blockchain.getSize()
	err = ledger.blockchain.addPersistenceChangesForNewBlock(block, newBlockNumber, writeBatch)
	if err != nil {
		return err
	}
	ledger.state.AddChangesForPersistence(newBlockNumber, writeBatch)

	//all commit done, we can add global state
	dbErr := ledger.AddGlobalState(ledger.currentStateHash, stateHash)

	if dbErr != nil {
		ledgerLogger.Error("CommitTx fail at add global state phase:", dbErr)
		return dbErr
	}

	dbErr = writeBatch.BatchCommit()

	if dbErr != nil {
		return dbErr
	}

	commitDone = true
	ledger.resetForNextTxGroup(true)
	ledger.blockchain.blockPersistenceStatus(true)

	sendProducerBlockEvent(block)

	//send chaincode events from transaction results
	sendChaincodeEvents(transactionResults)

	if len(transactionResults) != 0 {
		ledgerLogger.Debug("There were some erroneous transactions. We need to send a 'TX rejected' message here.")
	}

	ledgerLogger.Debugf("Committed: last block number: %d, blockchain size(heigth): %d",
		newBlockNumber,
		ledger.GetBlockchainSize())

	return nil
}

// RollbackTxBatch - Discards all the state changes that may have taken place during the execution of
// current transaction-batch
func (ledger *Ledger) RollbackTxBatch(id interface{}) error {
	ledgerLogger.Debugf("RollbackTxBatch for id = [%s]", id)
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return err
	}
	ledger.resetForNextTxGroup(false)
	return nil
}

// TxBegin - Marks the begin of a new transaction in the ongoing batch
func (ledger *Ledger) TxBegin(txID string) {
	ledger.state.TxBegin(txID)
}

// TxFinished - Marks the finish of the on-going transaction.
// If txSuccessful is false, the state changes made by the transaction are discarded
func (ledger *Ledger) TxFinished(txID string, txSuccessful bool) {
	ledger.state.TxFinish(txID, txSuccessful)
}

/////////////////// world-state related methods /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

// GetTempStateHash - Computes state hash by taking into account the state changes that may have taken
// place during the execution of current transaction-batch
func (ledger *Ledger) GetTempStateHash() ([]byte, error) {
	return ledger.state.GetHash()
}

// GetCurrentStateHash returns the current non-committed hash of the in memory state
func (ledger *Ledger) GetCurrentStateHash() (stateHash []byte, err error) {
	return ledger.GetTempStateHash()
}

// GetTempStateHashWithTxDeltaStateHashes - In addition to the state hash (as defined in method GetTempStateHash),
// this method returns a map [txUuid of Tx --> cryptoHash(stateChangesMadeByTx)]
// Only successful txs appear in this map
func (ledger *Ledger) GetTempStateHashWithTxDeltaStateHashes() ([]byte, map[string][]byte, error) {
	stateHash, err := ledger.state.GetHash()
	return stateHash, ledger.state.GetTxStateDeltaHash(), err
}

// GetState get state for chaincodeID and key. If committed is false, this first looks in memory
// and if missing, pulls from db.  If committed is true, this pulls from the db only.
func (ledger *Ledger) GetState(chaincodeID string, key string, committed bool) ([]byte, error) {
	return ledger.state.Get(chaincodeID, key, committed)
}

// GetStateRangeScanIterator returns an iterator to get all the keys (and values) between startKey and endKey
// (assuming lexical order of the keys) for a chaincodeID.
// If committed is true, the key-values are retrieved only from the db. If committed is false, the results from db
// are mergerd with the results in memory (giving preference to in-memory data)
// The key-values in the returned iterator are not guaranteed to be in any specific order
func (ledger *Ledger) GetStateRangeScanIterator(chaincodeID string, startKey string, endKey string, committed bool) (statemgmt.RangeScanIterator, error) {
	return ledger.state.GetRangeScanIterator(chaincodeID, startKey, endKey, committed)
}

// SetState sets state to given value for chaincodeID and key. Does not immideatly writes to DB
func (ledger *Ledger) SetState(chaincodeID string, key string, value []byte) error {
	if key == "" || value == nil {
		return newLedgerError(ErrorTypeInvalidArgument,
			fmt.Sprintf("An empty string key or a nil value is not supported. Method invoked with key='%s', value='%#v'", key, value))
	}
	return ledger.state.Set(chaincodeID, key, value)
}

// DeleteState tracks the deletion of state for chaincodeID and key. Does not immediately writes to DB
func (ledger *Ledger) DeleteState(chaincodeID string, key string) error {
	return ledger.state.Delete(chaincodeID, key)
}

// CopyState copies all the key-values from sourceChaincodeID to destChaincodeID
func (ledger *Ledger) CopyState(sourceChaincodeID string, destChaincodeID string) error {
	return ledger.state.CopyState(sourceChaincodeID, destChaincodeID)
}

// GetStateMultipleKeys returns the values for the multiple keys.
// This method is mainly to amortize the cost of grpc communication between chaincode shim peer
func (ledger *Ledger) GetStateMultipleKeys(chaincodeID string, keys []string, committed bool) ([][]byte, error) {
	return ledger.state.GetMultipleKeys(chaincodeID, keys, committed)
}

// SetStateMultipleKeys sets the values for the multiple keys.
// This method is mainly to amortize the cost of grpc communication between chaincode shim peer
func (ledger *Ledger) SetStateMultipleKeys(chaincodeID string, kvs map[string][]byte) error {
	return ledger.state.SetMultipleKeys(chaincodeID, kvs)
}

// GetStateSnapshot returns a point-in-time view of the global state for the current block. This
// should be used when transferring the state from one peer to another peer. You must call
// stateSnapshot.Release() once you are done with the snapshot to free up resources.
func (ledger *Ledger) GetStateSnapshot() (*state.StateSnapshot, error) {
	dbSnapshot := ledger.blockchain.GetSnapshot()
	blockHeight, err := fetchBlockchainSizeFromSnapshot(dbSnapshot)
	if err != nil {
		dbSnapshot.Release()
		return nil, err
	}
	if 0 == blockHeight {
		dbSnapshot.Release()
		return nil, fmt.Errorf("Blockchain has no blocks, cannot determine block number")
	}
	return ledger.state.GetSnapshot(blockHeight-1, dbSnapshot)
}

// GetStateDelta will return the state delta for the specified block if
// available.  If not available because it has been discarded, returns nil,nil.
func (ledger *Ledger) GetStateDelta(blockNumber uint64) (*statemgmt.StateDelta, error) {
	if blockNumber >= ledger.GetBlockchainSize() {
		return nil, ErrOutOfBounds
	}
	return ledger.state.FetchStateDeltaFromDB(blockNumber)
}

func (ledger *Ledger) GetGlobalState(statehash []byte) *protos.GlobalState {
	return db.GetGlobalDBHandle().GetGlobalState(statehash)
}

// ApplyStateDelta applies a state delta to the current state. This is an
// in memory change only. You must call ledger.CommitStateDelta to persist
// the change to the DB.
// This should only be used as part of state synchronization. State deltas
// can be retrieved from another peer though the Ledger.GetStateDelta function
// or by creating state deltas with keys retrieved from
// Ledger.GetStateSnapshot(). For an example, see TestSetRawState in
// ledger_test.go
// Note that there is no order checking in this function and it is up to
// the caller to ensure that deltas are applied in the correct order.
// For example, if you are currently at block 8 and call this function
// with a delta retrieved from Ledger.GetStateDelta(10), you would now
// be in a bad state because you did not apply the delta for block 9.
// It's possible to roll the state forwards or backwards using
// stateDelta.RollBackwards. By default, a delta retrieved for block 3 can
// be used to roll forwards from state at block 2 to state at block 3. If
// stateDelta.RollBackwards=false, the delta retrieved for block 3 can be
// used to roll backwards from the state at block 3 to the state at block 2.
func (ledger *Ledger) ApplyStateDelta(id interface{}, delta *statemgmt.StateDelta) error {
	err := ledger.checkValidIDBegin()
	if err != nil {
		return err
	}
	ledger.currentID = id
	ledger.state.ApplyStateDelta(delta)
	return nil
}

// CommitStateDelta will commit the state delta passed to ledger.ApplyStateDelta
// to the DB
func (ledger *Ledger) CommitStateDelta(id interface{}) error {
	return ledger.CommitAndIndexStateDelta(id, 0)
}

func (ledger *Ledger) CommitAndIndexStateDelta(id interface{}, blockNumber uint64) error {
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return err
	}
	defer ledger.resetForNextTxGroup(true)
	return ledger.state.CommitStateDelta(blockNumber)
}

// RollbackStateDelta will discard the state delta passed
// to ledger.ApplyStateDelta
func (ledger *Ledger) RollbackStateDelta(id interface{}) error {
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return err
	}
	ledger.resetForNextTxGroup(false)
	return nil
}

// alias of DeleteALLStateKeysAndValues
func (ledger *Ledger) EmptyState() error {
	return ledger.DeleteALLStateKeysAndValues()
}

// DeleteALLStateKeysAndValues deletes all keys and values from the state.
// This is generally only used during state synchronization when creating a
// new state from a snapshot.
func (ledger *Ledger) DeleteALLStateKeysAndValues() error {
	return ledger.state.DeleteState()
}

/////////////////// transaction related methods /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
func (ledger *Ledger) PoolTransaction(tx *protos.Transaction) {
	ledger.txpool.txPool[tx.GetTxid()] = tx
}

func (ledger *Ledger) PutTransactions(txs []*protos.Transaction) error {
	return ledger.txpool.putTransaction(txs)
}

// GetTransactionByID return transaction by it's txId
func (ledger *Ledger) GetTransactionByID(txID string) (*protos.Transaction, error) {
	return ledger.txpool.getTransaction(txID)
}

func (ledger *Ledger) GetCommitedTransaction(txID string) (*protos.Transaction, error) {
	return fetchTxFromDB(txID)
}

// GetTransactionByID return transaction by it's txId
func (ledger *Ledger) GetTransactionsByRange(statehash []byte, beg int, end int) ([]*protos.Transaction, error) {

	blk, err := ledger.blockchain.getBlockByState(statehash)
	if err != nil {
		return nil, err
	}

	if blk.GetTxids() == nil || len(blk.Txids) < end {
		return nil, ErrOutOfBounds
	}

	return fetchTxsFromDB(blk.Txids[beg:end]), nil
}

/////////////////// blockchain related methods /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

// GetBlockchainInfo returns information about the blockchain ledger such as
// height, current block hash, and previous block hash.
func (ledger *Ledger) GetBlockchainInfo() (*protos.BlockchainInfo, error) {
	return ledger.blockchain.getBlockchainInfo()
}

// HashBlock returns the hash of the included block, useful for mocking
func (*Ledger) HashBlock(block *protos.Block) ([]byte, error) {
	return block.GetHash()
}

// GetBlockByNumber return block given the number of the block on blockchain.
// Lowest block on chain is block number zero
func (ledger *Ledger) GetBlockByNumber(blockNumber uint64) (*protos.Block, error) {
	if blockNumber >= ledger.GetBlockchainSize() {
		return nil, ErrOutOfBounds
	}
	return ledger.blockchain.getBlock(blockNumber)
}

// GetBlockNumberByState return given block nuber from a state, also used to check
// states
func (ledger *Ledger) GetBlockNumberByState(hash []byte) (uint64, error) {
	//TODO: cache?
	return ledger.blockchain.indexer.fetchBlockNumberByStateHash(hash)
}

func (ledger *Ledger) GetBlockNumberByTxid(txID string) (uint64, uint64, error) {
	//TODO: cache?
	if _, ok := ledger.txpool.txPool[txID]; ok {
		return 0, 0, newLedgerError(ErrorTypeResourceNotFound, "ledger: tx is pending")
	}

	return ledger.blockchain.indexer.fetchTransactionIndexByID(txID)
}

// GetBlockchainSize returns number of blocks in blockchain
func (ledger *Ledger) GetBlockchainSize() uint64 {
	return ledger.blockchain.getSize()
}

// PutBlock put a raw block on the chain and also commit the corresponding transactions
func (ledger *Ledger) PutBlock(blockNumber uint64, block *protos.Block) error {

	//handle different type of block (with or without transaction included)
	var err error
	if block.GetTransactions() != nil {
		err = ledger.txpool.putTransaction(block.Transactions)
	} else {
		err = ledger.txpool.commitTransaction(block.GetTxids())
	}

	if err != nil {
		return err
	}

	return ledger.PutRawBlock(block, blockNumber)
}

// PutRawBlock just puts a raw block on the chain without handling the tx
func (ledger *Ledger) PutRawBlock(block *protos.Block, blockNumber uint64) error {

	err := ledger.blockchain.persistRawBlock(block, blockNumber)
	if err != nil {
		return err
	}
	sendProducerBlockEvent(block)
	return nil
}

// VerifyChain will verify the integrity of the blockchain. This is accomplished
// by ensuring that the previous block hash stored in each block matches
// the actual hash of the previous block in the chain. The return value is the
// block number of lowest block in the range which can be verified as valid.
// The first block is assumed to be valid, and an error is only returned if the
// first block does not exist, or some other sort of irrecoverable ledger error
// such as the first block failing to hash is encountered.
// For example, if VerifyChain(0, 99) is called and previous hash values stored
// in blocks 8, 32, and 42 do not match the actual hashes of respective previous
// block 42 would be the return value from this function.
// highBlock is the high block in the chain to include in verification. If you
// wish to verify the entire chain, use ledger.GetBlockchainSize() - 1.
// lowBlock is the low block in the chain to include in verification. If
// you wish to verify the entire chain, use 0 for the genesis block.
func (ledger *Ledger) VerifyChain(highBlock, lowBlock uint64) (uint64, error) {
	if highBlock >= ledger.GetBlockchainSize() {
		return highBlock, ErrOutOfBounds
	}
	if highBlock < lowBlock {
		return lowBlock, ErrOutOfBounds
	}

	currentBlock, err := ledger.GetBlockByNumber(highBlock)
	if err != nil {
		return highBlock, fmt.Errorf("Error fetching block %d.", highBlock)
	}
	if currentBlock == nil {
		return highBlock, fmt.Errorf("Block %d is nil.", highBlock)
	}

	for i := highBlock; i > lowBlock; i-- {
		previousBlock, err := ledger.GetBlockByNumber(i - 1)
		if err != nil {
			return i, nil
		}
		if previousBlock == nil {
			return i, nil
		}
		previousBlockHash, err := previousBlock.GetHash()
		if err != nil {
			return i, nil
		}
		if bytes.Compare(previousBlockHash, currentBlock.PreviousBlockHash) != 0 {
			return i, nil
		}
		currentBlock = previousBlock
	}

	return lowBlock, nil
}

//alias of verifychain
func (ledger *Ledger) VerifyBlockchain(highBlock, lowBlock uint64) (uint64, error) {
	return ledger.VerifyChain(highBlock, lowBlock)
}

func (ledger *Ledger) checkValidIDBegin() error {
	if ledger.currentID != nil {
		return fmt.Errorf("Another TxGroup [%s] already in-progress", ledger.currentID)
	}
	return nil
}

func (ledger *Ledger) checkValidIDCommitORRollback(id interface{}) error {
	if !reflect.DeepEqual(ledger.currentID, id) {
		return fmt.Errorf("Another TxGroup [%s] already in-progress", ledger.currentID)
	}
	return nil
}

func (ledger *Ledger) resetForNextTxGroup(txCommited bool) {
	ledgerLogger.Debug("resetting ledger state for next transaction batch")
	ledger.currentID = nil
	ledger.state.ClearInMemoryChanges(txCommited)
}

func sendProducerBlockEvent(block *protos.Block) {

	// Remove payload from deploy transactions. This is done to make block
	// events more lightweight as the payload for these types of transactions
	// can be very large.
	blockTransactions := block.GetTransactions()
	for _, transaction := range blockTransactions {
		if transaction.Type == protos.Transaction_CHAINCODE_DEPLOY {
			deploymentSpec := &protos.ChaincodeDeploymentSpec{}
			err := proto.Unmarshal(transaction.Payload, deploymentSpec)
			if err != nil {
				ledgerLogger.Errorf("Error unmarshalling deployment transaction for block event: %s", err)
				continue
			}
			deploymentSpec.CodePackage = nil
			deploymentSpecBytes, err := proto.Marshal(deploymentSpec)
			if err != nil {
				ledgerLogger.Errorf("Error marshalling deployment transaction for block event: %s", err)
				continue
			}
			transaction.Payload = deploymentSpecBytes
		}
	}

	producer.Send(producer.CreateBlockEvent(block))
}

//send chaincode events created by transactions
func sendChaincodeEvents(trs []*protos.TransactionResult) {
	if trs != nil {
		for _, tr := range trs {
			//we store empty chaincode events in the protobuf repeated array to make protobuf happy.
			//when we replay off a block ignore empty events
			if tr.ChaincodeEvent != nil && tr.ChaincodeEvent.ChaincodeID != "" {
				producer.Send(producer.CreateChaincodeEvent(tr.ChaincodeEvent))
			}
		}
	}
}
