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
	"fmt"

	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"github.com/op/go-logging"
)

var indexLogger = logging.MustGetLogger("indexes")
var prefixBlockHashKey = byte(1)
var prefixTxIDKey = byte(2)
var prefixAddressBlockNumCompositeKey = byte(3)
var prefixStateHashKey = byte(4)
var lastIndexedBlockKey = []byte{byte(0)}

type blockchainIndexer interface {
	start(blockchain *blockchain) error
	createIndexes(block *protos.Block, blockNumber uint64, blockHash []byte) error
	fetchBlockNumberByBlockHash(blockHash []byte) (uint64, error)
	fetchBlockNumberByStateHash(stateHash []byte) (uint64, error)
	fetchTransactionIndexByID(txID string) (uint64, uint64, error)
	stop()
}

type indexProgress struct {
	beginBlockID  uint64
	pendingBlocks []bool
}

func (i *indexProgress) GetProgress() uint64 { return i.beginBlockID }

func (i *indexProgress) FinishBlock(id uint64) {
	if id <= i.beginBlockID {
		indexLogger.Errorf("Have rewinded id [%d] (current %d)", id, i.beginBlockID)
		return
	}

	//laid a shortcut here
	if id == i.beginBlockID+1 && i.pendingBlocks == nil {
		i.beginBlockID = id
		return
	}

	offset := int(id - i.beginBlockID)
	for offset > len(i.pendingBlocks) {
		i.pendingBlocks = append(i.pendingBlocks, false)
	}
	i.pendingBlocks[offset-1] = true

	for ii, done := range i.pendingBlocks {
		if !done {
			i.pendingBlocks = i.pendingBlocks[ii:]
			i.beginBlockID = i.beginBlockID + uint64(ii)
			return
		}
	}

	//if we come here, means all the pendingBlocks has been clean
	i.beginBlockID = i.beginBlockID + uint64(len(i.pendingBlocks))
	i.pendingBlocks = nil
}

// Implementation for sync indexer
type blockchainIndexerSync struct {
	*db.OpenchainDB
	prog indexProgress
}

func newBlockchainIndexerSync() *blockchainIndexerSync {
	return &blockchainIndexerSync{}
}

func (indexer *blockchainIndexerSync) start(blockchain *blockchain) error {
	indexer.OpenchainDB = blockchain.OpenchainDB
	return nil
}

func (indexer *blockchainIndexerSync) createIndexes(
	block *protos.Block, blockNumber uint64, blockHash []byte) error {

	writeBatch := indexer.NewWriteBatch()
	defer writeBatch.Destroy()

	if err := addIndexDataForPersistence(block, blockNumber, blockHash, writeBatch); err != nil {
		return err
	}

	//block 0 will not be considered as a progress
	if blockNumber != 0 {
		indexer.prog.FinishBlock(blockNumber)
		writeBatch.PutCF(cf, lastIndexedBlockKey, encodeBlockNumber(indexer.prog.GetProgress()))
	}

	if err := writeBatch.BatchCommit(); err != nil {
		return err
	}

	return nil
}

func (indexer *blockchainIndexerSync) fetchBlockNumberByBlockHash(blockHash []byte) (uint64, error) {
	return fetchBlockNumberByBlockHashFromDB(indexer.OpenchainDB, blockHash)
}

func (indexer *blockchainIndexerSync) fetchBlockNumberByStateHash(stateHash []byte) (uint64, error) {
	return fetchBlockNumberByStateHashFromDB(indexer.OpenchainDB, stateHash)
}

func (indexer *blockchainIndexerSync) fetchTransactionIndexByID(txID string) (uint64, uint64, error) {
	return fetchTransactionIndexByIDFromDB(indexer.OpenchainDB, txID)
}

func (indexer *blockchainIndexerSync) stop() {
	indexer.OpenchainDB = nil
	return
}

func indexPendingBlocks(db *db.OpenchainDB, uint64 totalSize) error {
	if totalSize == 0 {
		// chain is empty as yet
		return nil
	}

	lastCommittedBlockNum := blockchain.getSize() - 1
	lastIndexedBlockNum := indexer.indexerState.getLastIndexedBlockNumber()
	zerothBlockIndexed := indexer.indexerState.isZerothBlockIndexed()

	indexLogger.Debugf("lastCommittedBlockNum=[%d], lastIndexedBlockNum=[%d], zerothBlockIndexed=[%t]",
		lastCommittedBlockNum, lastIndexedBlockNum, zerothBlockIndexed)

	if zerothBlockIndexed && lastCommittedBlockNum == lastIndexedBlockNum {
		// all committed blocks are indexed
		return nil
	}

	indexLogger.Infof("Async indexer need to finshish pending block from %d to %d first, please wait ...", lastIndexedBlockNum, lastCommittedBlockNum)

	// block numbers use uint64 - so, 'lastIndexedBlockNum = 0' is ambiguous.
	// So, explicitly checking whether zero-th block has been indexed
	if !zerothBlockIndexed {
		err := indexer.fetchBlockFromDBAndCreateIndexes(0)
		if err != nil {
			return err
		}
	}

	for ; lastIndexedBlockNum < lastCommittedBlockNum; lastIndexedBlockNum++ {
		blockNumToIndex := lastIndexedBlockNum + 1
		err := indexer.fetchBlockFromDBAndCreateIndexes(blockNumToIndex)
		if err != nil {
			return err
		}
	}
	return nil
}

// Functions for persisting and retrieving index data
func addIndexDataForPersistence(block *protos.Block, blockNumber uint64, blockHash []byte, writeBatch *db.DBWriteBatch) error {
	// add blockhash -> blockNumber
	cf := writeBatch.GetDBHandle().IndexesCF

	indexLogger.Debugf("Indexing block number [%d] by hash = [%x]", blockNumber, blockHash)
	writeBatch.PutCF(cf, encodeBlockHashKey(blockHash), encodeBlockNumber(blockNumber))
	if block.GetStateHash() != nil {
		writeBatch.PutCF(cf, encodeStateHashKey(block.StateHash), encodeBlockNumber(blockNumber))
	}

	//	addressToTxIndexesMap := make(map[string][]uint64)
	//	addressToChaincodeIDsMap := make(map[string][]*protos.ChaincodeID)

	// so we compatible with new format of raw block (without contents for transaction)
	if txids := block.GetTxids(); txids != nil {
		for txIndex, txid := range txids {
			// add TxID -> (blockNumber,indexWithinBlock)
			writeBatch.PutCF(cf, encodeTxIDKey(txid), encodeBlockNumTxIndex(blockNumber, uint64(txIndex)))
		}
	} else if transactions := block.GetTransactions(); transactions != nil {
		for txIndex, tx := range transactions {
			// add TxID -> (blockNumber,indexWithinBlock)
			writeBatch.PutCF(cf, encodeTxIDKey(tx.GetTxid()), encodeBlockNumTxIndex(blockNumber, uint64(txIndex)))

			// txExecutingAddress := getTxExecutingAddress(tx)
			// addressToTxIndexesMap[txExecutingAddress] = append(addressToTxIndexesMap[txExecutingAddress], uint64(txIndex))

			// switch tx.Type {
			// case protos.Transaction_CHAINCODE_DEPLOY, protos.Transaction_CHAINCODE_INVOKE:
			// 	authroizedAddresses, chaincodeID := getAuthorisedAddresses(tx)
			// 	for _, authroizedAddress := range authroizedAddresses {
			// 		addressToChaincodeIDsMap[authroizedAddress] = append(addressToChaincodeIDsMap[authroizedAddress], chaincodeID)
			// 	}
			// }
		}
	}
	// for address, txsIndexes := range addressToTxIndexesMap {
	// 	writeBatch.PutCF(cf, encodeAddressBlockNumCompositeKey(address, blockNumber),
	// 		encodeListTxIndexes(txsIndexes))
	// }
	return nil
}

func fetchBlockNumberByBlockHashFromDB(odb *db.OpenchainDB, blockHash []byte) (uint64, error) {
	indexLogger.Debugf("fetchBlockNumberByBlockHashFromDB() for blockhash [%x]", blockHash)
	blockNumberBytes, err := odb.GetValue(db.IndexesCF, encodeBlockHashKey(blockHash))
	if err != nil {
		return 0, err
	}
	indexLogger.Debugf("blockNumberBytes for blockhash [%x] is [%x]", blockHash, blockNumberBytes)
	if len(blockNumberBytes) == 0 {
		return 0, newLedgerError(ErrorTypeBlockNotFound, fmt.Sprintf("No block indexed with block hash [%x]", blockHash))
	}
	blockNumber := decodeBlockNumber(blockNumberBytes)
	return blockNumber, nil
}

func fetchBlockNumberByStateHashFromDB(odb *db.OpenchainDB, stateHash []byte) (uint64, error) {
	indexLogger.Debugf("fetchBlockNumberByStateHashFromDB() for statehash [%x]", stateHash)
	blockNumberBytes, err := odb.GetValue(db.IndexesCF, encodeStateHashKey(stateHash))
	if err != nil {
		return 0, err
	}
	indexLogger.Debugf("blockNumberBytes for statehash [%x] is [%x]", stateHash, blockNumberBytes)
	if len(blockNumberBytes) == 0 {
		return 0, newLedgerError(ErrorTypeBlockNotFound, fmt.Sprintf("No block indexed with block hash [%x]", stateHash))
	}
	blockNumber := decodeBlockNumber(blockNumberBytes)
	return blockNumber, nil
}

func fetchTransactionIndexByIDFromDB(odb *db.OpenchainDB, txID string) (uint64, uint64, error) {
	blockNumTxIndexBytes, err := odb.GetValue(db.IndexesCF, encodeTxIDKey(txID))
	if err != nil {
		return 0, 0, err
	}
	if len(blockNumTxIndexBytes) == 0 {
		return 0, 0, newLedgerError(ErrorTypeBlockNotFound, fmt.Sprintf("No block indexed with transaction id [%s]", txID))
	}
	return decodeBlockNumTxIndex(blockNumTxIndexBytes)
}

func getTxExecutingAddress(tx *protos.Transaction) string {
	// TODO Fetch address form tx
	return "address1"
}

func getAuthorisedAddresses(tx *protos.Transaction) ([]string, *protos.ChaincodeID) {
	// TODO fetch address from chaincode deployment tx
	// TODO this method should also return error
	data := tx.ChaincodeID
	cID := &protos.ChaincodeID{}
	err := proto.Unmarshal(data, cID)
	if err != nil {
		return nil, nil
	}
	return []string{"address1", "address2"}, cID
}

// functions for encoding/decoding db keys/values for index data
// encode / decode BlockNumber
func encodeBlockNumber(blockNumber uint64) []byte {
	return proto.EncodeVarint(blockNumber)
}

func decodeBlockNumber(blockNumberBytes []byte) (blockNumber uint64) {
	blockNumber, _ = proto.DecodeVarint(blockNumberBytes)
	return
}

// encode / decode BlockNumTxIndex
func encodeBlockNumTxIndex(blockNumber uint64, txIndexInBlock uint64) []byte {
	b := proto.NewBuffer([]byte{})
	b.EncodeVarint(blockNumber)
	b.EncodeVarint(txIndexInBlock)
	return b.Bytes()
}

func decodeBlockNumTxIndex(bytes []byte) (blockNum uint64, txIndex uint64, err error) {
	b := proto.NewBuffer(bytes)
	blockNum, err = b.DecodeVarint()
	if err != nil {
		return
	}
	txIndex, err = b.DecodeVarint()
	if err != nil {
		return
	}
	return
}

// encode BlockHashKey
func encodeBlockHashKey(blockHash []byte) []byte {
	return prependKeyPrefix(prefixBlockHashKey, blockHash)
}

func encodeStateHashKey(stateHash []byte) []byte {
	return prependKeyPrefix(prefixStateHashKey, stateHash)
}

// encode TxIDKey
func encodeTxIDKey(txID string) []byte {
	return prependKeyPrefix(prefixTxIDKey, []byte(txID))
}

func encodeAddressBlockNumCompositeKey(address string, blockNumber uint64) []byte {
	b := proto.NewBuffer([]byte{prefixAddressBlockNumCompositeKey})
	b.EncodeRawBytes([]byte(address))
	b.EncodeVarint(blockNumber)
	return b.Bytes()
}

func encodeListTxIndexes(listTx []uint64) []byte {
	b := proto.NewBuffer([]byte{})
	for i := range listTx {
		b.EncodeVarint(listTx[i])
	}
	return b.Bytes()
}

func prependKeyPrefix(prefix byte, key []byte) []byte {
	modifiedKey := []byte{}
	modifiedKey = append(modifiedKey, prefix)
	modifiedKey = append(modifiedKey, key...)
	return modifiedKey
}
