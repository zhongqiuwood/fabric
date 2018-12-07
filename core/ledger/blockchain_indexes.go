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

	"encoding/binary"
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
var prefixIndexMarking = byte(8)
var indexMarkingMagicCode = []byte{42,42,42}
var lastIndexedBlockKey = []byte{byte(0)}

type blockchainIndexer interface {
	start(blockchain *blockchain) error
	createIndexes(block *protos.Block, blockNumber uint64, blockHash []byte) error
	fetchBlockNumberByBlockHash(blockHash []byte) (uint64, error)
	fetchBlockNumberByStateHash(stateHash []byte) (uint64, error)
	fetchTransactionIndexByID(txID string) (uint64, uint64, error)
	stop()
}

// Implementation for sync indexer
type blockchainIndexerSync struct {
	*db.OpenchainDB
	maxBlockSize uint64
	curIndexedBlockNum uint64
}

func newBlockchainIndexerSync() *blockchainIndexerSync {
	return &blockchainIndexerSync{}
}

func (indexer *blockchainIndexerSync) start(blockchain *blockchain) (err error) {
	indexer.OpenchainDB = blockchain.OpenchainDB
	indexer.maxBlockSize = blockchain.getSize()
	err, indexer.curIndexedBlockNum = checkIndex(blockchain, indexer.maxBlockNum)
	return 
}

func (indexer *blockchainIndexerSync) createIndexes(
	block *protos.Block, blockNumber uint64, blockHash []byte) error {

	if indexer.curIndexedBlockNum

	writeBatch := indexer.NewWriteBatch()
	defer writeBatch.Destroy()

	if err := addIndexDataForPersistence(block, blockNumber, blockHash, writeBatch); err != nil {
		return err
	}

	if progressIndexs(indexer.OpenchainDB, indexer.maxBlockNum, indexer.curIndexedBlockNum, nil)

	if err := writeBatch.BatchCommit(); err != nil {
		return err
	}

	if indexer.maxBlockSize <= blockNumber{
		indexer.maxBlockSize = blockNumber + 1
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

func checkIndex(odb *db.OpenchainDB, totalSize uint64) (error, uint64) {

	if totalSize == 0 {
		// chain is empty as yet
		return nil, 0
	}

	lastIndexedBlockNum := uint64(0)
	lastCommittedBlockNum := totalSize - 1
	if lastIndexedBlockNumberBytes, err := odb.GetValue(db.IndexesCF, lastIndexedBlockKey); err != nil {
		return err, 0
	} else if lastIndexedBlockNumberBytes != nil {
		//so if we have a "last indexed" record, we start from which plus one
		lastIndexedBlockNum = decodeBlockNumber(lastIndexedBlockNumberBytes) + 1
	}

	if totalSize == lastIndexedBlockNum {
		// all committed blocks are indexed
		return nil, 0
	} else if totalSize < lastIndexedBlockNum {
		//TODO: should be suppose error?
		indexLogger.Warningf("Encounter a higher index [%d] record than current block size [%d]", lastIndexedBlockNum, totalSize)
		//return nil, 0
	}

	indexLogger.Infof("indexer need to finshish pending block from %d to %d first, please wait ...", lastIndexedBlockNum, lastCommittedBlockNum)

	writeBatch := odb.NewWriteBatch()
	defer writeBatch.Destroy()

	calledCounter := 0
	writef := func(block *protos.Block, blocknum uint64, blockhash []byte) error{
		addIndexDataForPersistence(writeBatch, block, blocknum, blockhash)
		calledCounter++
		justCommit = false
		//so we commit each 8 blocks
		if calledCounter > 8 {
			calledCounter = 0
			justCommit = true
			if err := writeBatch.BatchCommit(); err != nil {
				return fmt.Errorf("fail in commit batch: %s", err)
			}	
		}
		return nil
	}

	finalBlockNum, err := progressIndexs(odb, totalSize, lastIndexedBlockNum, writef)
	if err != nil{
		return fmt.Errorf("progressIndexs fail: %s", err), 0
	}

	writeBatch.PutCF(writeBatch.GetDBHandle().IndexesCF, lastIndexedBlockKey, encodeBlockNumber(finalBlockNum))
	if err := writeBatch.BatchCommit(); err != nil {
		return fmt.Errorf("fail in last commit batch: %s", err), 0
	}
	indexLogger.Infof("indexer task [%d - %d] finished at %d", lastIndexedBlockNum, lastCommittedBlockNum, finalBlockNum)
	
	return nil, finalBlockNum
}

//check our progress, obtain the real "lastIndexed" number and return
//the incoming "lastIndexed" number is the recorded number plus 1
func progressIndexs(odb *db.OpenchainDB, lastCommitedBlockNum uint64, lastIndexedBlockNum uint64,
	persistBlock func(*protos.Block, uint64, []byte)error) (error, uint64) {

	indexLogger.Debugf("lastCommittedBlockNum=[%d], lastIndexedBlockNum=[%d]",
	lastCommitedBlockNum, lastIndexedBlockNum)

	//the cache iterator will greatly help the efficiency
	indMarkItr := odb.GetIterator(db.IndexesCF)
	defer indMarkItr.Close()
	indMarkItr.Seek(encodeIndexMarkKey(lastIndexedBlockNum))

	for ; lastIndexedBlockNum <= lastCommitedBlockNum; lastIndexedBlockNum++ {

		if indMarkItr != nil{
			if !indMarkItr.Valid() || indMarkItr.ValidForPrefix(prefixIndexMarking){
				indexLogger.Infof("We have no cache from [%d], iterator closed", lastIndexedBlockNum)
				indMarkItr = nil
			}else if num, err := decodeAndVerifyIndexMarkKey(indMarkItr.Key().Data(), indMarkItr.Value().Data()); err != nil{
				indexLogger.Error("Wrong cache mark data:", err)
				indMarkItr.Next() //TODO: try next? or fail? that's a problem
			}else if num < lastIndexedBlockNum{
				indexLogger.Errorf("Encounter a less number in key than expected [%d vs %d], something wrong, iterator closed", num, lastIndexedBlockNum)
				//this should not happen ...
				indMarkItr = nil
			}else if num == lastIndexedBlockNum{
				//good, we can skip this number and go for next one
				indMarkItr.Next()
				indexLogger.Debugf("Match cached at %d, forward next one", num)
				continue
			}else{
				indexLogger.Debugf("block %d is not marked yet, deep inspect this, next one is %d", lastIndexedBlockNum,num)
			}
		}

		blockToIndex, err := fetchRawBlockFromDB(odb, lastIndexedBlockNum)
		//interrupt for any db error
		if err != nil {
			return fmt.Errorf("db fail in getblock: %s", err), 0
		}

		//when get hash, we must prepare for a incoming "raw" block (the old version require txs to obtain blockhash)
		blockToIndex = compatibleLegacyBlock(blockToIndex)
		blockHash, err := blockToIndex.GetHash()
		if err != nil {
			return fmt.Errorf("fail in obtained block %d's hash: %s", lastIndexedBlockNum, err), 0
		}

		//if we index genesis block, no check is needed
		if lastIndexedBlockNum != 0 {
			//add checking ...
			if index, err := fetchBlockNumberByBlockHashFromDB(odb, blockHash); err != nil {
				return fmt.Errorf("db fail in get index: %s", err)
			} else if index == lastIndexedBlockNum {
				//has been indexed, we can skip
				indexLogger.Infof("Match has been cached at %d, forward next one", lastIndexedBlockNum)
				//but still add the cache mark so we can go faster even the index building
				//ruined in progress
				odb.PutValue(db.IndexesCF, encodeIndexMarkKey(index), indexMarkingMagicCode)
				continue
			} else if index != 0 {
				indexLogger.Errorf("block %d is indexed at different position %d", lastIndexedBlockNum, index)
			}
		}

		err = persistBlock(blockToIndex, lastIndexedBlockNum, blockHash)
		if err != nil {
			return fmt.Errorf("fail in execute persistent index: %s", err), 0
		}
	}

	return nil, lastIndexedBlockNum
}

// Functions for persisting and retrieving index data
func addIndexDataForPersistence(writeBatch *db.DBWriteBatch, block *protos.Block, blockNumber uint64, blockHash []byte) {

	cf := writeBatch.GetDBHandle().IndexesCF

	indexLogger.Debugf("Indexing block number [%d] by hash = [%x]", blockNumber, blockHash)
	//add the mark
	writeBatch.PutCF(cf, encodeIndexMarkKey(blockNumber), indexMarkingMagicCode)

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

func decodeAndVerifyIndexMarkKey(key []byte, val []byte) (uint64, error) {

	if len(key) < 9 {
		return 0, fmt.Errorf("We obtain wrong key content: %x", key)
	}

	num := binary.BigEndian.Uint64(key[1:])

	//check the magic code ([]byte{42,42,42})
	if len(val) != 3 || val[0] != 42 || val[1] != 42 || val[2] = 42{
		return num, fmt.Errorf("We obtain a cache mark which contain not the magic code: %x", val)
	}

	return num, nil
}

func encodeIndexMarkKey(num uint64) []byte {

	buffer := [9]byte{prefixIndexMarking}
	binary.BigEndian.PutUint64(buffer, num)
	return buffer
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
