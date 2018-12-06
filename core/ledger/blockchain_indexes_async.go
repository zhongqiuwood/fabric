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
	"sync"

	"bytes"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
)

const maxPendingBlocksForIndexed = 3

type blockWrapper struct {
	block       *protos.Block
	blockNumber uint64
	blockHash   []byte
	stopNow     bool
}

type blockIndex struct {
	shash   []byte
	bhash   []byte
	txindex map[string]int
}

type indexerState struct {
	sync.RWMutex
	index map[uint64]*blockIndex
}

func (c *indexerState) fetchTransactionIndex(txID string) (uint64, uint64) {

	c.RLock()
	defer c.RUnlock()

	for blkI, idx := range c.index {
		if txI, ok := idx.txindex[txID]; ok {
			return blkI, uint64(txI)
		}
	}

	return 0, 0
}

func (c *indexerState) fetchBlockNumberByBlockHash(hash []byte) uint64 {

	c.RLock()
	defer c.RUnlock()

	for blkI, idx := range c.index {
		if bytes.Compare(hash, idx.bhash) == 0 {
			return blkI
		}
	}

	return 0
}

func (c *indexerState) fetchBlockNumberByStateHash(hash []byte) uint64 {

	c.RLock()
	defer c.RUnlock()

	for blkI, idx := range c.index {
		if bytes.Compare(hash, idx.shash) == 0 {
			return blkI
		}
	}

	return 0
}

func (c *indexerState) cacheBlock(block *protos.Block, blockNumber uint64, blockHash []byte) error {

	idx := &blockIndex{block.GetStateHash(), blockHash, make(map[string]int)}

	if txids := block.GetTxids(); txids != nil {
		for i, txid := range txids {
			idx.txindex[txid] = i
		}
	} else if txs := block.GetTransactions(); txs != nil {
		panic("Accept malformed block, we have deprecated code?")
		// indexLogger.Debugf("No txid array found in block, we have deprecated code?")
		// for i, tx := range txs {
		// 	idx.txindex[tx.GetTxid()] = i
		// }
	}

	c.Lock()
	//a sanity check
	if len(c.index) > 2*maxPendingBlocksForIndexed {
		panic("We have too many pending index, something wrong?")
	}
	c.index[blockNumber] = idx
	c.Unlock()

	return nil
}

func (c *indexerState) purneCachedBlock(blockNumber uint64) {
	c.Lock()
	delete(c.index, blockNumber)
	c.Unlock()
}

type blockchainIndexerAsync struct {
	*db.OpenchainDB
	// Channel for transferring block from block chain for indexing
	blockChan       chan blockWrapper
	indexerStates   []blockchainIndexerState
	state           *indexerState
	newBlockIndexed *sync.Cond
	stopf           context.CancelFunc
}

func newBlockchainIndexerAsync(threads int) *blockchainIndexerAsync {
	ret := new(blockchainIndexerAsync)
	ret.state = &indexerState{index: make(map[uint64]*blockIndex)}
	if threads == 0 {
		indexLogger.Warningf("async indexer not specified, set to 1")
		threads = 1
	}
	ret.indexerStates = make([]blockchainIndexerState, threads)
	return ret
}

func (indexer *blockchainIndexerAsync) start(blockchain *blockchain) error {
	indexer.blockchain = blockchain
	indexerState, err := newBlockchainIndexerState(indexer)
	if err != nil {
		return err
	}
	indexer.indexerState = indexerState
	indexLogger.Debugf("staring indexer, lastIndexedBlockNum = [%d]",
		indexer.indexerState.getLastIndexedBlockNumber())

	err = indexer.indexPendingBlocks()
	if err != nil {
		return err
	}
	indexLogger.Debugf("staring indexer, lastIndexedBlockNum = [%d] after processing pending blocks",
		indexer.indexerState.getLastIndexedBlockNumber())
	indexer.blockChan = make(chan blockWrapper, maxPendingBlocksForIndexed)
	var ctx context.Context
	ctx, indexer.stopf = context.WithCancel(context.TODO())

	go func() {
		defer indexer.indexerState.setError(fmt.Errorf("User stop"))
		for {
			indexLogger.Debug("Going to wait on channel for next block to index")
			select {
			case blockWrapper := <-indexer.blockChan:
				indexLogger.Debugf("Blockwrapper received on channel: block number = [%d]", blockWrapper.blockNumber)

				if indexer.indexerState.hasError() {
					indexLogger.Debugf("Not indexing block number [%d]. Because of previous error: %s.",
						blockWrapper.blockNumber, indexer.indexerState.getError())
					continue
				}

				err := indexer.createIndexesInternal(blockWrapper.block, blockWrapper.blockNumber, blockWrapper.blockHash)
				if err != nil {
					indexer.indexerState.setError(err)
					indexLogger.Errorf(
						"Error occured while indexing block number [%d]. Error: %s. Further blocks will not be indexed",
						blockWrapper.blockNumber, err)

				} else {
					indexer.indexerState.blockIndexed(blockWrapper.blockNumber)
					indexLogger.Debugf("Finished indexing block number [%d]", blockWrapper.blockNumber)
				}
			case <-ctx.Done():
				indexLogger.Debug("channel is closed, stop")
				return
			}

		}
	}()
	return nil
}

func (indexer *blockchainIndexerAsync) createIndexes(block *protos.Block, blockNumber uint64, blockHash []byte) error {

	err := indexer.indexerState.checkError()
	if err == nil {
		//only cache when indexer is working well...
		err = indexer.cache.cacheBlock(block, blockNumber, blockHash)
	}

	if err != nil {
		indexLogger.Warning("Can't cache block, we still forward but query may ruin:", err)
		//TODO: maybe we should return all of the error?
		//return err
	}

	for {
		select {
		case indexer.blockChan <- blockWrapper{block, blockNumber, blockHash, false}:
			return nil
		default:
			if err = indexer.indexerState.waitForLastCommittedBlock(); err != nil {
				return err
			}
		}
	}

}

// createIndexes adds entries into db for creating indexes on various attributes
func (indexer *blockchainIndexerAsync) createIndexesInternal(block *protos.Block, blockNumber uint64, blockHash []byte) error {

	writeBatch := indexer.blockchain.NewWriteBatch()
	defer writeBatch.Destroy()
	defer indexer.cache.purneCachedBlock(blockNumber)
	err := addIndexDataForPersistence(block, blockNumber, blockHash, writeBatch)
	if err != nil {
		return err
	}

	//block 0 will not be considered as a progress
	if blockNumber != 0 {
		indexer.prog.FinishBlock(blockNumber)
		writeBatch.PutCF(cf, lastIndexedBlockKey, encodeBlockNumber(indexer.prog.GetProgress()))
	}

	err = writeBatch.BatchCommit()
	if err != nil {
		return err
	}
	return nil
}

func (indexer *blockchainIndexerAsync) fetchBlockNumberByBlockHash(blockHash []byte) (uint64, error) {
	err := indexer.indexerState.checkError()
	if err != nil {
		indexLogger.Debug("Async indexer has a previous error. Returing the error")
		return 0, err
	}

	if bi := indexer.cache.fetchBlockNumberByBlockHash(blockHash); bi != 0 {
		return bi, nil
	}

	return fetchBlockNumberByBlockHashFromDB(indexer.blockchain.OpenchainDB, blockHash)
}

func (indexer *blockchainIndexerAsync) fetchBlockNumberByStateHash(stateHash []byte) (uint64, error) {
	err := indexer.indexerState.checkError()
	if err != nil {
		indexLogger.Debug("Async indexer has a previous error. Returing the error")
		return 0, err
	}

	if bi := indexer.cache.fetchBlockNumberByStateHash(stateHash); bi != 0 {
		return bi, nil
	}

	return fetchBlockNumberByStateHashFromDB(indexer.blockchain.OpenchainDB, stateHash)
}

func (indexer *blockchainIndexerAsync) fetchTransactionIndexByID(txID string) (bi uint64, ti uint64, err error) {
	err = indexer.indexerState.checkError()
	if err != nil {
		indexLogger.Debug("Async indexer has a previous error. Returing the error")
		return
	}

	bi, ti = indexer.cache.fetchTransactionIndex(txID)
	if bi != 0 {
		return
	}

	bi, ti, err = fetchTransactionIndexByIDFromDB(indexer.blockchain.OpenchainDB, txID)
	return
}

func (indexer *blockchainIndexerAsync) indexPendingBlocks() error {
	blockchain := indexer.blockchain
	if blockchain.getSize() == 0 {
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

func (indexer *blockchainIndexerAsync) fetchBlockFromDBAndCreateIndexes(blockNumber uint64) error {
	blockchain := indexer.blockchain
	blockToIndex, errBlockFetch := blockchain.getRawBlock(blockNumber)
	if errBlockFetch != nil {
		return errBlockFetch
	}

	//when get hash, we must prepare for a incoming "raw" block (hash is not )
	blockToIndex = compatibleLegacyBlock(blockToIndex)

	blockHash, errBlockHash := blockToIndex.GetHash()
	if errBlockHash != nil {
		return errBlockHash
	}
	indexer.createIndexesInternal(blockToIndex, blockNumber, blockHash)
	return nil
}

func (indexer *blockchainIndexerAsync) stop() {
	if indexer.stopf == nil {
		indexLogger.Warning("stop is called before start")
		return
	}
	indexer.stopf()
	indexer.indexerState.waitForLastCommittedBlock()
	indexLogger.Debugf("async indexer stopped: %s", indexer.indexerState.checkError())
}

// Code related to tracking the block number that has been indexed
// and if there has been an error in indexing a block
// Since, we index blocks asynchronously, there may be a case when
// a client query arrives before a block has been indexed.
//
// Do we really need strict semantics such that an index query results
// should include up to block number (or higher) that may have been committed
// when user query arrives?
// If a delay of a couple of blocks are allowed, we can get rid of this synchronization stuff
type blockchainIndexerState struct {
	indexer *blockchainIndexerAsync

	lastBlockIndexed uint64
	err              error
}

func newBlockchainIndexerState(indexer *blockchainIndexerAsync) (*blockchainIndexerState, error) {
	var lock sync.RWMutex
	zerothBlockIndexed, lastIndexedBlockNum, err := fetchLastIndexedBlockNumFromDB(indexer.blockchain.OpenchainDB)
	if err != nil {
		return nil, err
	}
	return &blockchainIndexerState{indexer, zerothBlockIndexed, lastIndexedBlockNum, nil, &lock, sync.NewCond(&lock)}, nil
}

func (indexerState *blockchainIndexerState) blockIndexed(blockNumber uint64) {
	indexerState.newBlockIndexed.L.Lock()
	defer indexerState.newBlockIndexed.L.Unlock()
	indexerState.lastBlockIndexed = blockNumber
	indexerState.zerothBlockIndexed = true
	indexerState.newBlockIndexed.Broadcast()
}

func (indexerState *blockchainIndexerState) getLastIndexedBlockNumber() uint64 {
	indexerState.lock.RLock()
	defer indexerState.lock.RUnlock()
	return indexerState.lastBlockIndexed
}

func (indexerState *blockchainIndexerState) isZerothBlockIndexed() bool {
	indexerState.lock.RLock()
	defer indexerState.lock.RUnlock()
	return indexerState.zerothBlockIndexed
}

func (indexerState *blockchainIndexerState) waitForLastCommittedBlock() error {
	indexLogger.Debugf("waitForLastCommittedBlock() indexerState.err = %#v", indexerState.err)
	chain := indexerState.indexer.blockchain
	indexerState.lock.Lock()
	defer indexerState.lock.Unlock()
	if indexerState.err != nil {
		return indexerState.err
	}

	lastBlockCommitted := chain.getSize() - 1
	indexLogger.Debugf(
		"Waiting for index to catch up with block chain. lastBlockCommitted=[%d] and lastBlockIndexed=[%d]",
		lastBlockCommitted, indexerState.lastBlockIndexed)
	indexerState.newBlockIndexed.Wait()

	return indexerState.err
}

func (indexerState *blockchainIndexerState) setError(err error) {
	indexerState.lock.Lock()
	defer indexerState.lock.Unlock()
	if indexerState.err == nil {
		indexerState.err = err
		indexLogger.Debugf("setError() indexerState.err = %#v", indexerState.err)
	}
	indexerState.newBlockIndexed.Broadcast()
}

func (indexerState *blockchainIndexerState) hasError() bool {
	indexerState.lock.RLock()
	defer indexerState.lock.RUnlock()
	return indexerState.err != nil
}

func (indexerState *blockchainIndexerState) getError() error {
	indexerState.lock.RLock()
	defer indexerState.lock.RUnlock()
	return indexerState.err
}

func (indexerState *blockchainIndexerState) checkError() error {
	indexerState.lock.RLock()
	defer indexerState.lock.RUnlock()
	if indexerState.err != nil {
		return fmt.Errorf(
			"An error had occured during indexing block number [%d]. So, index is out of sync. Detail of the error = %s",
			indexerState.getLastIndexedBlockNumber()+1, indexerState.err)
	}
	return indexerState.err
}

func fetchLastIndexedBlockNumFromDB(odb *db.OpenchainDB) (zerothBlockIndexed bool, lastIndexedBlockNum uint64, err error) {
	lastIndexedBlockNumberBytes, err := odb.GetValue(db.IndexesCF, lastIndexedBlockKey)
	if err != nil {
		return
	}
	if lastIndexedBlockNumberBytes == nil {
		return
	}
	lastIndexedBlockNum = decodeBlockNumber(lastIndexedBlockNumberBytes)
	zerothBlockIndexed = true
	return
}
