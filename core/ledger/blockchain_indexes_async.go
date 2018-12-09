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

type blockIndex struct {
	blockNum  uint64
	blockHash []byte
	block     *protos.Block
	txindex   map[string]int
}

//we use a channel, instead of condition, to notify new block, so
//the worker can wait for both incoming block and external context
type indexerCache struct {
	blockChan chan *blockIndex
	sync.RWMutex
	index    map[uint64]*blockIndex
	indexErr error
}

func (c *indexerCache) fetchTransactionIndex(txID string) (uint64, uint64) {

	c.RLock()
	defer c.RUnlock()

	for blkI, idx := range c.index {
		if txI, ok := idx.txindex[txID]; ok {
			return blkI, uint64(txI)
		}
	}

	return 0, 0
}

func (c *indexerCache) fetchBlockNumberByBlockHash(hash []byte) uint64 {

	c.RLock()
	defer c.RUnlock()

	for blkI, idx := range c.index {
		if bytes.Compare(hash, idx.blockHash) == 0 {
			return blkI
		}
	}

	return 0
}

func (c *indexerCache) fetchBlockNumberByStateHash(hash []byte) uint64 {

	c.RLock()
	defer c.RUnlock()

	for blkI, idx := range c.index {
		if bytes.Compare(hash, idx.block.GetStateHash()) == 0 {
			return blkI
		}
	}

	return 0
}

func (c *indexerCache) cacheBlock(block *protos.Block, blockNumber uint64, blockHash []byte) (*blockIndex, error) {

	idx := &blockIndex{blockNumber, blockHash, block, make(map[string]int)}

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
	defer c.Unlock()
	//not allow cache more ...
	if c.indexErr != nil {
		return nil, c.indexErr
	}

	c.index[blockNumber] = idx

	return idx, nil
}

func (c *indexerCache) fetchAnyBlock() *blockIndex {
	c.Lock()
	defer c.Unlock()
	for _, block := range c.index {
		return block
	}

	return nil

}

func (c *indexerCache) purneCachedBlock(blockNumber uint64) {
	c.Lock()
	delete(c.index, blockNumber)
	c.Unlock()
}

func (c *indexerCache) setError(err error) {
	c.Lock()
	defer c.Unlock()
	if c.indexErr == nil {
		c.indexErr = err
		indexLogger.Debugf("setError() = %#v", c.indexErr)
	}
}

type indexerState struct {
	sync.Mutex
	indexProgress
}

type indexerWorkState struct {
	exitNotify chan interface{}
	exitError  error
}

func (w *indexerWorkState) setError(err error) {
	w.exitError = err
	close(w.exitNotify)
}

type blockchainIndexerAsync struct {
	*db.OpenchainDB
	// Channel for transferring block from block chain for indexing
	indexerStates []indexerWorkState
	cache         indexerCache
	prog          indexerState
	stopf         context.CancelFunc
}

const (
	defaultThreadCount    = 1
	defaultCachePerThread = 3
)

//legacy method
func newBlockchainIndexerAsync() *blockchainIndexerAsync {
	return newBlockchainIndexerAsyncEx(0)
}

func newBlockchainIndexerAsyncEx(threads int) *blockchainIndexerAsync {
	ret := new(blockchainIndexerAsync)
	if threads == 0 {
		indexLogger.Infof("async indexer not specified, set to default (%d)", defaultThreadCount)
		threads = defaultThreadCount
	} else if threads < defaultThreadCount {
		indexLogger.Warningf("user specified a thread count (%d) less than required (%d)", threads, defaultThreadCount)
	}

	ret.cache.index = make(map[uint64]*blockIndex)
	ret.cache.blockChan = make(chan *blockIndex, defaultCachePerThread*threads)
	ret.indexerStates = make([]indexerWorkState, threads)

	return ret
}

func (indexer *blockchainIndexerAsync) start(blockchain *blockchain) (err error) {

	indexer.OpenchainDB = blockchain.OpenchainDB
	err, indexer.prog.beginBlockID = checkIndex(blockchain)

	var ctx context.Context
	ctx, indexer.stopf = context.WithCancel(context.TODO())

	workerfunc := func(ind int) {
		defer indexer.indexerStates[ind].setError(fmt.Errorf("User stop"))
		var block *blockIndex

		for {
			indexLogger.Debug("Going to wait on channel for next block to index")

			//check available from channel first
			select {
			case block = <-indexer.cache.blockChan:
			default:
				block = indexer.cache.fetchAnyBlock()
			}

			if block == nil {
				//waiting-only phrase
				select {
				case <-ctx.Done():
					indexLogger.Debug("channel is closed, stop")
					return
				case block = <-indexer.cache.blockChan:
				}
			}

			err := indexer.createIndexesInternal(block)
			if err != nil {
				indexer.cache.setError(err)
				indexLogger.Errorf(
					"Error occured while indexing block number [%d]. Error: %s. Further blocks will not be indexed",
					block.blockNum, err)
			} else {
				indexer.cache.purneCachedBlock(block.blockNum)
			}
		}
	}

	indexLogger.Debugf("staring indexer (sync), lastIndexedBlockNum = [%d] after processing pending blocks",
		indexer.prog.GetProgress())

	for i, _ := range indexer.indexerStates {
		indexer.indexerStates[i].exitNotify = make(chan interface{})
		go workerfunc(i)
	}
	return nil
}

func (indexer *blockchainIndexerAsync) stop() {
	if indexer.stopf == nil {
		indexLogger.Warning("stop is called before start")
		return
	}
	indexer.stopf()
	for _, state := range indexer.indexerStates {
		<-state.exitNotify
	}
	indexLogger.Debugf("async indexer stopped")
}

//just for some legacy code (e.g. testing)
var omitCreateIndexFailure = false

func (indexer *blockchainIndexerAsync) createIndexes(block *protos.Block, blockNumber uint64, blockHash []byte) error {

	blockind, err := indexer.cache.cacheBlock(block, blockNumber, blockHash)

	if err != nil {
		if omitCreateIndexFailure {
			indexLogger.Warningf("cache indexs has failed [%s] but omitted")
			return nil
		}
		return fmt.Errorf("Can't cache block: %s", err)
	}

	for {
		select {
		case indexer.cache.blockChan <- blockind:
			return nil
		default:
			indexLogger.Warning("notify channel full, we may encounter a slow indexer")
		}
	}

}

// createIndexes adds entries into db for creating indexes on various attributes
func (indexer *blockchainIndexerAsync) createIndexesInternal(block *blockIndex) error {

	writeBatch := indexer.NewWriteBatch()
	defer writeBatch.Destroy()

	blockNumber := block.blockNum
	addIndexDataForPersistence(writeBatch, block.block, blockNumber, block.blockHash)
	if err := writeBatch.BatchCommit(); err != nil {
		return fmt.Errorf("fail in last commit batch: %s", err)
	}

	indexer.cache.purneCachedBlock(blockNumber)

	indexer.prog.Lock()
	defer indexer.prog.Unlock()

	before := indexer.prog.GetProgress()
	indexer.prog.FinishBlock(blockNumber)
	if after := indexer.prog.GetProgress(); after > before {

		if err := indexer.PutValue(db.IndexesCF, lastIndexedBlockKey, encodeBlockNumber(after)); err != nil {
			return fmt.Errorf("fail in write progress: %s", err)
		}
	}

	return nil
}

func (indexer *blockchainIndexerAsync) fetchBlockNumberByBlockHash(blockHash []byte) (uint64, error) {

	if bi := indexer.cache.fetchBlockNumberByBlockHash(blockHash); bi != 0 {
		return bi, nil
	}

	return fetchBlockNumberByBlockHashFromDB(indexer.OpenchainDB, blockHash)
}

func (indexer *blockchainIndexerAsync) fetchBlockNumberByStateHash(stateHash []byte) (uint64, error) {

	if bi := indexer.cache.fetchBlockNumberByStateHash(stateHash); bi != 0 {
		return bi, nil
	}

	return fetchBlockNumberByStateHashFromDB(indexer.OpenchainDB, stateHash)
}

func (indexer *blockchainIndexerAsync) fetchTransactionIndexByID(txID string) (bi uint64, ti uint64, err error) {

	bi, ti = indexer.cache.fetchTransactionIndex(txID)
	if bi != 0 {
		return
	}

	bi, ti, err = fetchTransactionIndexByIDFromDB(indexer.OpenchainDB, txID)
	return
}
