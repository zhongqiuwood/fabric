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

package buckettree

import (
	"bytes"

	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/op/go-logging"
	pb "github.com/abchain/fabric/protos"
	"github.com/abchain/fabric/core/ledger/statemgmt/persist"
	"github.com/golang/proto"
	"fmt"
	"github.com/spf13/viper"
)

var logger = logging.MustGetLogger("buckettree")

// StateImpl - implements the interface - 'statemgmt.HashableState'
type StateImpl struct {
	*db.OpenchainDB
	dataNodesDelta         *dataNodesDelta  // bucket nodes map  level-bucketNum -> node, stores users key-value
	bucketTreeDelta        *bucketTreeDelta // bucket tree, each node contains all its child hash
	persistedStateHash     []byte
	lastComputedCryptoHash []byte
	recomputeCryptoHash    bool
	bucketCache            *bucketCache
}

// NewStateImpl constructs a new StateImpl
func NewStateImpl(db *db.OpenchainDB) *StateImpl {
	return &StateImpl{OpenchainDB: db}
}

// Initialize - method implementation for interface 'statemgmt.HashableState'
func (stateImpl *StateImpl) Initialize(configs map[string]interface{}) error {
	initConfig(configs)
	rootBucketNode, err := fetchBucketNodeFromDB(stateImpl.OpenchainDB, constructRootBucketKey())
	if err != nil {
		return err
	}
	if rootBucketNode != nil {
		stateImpl.persistedStateHash = rootBucketNode.computeCryptoHash()
		stateImpl.lastComputedCryptoHash = stateImpl.persistedStateHash
	}

	bucketCacheMaxSize, ok := configs["bucketCacheSize"].(int)
	if !ok {
		bucketCacheMaxSize = defaultBucketCacheMaxSize
	}
	stateImpl.bucketCache = newBucketCache(bucketCacheMaxSize, stateImpl.OpenchainDB)
	stateImpl.bucketCache.loadAllBucketNodesFromDB()
	return nil
}



// Get - method implementation for interface 'statemgmt.HashableState'
func (stateImpl *StateImpl) Get(chaincodeID string, key string) ([]byte, error) {
	dataKey := newDataKey(chaincodeID, key)
	dataNode, err := fetchDataNodeFromDB(stateImpl.OpenchainDB, dataKey)
	if err != nil {
		return nil, err
	}
	if dataNode == nil {
		return nil, nil
	}
	return dataNode.value, nil
}

// PrepareWorkingSet - method implementation for interface 'statemgmt.HashableState'
func (stateImpl *StateImpl) PrepareWorkingSet(stateDelta *statemgmt.StateDelta) error {
	logger.Debug("Enter - PrepareWorkingSet()")
	if stateDelta.IsEmpty() {
		logger.Debug("Ignoring working-set as it is empty")
		return nil
	}
	stateImpl.dataNodesDelta = newDataNodesDelta(stateDelta)
	stateImpl.bucketTreeDelta = newBucketTreeDelta()
	stateImpl.recomputeCryptoHash = true
	return nil
}

// ClearWorkingSet - method implementation for interface 'statemgmt.HashableState'
func (stateImpl *StateImpl) ClearWorkingSet(changesPersisted bool, reloadCache bool) {
	if changesPersisted {
		stateImpl.persistedStateHash = stateImpl.lastComputedCryptoHash
		stateImpl.updateBucketCache()
	} else {
		stateImpl.lastComputedCryptoHash = stateImpl.persistedStateHash
	}
	stateImpl.dataNodesDelta = nil
	stateImpl.bucketTreeDelta = nil
	stateImpl.recomputeCryptoHash = false

	if reloadCache {
		stateImpl.bucketCache.loadAllBucketNodesFromDB()
	}
}

// ComputeCryptoHash - method implementation for interface 'statemgmt.HashableState'
func (stateImpl *StateImpl) ComputeCryptoHash() ([]byte, error) {
	logger.Debug("Enter - ComputeCryptoHash()")
	if stateImpl.recomputeCryptoHash {
		logger.Debug("Recomputing crypto-hash...")
		err := stateImpl.processDataNodeDelta() // feed all leaf nodes(level n) and all their parent nodes(level n-1)
		if err != nil {
			return nil, err
		}
		err = stateImpl.processBucketTreeDelta()  // feed all other nodes(level n-2 to level 0)
		if err != nil {
			return nil, err
		}
		stateImpl.lastComputedCryptoHash = stateImpl.computeRootNodeCryptoHash()
		stateImpl.recomputeCryptoHash = false
	} else {
		logger.Debug("Returing existing crypto-hash as recomputation not required")
	}
	return stateImpl.lastComputedCryptoHash, nil
}

func (stateImpl *StateImpl) processDataNodeDelta() error {
	afftectedBuckets := stateImpl.dataNodesDelta.getAffectedBuckets()

	it := stateImpl.OpenchainDB.GetIterator(db.StateCF)
	defer it.Close()

	for _, bucketKey := range afftectedBuckets {

		updatedDataNodes := stateImpl.dataNodesDelta.getSortedDataNodesFor(bucketKey)

		existingDataNodes, err := fetchDataNodesFromDBFor(it, bucketKey)
		if err != nil {
			return err
		}
		cryptoHashForBucket := computeDataNodesCryptoHash(bucketKey, updatedDataNodes, existingDataNodes)
		logger.Debugf("Crypto-hash for lowest-level bucket [%s] is [%x]", bucketKey, cryptoHashForBucket)
		parentBucket := stateImpl.bucketTreeDelta.getOrCreateBucketNode(bucketKey.getParentKey())

		logger.Debugf("Feed DataNode<%s> to parentBucket [%+v]", bucketKey, parentBucket.bucketKey)

		// set second last level children hash by index
		parentBucket.setChildCryptoHash(bucketKey, cryptoHashForBucket)
	}
	return nil
}

func (stateImpl *StateImpl) processBucketTreeDelta() error {
	secondLastLevel := conf.getLowestLevel() - 1
	for level := secondLastLevel; level >= 0; level-- {
		bucketNodes := stateImpl.bucketTreeDelta.getBucketNodesAt(level)
		logger.Debugf("Bucket tree delta. Number of buckets at level [%d] are [%d]", level, len(bucketNodes))
		for _, bucketNode := range bucketNodes {
			logger.Debugf("bucketNode in tree-delta [%s]", bucketNode)
			// get middle node from db
			dbBucketNode, err := stateImpl.bucketCache.get(*bucketNode.bucketKey)
			logger.Debugf("bucket node from db [%s]", dbBucketNode)
			if err != nil {
				return err
			}

			// merge updated child hash into middle node by index
			if dbBucketNode != nil {
				bucketNode.mergeBucketNode(dbBucketNode)
				logger.Debugf("After merge... bucketNode in tree-delta [%s]", bucketNode)
			}
			if level == 0 {
				return nil
			}
			logger.Debugf("Computing cryptoHash for bucket [%s]", bucketNode)
			cryptoHash := bucketNode.computeCryptoHash()
			logger.Debugf("cryptoHash for bucket [%s] is [%x]", bucketNode, cryptoHash)
			parentBucket := stateImpl.bucketTreeDelta.getOrCreateBucketNode(bucketNode.bucketKey.getParentKey())

			logger.Debugf("Feed bucketNode <%s> to parentBucket [%+v]", bucketNode.bucketKey, parentBucket.bucketKey)
			parentBucket.setChildCryptoHash(bucketNode.bucketKey, cryptoHash)
		}
	}
	return nil
}

func (stateImpl *StateImpl) computeRootNodeCryptoHash() []byte {
	return stateImpl.bucketTreeDelta.getRootNode().computeCryptoHash()
}

// compute a leaf bucket hash by updatedNodes and existingNodes
func computeDataNodesCryptoHash(bucketKey *bucketKey, updatedNodes dataNodes, existingNodes dataNodes) []byte {
	logger.Debugf("Computing crypto-hash for bucket [%s]. numUpdatedNodes=[%d], numExistingNodes=[%d]",
		bucketKey, len(updatedNodes), len(existingNodes))

	bucketHashCalculator := newBucketHashCalculator(bucketKey)
	i := 0
	j := 0
	for i < len(updatedNodes) && j < len(existingNodes) {
		updatedNode := updatedNodes[i]
		existingNode := existingNodes[j]
		c := bytes.Compare(updatedNode.dataKey.compositeKey, existingNode.dataKey.compositeKey)
		var nextNode *dataNode
		switch c {
		case -1:
			nextNode = updatedNode
			i++
		case 0:
			nextNode = updatedNode
			i++
			j++
		case 1:
			nextNode = existingNode
			j++
		}
		if !nextNode.isDelete() {
			bucketHashCalculator.addNextNode(nextNode)
		}
	}

	var remainingNodes dataNodes
	if i < len(updatedNodes) {
		remainingNodes = updatedNodes[i:]
	} else if j < len(existingNodes) {
		remainingNodes = existingNodes[j:]
	}

	for _, remainingNode := range remainingNodes {
		if !remainingNode.isDelete() {
			bucketHashCalculator.addNextNode(remainingNode)
		}
	}
	return bucketHashCalculator.computeCryptoHash()
}

// AddChangesForPersistence - method implementation for interface 'statemgmt.HashableState'
func (stateImpl *StateImpl) AddChangesForPersistence(writeBatch *db.DBWriteBatch) error {

	if stateImpl.dataNodesDelta == nil {
		return nil
	}

	if stateImpl.recomputeCryptoHash {
		_, err := stateImpl.ComputeCryptoHash()
		if err != nil {
			return nil
		}
	}
	stateImpl.addDataNodeChangesForPersistence(writeBatch)
	stateImpl.addBucketNodeChangesForPersistence(writeBatch)
	return nil
}

func (stateImpl *StateImpl) addDataNodeChangesForPersistence(writeBatch *db.DBWriteBatch) {
	openchainDB := writeBatch.GetDBHandle()
	affectedBuckets := stateImpl.dataNodesDelta.getAffectedBuckets()
	for _, affectedBucket := range affectedBuckets {
		dataNodes := stateImpl.dataNodesDelta.getSortedDataNodesFor(affectedBucket)
		for _, dataNode := range dataNodes {
			if dataNode.isDelete() {
				logger.Debugf("Deleting data node key = %#v", dataNode.dataKey)
				writeBatch.DeleteCF(openchainDB.StateCF, dataNode.dataKey.getEncodedBytes())
			} else {
				logger.Debugf("Adding data node with dataKey<%x>, compositedKey<%s>, value = %#v",
					dataNode.dataKey.getEncodedBytes(),
					dataNode.dataKey.compositeKey, dataNode.value)
				writeBatch.PutCF(openchainDB.StateCF, dataNode.dataKey.getEncodedBytes(), dataNode.value)
			}
		}
	}
}

func (stateImpl *StateImpl) addBucketNodeChangesForPersistence(writeBatch *db.DBWriteBatch) {
	openchainDB := writeBatch.GetDBHandle()
	secondLastLevel := conf.getLowestLevel() - 1
	for level := secondLastLevel; level >= 0; level-- {
		bucketNodes := stateImpl.bucketTreeDelta.getBucketNodesAt(level)
		for _, bucketNode := range bucketNodes {
			if bucketNode.markedForDeletion {
				writeBatch.DeleteCF(openchainDB.StateCF, bucketNode.bucketKey.getEncodedBytes())
			} else {

				logger.Debugf("Adding data node<%s> with dataKey<%x>, value <%x>",bucketNode.bucketKey,
					bucketNode.bucketKey.getEncodedBytes(),	bucketNode.marshal())

				writeBatch.PutCF(openchainDB.StateCF,
					bucketNode.bucketKey.getEncodedBytes(), bucketNode.marshal())
			}
		}
	}
}

func (stateImpl *StateImpl) updateBucketCache() {
	if stateImpl.bucketTreeDelta == nil || stateImpl.bucketTreeDelta.isEmpty() {
		return
	}
	stateImpl.bucketCache.lock.Lock()
	defer stateImpl.bucketCache.lock.Unlock()
	secondLastLevel := conf.getLowestLevel() - 1
	for level := 0; level <= secondLastLevel; level++ {
		bucketNodes := stateImpl.bucketTreeDelta.getBucketNodesAt(level)
		for _, bucketNode := range bucketNodes {
			key := *bucketNode.bucketKey
			if bucketNode.markedForDeletion {
				stateImpl.bucketCache.removeWithoutLock(key)
			} else {
				stateImpl.bucketCache.putWithoutLock(key, bucketNode)
			}
		}
	}
}

// PerfHintKeyChanged - method implementation for interface 'statemgmt.HashableState'
func (stateImpl *StateImpl) PerfHintKeyChanged(chaincodeID string, key string) {
	// We can create a cache. Pull all the keys for the bucket (to which given key belongs) in a separate thread
	// This prefetching can help making method 'ComputeCryptoHash' faster.
}

// GetStateSnapshotIterator - method implementation for interface 'statemgmt.HashableState'
func (stateImpl *StateImpl) GetStateSnapshotIterator(snapshot *db.DBSnapshot) (statemgmt.StateSnapshotIterator, error) {
	return newStateSnapshotIterator(snapshot)
}

// GetRangeScanIterator - method implementation for interface 'statemgmt.HashableState'
func (stateImpl *StateImpl) GetRangeScanIterator(chaincodeID string, startKey string, endKey string) (statemgmt.RangeScanIterator, error) {
	return newRangeScanIterator(stateImpl.OpenchainDB, chaincodeID, startKey, endKey)
}

// report local root hash to server
func (stateImpl *StateImpl) getRootStateHashFromDB(snapshotHandler *db.DBSnapshot) ([]byte, error) {

	var persistedStateHash []byte = nil
	var rootBucketNode *bucketNode
	var err error

	rootBucketNode, err = fetchBucketNode(snapshotHandler, stateImpl.OpenchainDB, constructRootBucketKey())

	if err == nil && rootBucketNode != nil {
		persistedStateHash = rootBucketNode.computeCryptoHash()
	}
	return persistedStateHash, err
}

func (stateImpl *StateImpl) VerifySyncState(syncState *pb.SyncState, snapshotHandler *db.DBSnapshot) error {

	var err error
	var localHash []byte
	btOffset, err := byte2BucketTreeOffset(syncState.Offset.Data)
	if err != nil {
		return err
	}

	logger.Infof("state offset: <%+v>, config<%v>", btOffset, conf)

	if btOffset.BucketNum == 0 {
		if syncState.Statehash != nil {
			err = fmt.Errorf("Invalid Statehash<%x>. The nil expected", syncState.Statehash)
		}
	} else {

		localHash, err = ComputeStateHashByOffset(syncState.Offset, snapshotHandler)
		if !bytes.Equal(localHash, syncState.Statehash) {
			err = fmt.Errorf("Wrong Statehash, at level-num<%d-%d>\n" +
				"remote hash<%x>\n" +
				"local  hash<%x>",
				btOffset.Level, btOffset.BucketNum,
				syncState.Statehash, localHash)
		}
	}
	return err
}



func (stateImpl *StateImpl) GetStateDeltaFromDB(offset *pb.StateOffset, snapshotHandler *db.DBSnapshot) (*pb.SyncStateChunk, error){

	var err error
	var stateDelta *statemgmt.StateDelta
	stateChunk := &pb.SyncStateChunk{}

	bucketTreeOffset, err := byte2BucketTreeOffset(offset.Data)

	if err != nil {
		return nil, err
	}

	level := int(bucketTreeOffset.Level)
	startNum := int(bucketTreeOffset.BucketNum)
	endNum := int(bucketTreeOffset.Delta + bucketTreeOffset.BucketNum - 1)

	if level > conf.GetLowestLevel() {
		return nil, fmt.Errorf("invalid level")
	}

	maxBucketNum := conf.GetNumBuckets(level)
	if maxBucketNum < endNum {
		return nil, fmt.Errorf("invalid offset")
	} else if maxBucketNum == endNum {
		stateChunk.Roothash, err = stateImpl.getRootStateHashFromDB(snapshotHandler)
		if err != nil {
			return nil, err
		}
	}

	stateDeltaAll := statemgmt.NewStateDelta()

	itr := snapshotHandler.GetStateCFSnapshotIterator()
	defer itr.Close()

	for index := startNum; index <= endNum; index++ {

		if viper.GetBool("peer.breakpoint") {
			// for test only
			if index > maxBucketNum / 2 {
				err = fmt.Errorf("hit breakpoint at <%d-%d>, bucket tree: level<%d>, bucketNum<%d>",
					level, index, level, maxBucketNum, )
				break
			}
		}

		start, end := conf.getLeafBuckets(int(level), int(index))
		stateDelta, err = produceStateDeltaFromDB(start, end, itr)
		if err != nil {
			break
		}
		stateDeltaAll.ApplyChanges(stateDelta)
	}
	stateChunk.ChaincodeStateDeltas = stateDeltaAll.ChaincodeStateDeltas

	return stateChunk, err
}



func (impl *StateImpl) SaveStateOffset(committedOffset *pb.StateOffset) error {

	btoffset, err := byte2BucketTreeOffset(committedOffset.Data)

	if err != nil {
		return err
	}
	logger.Debugf("Committed state offset: level-num<%d-%d>",
		btoffset.Level,	btoffset.BucketNum + btoffset.Delta - 1)

	return persist.StoreSyncPosition(committedOffset.Data)
}

func (impl *StateImpl) LoadStateOffsetFromDB() []byte {
	return persist.LoadSyncPosition()
}


func (impl *StateImpl) NextStateOffset(curOffset *pb.StateOffset)(*pb.StateOffset, error) {

	var err error
	var data []byte
	var bucketTreeOffset *pb.BucketTreeOffset

	if curOffset == nil {
		data = persist.LoadSyncPosition()
	} else {
		data = curOffset.Data
	}

	if data == nil {
		bucketTreeOffset = &pb.BucketTreeOffset{}
		bucketTreeOffset.Level = uint64(conf.GetSyncLevel())
		maxNum := uint64(conf.GetNumBuckets(int(bucketTreeOffset.Level)))

		bucketTreeOffset.Delta = min(uint64(conf.syncDelta), maxNum)
		bucketTreeOffset.BucketNum = 1
	} else {

		bucketTreeOffset, err = byte2BucketTreeOffset(data)
		if err != nil {
			return nil, err
		}

		maxNum := uint64(conf.GetNumBuckets(int(bucketTreeOffset.Level)))

		bucketTreeOffset.BucketNum += bucketTreeOffset.Delta
		if maxNum <= bucketTreeOffset.BucketNum - 1 {
			logger.Infof("Hit maxBucketNum<%d>, target BucketNum<%d>", maxNum,  bucketTreeOffset.BucketNum)
			return nil, nil
		}
		bucketTreeOffset.Delta = min(uint64(conf.syncDelta), maxNum - bucketTreeOffset.BucketNum + 1)
	}

	logger.Debugf("Next state offset <%+v>", bucketTreeOffset)
	nextOffset := &pb.StateOffset{}
	nextOffset.Data, err = bucketTreeOffset2Byte(bucketTreeOffset)

	return nextOffset, err
}


// return root hash of a bucket tree consisted of all dataNodes belong to bucket nodes between [lv-0, lv-bucketNum] include,
// if lv is the lowest level, then the bucket tree contains all all dataNode [0, bucketNum]
func ComputeStateHashByOffset(offset *pb.StateOffset, snapshotHandler *db.DBSnapshot) ([]byte, error) {

	btoffset, err := byte2BucketTreeOffset(offset.Data)

	if err != nil {
		return nil, err
	}

	lv, bucketNum := int(btoffset.Level), int(btoffset.BucketNum + btoffset.Delta - 1)
	necessaryBuckets := conf.getNecessaryBuckets(lv, bucketNum)
	bucketTree := newBucketTreeDelta()
	for _, bucketKey := range necessaryBuckets {
		bucketNode, err := fetchBucketNode(snapshotHandler, db.GetDBHandle(), bucketKey)

		if err !=nil {
			logger.Errorf("Failed to fetch BucketNode<%s> From Snapshot, err: %s\n", bucketKey, err)
			return nil, err
		}

		bucketTree.getOrCreateBucketNode(bucketKey)
		if bucketNode != nil && bucketNode.childrenCryptoHash != nil {
			logger.Debugf("<%s>: %d children\n", bucketKey, len(bucketNode.childrenCryptoHash))
			bucketTree.byLevel[bucketKey.level][bucketKey.bucketNumber] = bucketNode
		} else {
			logger.Debugf("<%s>: 0 children\n", bucketKey)
		}
	}

	for level := conf.lowestLevel; level > 0; level-- {
		bucketNodes := bucketTree.getBucketNodesAt(level)

		for _, node := range bucketNodes {

			logger.Debugf("node.bucketKey<%s>", node.bucketKey)

			parentKey := node.bucketKey.getParentKey()
			cryptoHash := node.computeCryptoHash()

			if level == conf.lowestLevel {
				index := parentKey.getChildIndex(node.bucketKey)
				parentBucketNodeOnDisk, err := fetchBucketNode(snapshotHandler, db.GetDBHandle(), parentKey)

				if err !=nil {
					logger.Errorf("<%s>, index<%d> in parent child num: <%d>, err: %s\n",
						node.bucketKey, index, err)
					return nil, err
				}

				if parentBucketNodeOnDisk ==nil {
					logger.Debugf("<%s>, index<%d>, no parentKey<%s> ondisk\n", node.bucketKey, index, parentKey)
				} else {
					cryptoHash = parentBucketNodeOnDisk.childrenCryptoHash[index]
					logger.Debugf("<%s>, index<%d> in parent child num: <%d>\n", node.bucketKey,
						index,	len(parentBucketNodeOnDisk.childrenCryptoHash))
				}
			}

			bucketNodeInMem := bucketTree.getOrCreateBucketNode(parentKey)
			bucketNodeInMem.setChildCryptoHash(node.bucketKey, cryptoHash)
			logger.Debugf("set <%s> hash into parent <%s>, hash<%x>\n",
				node.bucketKey, parentKey, cryptoHash)
		}
	}

	hash := bucketTree.getRootNode().computeCryptoHash()
	logger.Infof("<%d-%d>: root hash <%x>\n", lv, bucketNum, 	hash)
	return hash, nil
}

func byte2BucketTreeOffset(data []byte) (*pb.BucketTreeOffset, error) {
	bucketTreeOffset := &pb.BucketTreeOffset{}
	err := proto.Unmarshal(data, bucketTreeOffset)
	return bucketTreeOffset, err
}

func bucketTreeOffset2Byte(offset *pb.BucketTreeOffset) ([]byte, error)  {
	return offset.Byte()
}