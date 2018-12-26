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
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"fmt"
	"github.com/tecbot/gorocksdb"
)

// db or snapshot Iterator
type stateCfIterator interface {
	Seek(key []byte)
	Next()
	Close()
	Valid() bool
	Key() *gorocksdb.Slice
	Value() *gorocksdb.Slice
}

// fetch one DataNode FromDB by a dataKey
func fetchDataNodeFromDB(odb *db.OpenchainDB, dataKey *dataKey) (*dataNode, error) {
	nodeBytes, err := odb.GetValue(db.StateCF, dataKey.getEncodedBytes())
	if err != nil {
		return nil, err
	}
	if nodeBytes == nil {
		logger.Debug("nodeBytes from db is nil")
	} else if len(nodeBytes) == 0 {
		logger.Debug("nodeBytes from db is an empty array")
	}
	// key does not exist
	if nodeBytes == nil {
		return nil, nil
	}
	return unmarshalDataNode(dataKey, nodeBytes), nil
}

func fetchBucketNodeFromDB(odb *db.OpenchainDB, bucketKey *bucketKey) (*bucketNode, error) {
	nodeBytes, err := odb.GetValue(db.StateCF, bucketKey.getEncodedBytes())
	if err != nil {
		return nil, err
	}
	if nodeBytes == nil {
		return nil, nil
	}
	return unmarshalBucketNode(bucketKey, nodeBytes), nil
}


// fetch a DataNode array belone to a Lowest Level bucketKey FromDB
func fetchDataNodesFromDBFor(itr stateCfIterator, bucketKey *bucketKey) (dataNodes, error) {

	if bucketKey.level != conf.GetLowestLevel() {
		return nil, fmt.Errorf("Invalid bucketKey")
	}

	minimumDataKeyBytes := minimumPossibleDataKeyBytesFor(bucketKey)

	logger.Debugf("Fetching from DB data nodes for bucket [%s], minimumDataKeyBytes<%x>",
		bucketKey, minimumDataKeyBytes)

	var dataNodes dataNodes
	itr.Seek(minimumDataKeyBytes)

	for ; itr.Valid(); itr.Next() {

		// making a copy of key-value bytes because, underlying key bytes are reused by itr.
		// no need to free slices as iterator frees memory when closed.
		keyBytes := statemgmt.Copy(itr.Key().Data())
		valueBytes := statemgmt.Copy(itr.Value().Data())

		dataKey := newDataKeyFromEncodedBytes(keyBytes)
		logger.Debugf("Retrieved data key [%s] from DB for bucket [%s]", dataKey, bucketKey)
		if !dataKey.getBucketKey().equals(bucketKey) {
			logger.Debugf("Data key [%s] from DB does not belong to bucket = [%s]. Stopping further iteration and returning results [%v]", dataKey, bucketKey, dataNodes)
			return dataNodes, nil
		}
		dataNode := unmarshalDataNode(dataKey, valueBytes)

		logger.Debugf("Data node [%s] from DB belongs to bucket = [%s]. Including the key in results...", dataNode, bucketKey)
		dataNodes = append(dataNodes, dataNode)
	}
	logger.Debugf("Returning results [%v]", dataNodes)
	logger.Debugf("[%s], <%d> data nodes: [%v]", bucketKey, len(dataNodes), dataNodes)
	return dataNodes, nil
}

func fetchBucketNode(snapshotHandler *db.DBSnapshot, odb *db.OpenchainDB, bucketKey *bucketKey) (*bucketNode, error) {

	var nodeBytes []byte
	var err error

	if snapshotHandler != nil {
		nodeBytes, err = snapshotHandler.GetFromSnapshot(db.StateCF, bucketKey.getEncodedBytes())
	} else {
		return fetchBucketNodeFromDB(odb, bucketKey)
	}

	if err != nil {
		return nil, err
	}
	if nodeBytes == nil {
		return nil, nil
	}
	return unmarshalBucketNode(bucketKey, nodeBytes), nil
}

func produceStateDeltaFromDB(start, end int, itr stateCfIterator) (*statemgmt.StateDelta, error) {

	var dataNodes dataNodes = nil
	for i := start; i <= end ; i++ {

		detal, err := fetchDataNodesFromDBFor(itr, &bucketKey{conf.lowestLevel, i})

		if err != nil {
			return nil, err
		}

		if dataNodes == nil {
			dataNodes = detal
		} else {
			dataNodes = append(dataNodes, detal...)
		}
	}

	stateDelta := statemgmt.NewStateDelta()
	for _, dataNode := range dataNodes {
		ccdId, key := dataNode.getKeyElements()

		logger.Infof("<%s> stateDelta.Set: [%s][%s]: %s", dataNode.dataKey.bucketKey,
			ccdId, key, dataNode.getValue())
		stateDelta.Set(ccdId, key, dataNode.getValue(), nil)
	}

	return stateDelta, nil
}

func DumpDataNodes() (dataNodes, error) {
	itr := db.GetDBHandle().GetIterator(db.StateCF)
	defer itr.Close()
	minimumDataKeyBytes := minimumPossibleDataKeyBytesFor(newBucketKeyAtLowestLevel(1))

	logger.Infof("minimumDataKeyBytes [%x]", minimumDataKeyBytes)

	var dataNodes dataNodes
	itr.Seek(minimumDataKeyBytes)

	idx := 1
	for ; itr.Valid(); itr.Next() {

		// making a copy of key-value bytes because, underlying key bytes are reused by itr.
		// no need to free slices as iterator frees memory when closed.
		keyBytes := statemgmt.Copy(itr.Key().Data())
		valueBytes := statemgmt.Copy(itr.Value().Data())

		dataKey := newDataKeyFromEncodedBytes(keyBytes)
		dataNode := unmarshalDataNode(dataKey, valueBytes)

		logger.Debugf("Data node[%d]: [%s]", idx, dataNode)
		idx++
		dataNodes = append(dataNodes, dataNode)
	}
	logger.Debugf("Returning results [%v]", dataNodes)
	return dataNodes, nil
}