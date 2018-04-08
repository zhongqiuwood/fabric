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

package db

import (

	"github.com/op/go-logging"
	"github.com/tecbot/gorocksdb"
	"os"
	"github.com/abchain/fabric/dbg"
	"github.com/abchain/fabric/protos"
)

var dbLogger = logging.MustGetLogger("db")
var rocksDBLogLevelMap = map[string]gorocksdb.InfoLogLevel{
	"debug": gorocksdb.DebugInfoLogLevel,
	"info":  gorocksdb.InfoInfoLogLevel,
	"warn":  gorocksdb.WarnInfoLogLevel,
	"error": gorocksdb.ErrorInfoLogLevel,
	"fatal": gorocksdb.FatalInfoLogLevel}

var columnfamilies = []string{
	BlockchainCF, // blocks of the block chain
	StateCF,      // world state
	StateDeltaCF, // open transaction state
	IndexesCF,    // tx uuid -> blockno
	PersistCF,    // persistent per-peer state (consensus)
}

type OpenchainDB struct {
	BaseHandler
}

var originalDB = createOriginalDB()
func createOriginalDB() *OpenchainDB {
	db := &OpenchainDB{}
	db.cfMap = make(map[string]*gorocksdb.ColumnFamilyHandle)
	return db
}

func (openchainDB *OpenchainDB) open(dbname string, cf []string) {

	dbPath := getDBPath(dbname)
	openchainDB.dbName = dbname
	cfhandlers := openchainDB.opendb(dbPath, cf)
	openchainDB.feedCfHandlers(cfhandlers)
}


func (openchainDB *OpenchainDB) feedCfHandlers(cfHandlers []*gorocksdb.ColumnFamilyHandle) {
	openchainDB.cfMap[BlockchainCF] = cfHandlers[1]
	openchainDB.cfMap[StateCF] = cfHandlers[2]
	openchainDB.cfMap[StateDeltaCF] = cfHandlers[3]
	openchainDB.cfMap[IndexesCF] = cfHandlers[4]
	openchainDB.cfMap[PersistCF] = cfHandlers[5]
}

// GetFromBlockchainCFSnapshot get value for given key from column family in a DB snapshot - blockchainCF
func (openchainDB *OpenchainDB) GetFromBlockchainCFSnapshot(snapshot *gorocksdb.Snapshot, key []byte) ([]byte, error) {
	return openchainDB.getFromSnapshot(snapshot, openchainDB.getCFByName(BlockchainCF), key)
}

// GetStateCFSnapshotIterator get iterator for column family - stateCF. This iterator
// is based on a snapshot and should be used for long running scans, such as
// reading the entire state. Remember to call iterator.Close() when you are done.
func (openchainDB *OpenchainDB) GetStateCFSnapshotIterator(snapshot *gorocksdb.Snapshot) *gorocksdb.Iterator {
	return openchainDB.getSnapshotIterator(snapshot, openchainDB.getCFByName(StateCF))
}

// GetSnapshot returns a point-in-time view of the DB. You MUST call snapshot.Release()
// when you are done with the snapshot.
func (openchainDB *OpenchainDB) GetSnapshot() *gorocksdb.Snapshot {
	return openchainDB.dbHandler.NewSnapshot()
}

func (openchainDB *OpenchainDB) ReleaseSnapshot(snapshot *gorocksdb.Snapshot) {
	openchainDB.dbHandler.ReleaseSnapshot(snapshot)
}

func (openchainDB *OpenchainDB) OpenCheckPoint(dbName string, statehash string) error {
	openchainDB.closeDBHandler()
	targetDir := getDBPath(dbName)
	err := os.RemoveAll(targetDir)
	dbg.ChkErr(err)
	if err == nil {
		openchainDB.produceDbByCheckPoint(dbName, statehash, columnfamilies)
		openchainDB.closeDBHandler()
		openchainDB.open(dbName, columnfamilies)
	}
	return err
}

// DeleteState delets ALL state keys/values from the DB. This is generally
// only used during state synchronization when creating a new state from
// a snapshot.
func (openchainDB *OpenchainDB) DeleteDbState() error {
	err := openchainDB.dbHandler.DropColumnFamily(openchainDB.getCFByName(StateCF))
	if err != nil {
		dbLogger.Errorf("Error dropping state CF: %s", err)
		return err
	}
	err = openchainDB.dbHandler.DropColumnFamily(openchainDB.getCFByName(StateDeltaCF))
	if err != nil {
		dbLogger.Errorf("Error dropping state delta CF: %s", err)
		return err
	}
	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()
	openchainDB.cfMap[StateCF], err = openchainDB.dbHandler.CreateColumnFamily(opts, StateCF)
	if err != nil {
		dbLogger.Errorf("Error creating state CF: %s", err)
		return err
	}
	openchainDB.cfMap[StateDeltaCF], err = openchainDB.dbHandler.CreateColumnFamily(opts, StateDeltaCF)
	if err != nil {
		dbLogger.Errorf("Error creating state delta CF: %s", err)
		return err
	}
	return nil
}


//func (orgdb *OpenchainDB) getTxids(blockNumber uint64) []string {
//
//	block, err := orgdb.FetchBlockFromDB(blockNumber, false)
//	if err != nil {
//		dbg.Errorf("Error Fetch BlockFromDB by blockNumber<%d>. Err: %s", blockNumber, err)
//		return nil
//	}
//
//	if block == nil {
//		dbg.Errorf("No such a block, blockNumber<%d>. Err: %s", blockNumber)
//		return nil
//	}
//
//	return block.Txids
//}


func (orgdb *OpenchainDB) FetchBlockFromDB(blockNumber uint64, dbversion uint32) (*protos.Block, error) {

	blockBytes, err := orgdb.GetValue(BlockchainCF,	EncodeBlockNumberDBKey(blockNumber))
	if err != nil {

		return nil, err
	}
	if blockBytes == nil {

		return nil, nil
	}
	block, errUnmarshall := protos.UnmarshallBlock(blockBytes)

	if block != nil && dbversion == 1 {
		// fetch transcations from txdb since v1
		block.Transactions = GetGlobalDBHandle().getTransactionFromDB(block.Txids)
	}

	return block, errUnmarshall
}

func (orgdb *OpenchainDB) FetchBlockchainSizeFromDB() (uint64, error) {
	bytes, err := orgdb.GetValue(BlockchainCF, BlockCountKey)
	if err != nil {
		return 0, err
	}
	if bytes == nil {
		return 0, nil
	}
	return DecodeToUint64(bytes), nil
}

func (orgdb *OpenchainDB) FetchBlockchainSizeFromSnapshot(snapshot *gorocksdb.Snapshot) (uint64, error) {
	blockNumberBytes, err := orgdb.GetFromBlockchainCFSnapshot(snapshot, BlockCountKey)
	if err != nil {
		return 0, err
	}
	var blockNumber uint64
	if blockNumberBytes != nil {
		blockNumber = DecodeToUint64(blockNumberBytes)
	}
	return blockNumber, nil
}