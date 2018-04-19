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
	"errors"
	flog "github.com/abchain/fabric/flogging"
	"github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"github.com/tecbot/gorocksdb"
	"os"
	"sync"
)

var dbLogger = logging.MustGetLogger("db")
var printGID = flog.GoRDef
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
	StateIndCF,   // state hash -> blockno
}

type OpenchainDB struct {
	baseHandler
	sync.RWMutex
	blockchainCF *gorocksdb.ColumnFamilyHandle
	stateCF      *gorocksdb.ColumnFamilyHandle
	stateDeltaCF *gorocksdb.ColumnFamilyHandle
	indexesCF    *gorocksdb.ColumnFamilyHandle
	persistCF    *gorocksdb.ColumnFamilyHandle
	stateIndCF   *gorocksdb.ColumnFamilyHandle
}

var originalDB = &OpenchainDB{}

func (openchainDB *OpenchainDB) open(dbpath string) error {

	cfhandlers := openchainDB.opendb(dbPath, columnfamilies)

	if len(cfhandlers) != 7 {
		return errors.New("rocksdb may ruin or not work as expected")
	}

	//0 is default CF and we should close ...
	cfhandlers[0].Destroy()

	//feed cf
	openchainDB.blockchainCF = cfHandlers[1]
	openchainDB.stateCF = cfHandlers[2]
	openchainDB.stateDeltaCF = cfHandlers[3]
	openchainDB.indexesCF = cfHandlers[4]
	openchainDB.persistCF = cfHandlers[5]
	openchainDB.stateIndCF = cfHandlers[6]

	openchainDB.cfMap = map[string]*gorocksdb.ColumnFamilyHandle{
		BlockchainCF: openchainDB.blockchainCF,
		StateCF:      openchainDB.stateCF,
		StateDeltaCF: openchainDB.stateDeltaCF,
		IndexesCF:    openchainDB.indexesCF,
		PersistCF:    openchainDB.persistCF,
		StateIndCF:   openchainDB.stateIndCF,
	}

	return nil
}

// override methods with rwlock
func (openchainDB *OpenchainDB) GetValue(cfname string, key []byte) ([]byte, error) {
	openchainDB.RLock()
	defer openchainDB.RUnlock()
	return openchainDB.BaseHandler.GetValue(cfname, key)
}

func (openchainDB *OpenchainDB) DeleteKey(cfname string, key []byte) error {
	openchainDB.RLock()
	defer openchainDB.RUnlock()
	return openchainDB.BaseHandler.DeleteKey(cfname, key)
}

func (openchainDB *OpenchainDB) PutValue(cfname string, key []byte, value []byte) error {
	openchainDB.RLock()
	defer openchainDB.RUnlock()
	return openchainDB.BaseHandler.PutValue(cfname, key, value)
}

// Some legacy entries ....

// GetFromBlockchainCFSnapshot get value for given key from column family in a DB snapshot - blockchainCF
func (openchainDB *OpenchainDB) GetFromBlockchainCFSnapshot(snapshot *gorocksdb.Snapshot, key []byte) ([]byte, error) {
	return openchainDB.getFromSnapshot(snapshot, openchainDB.blockchainCF, key)
}

// GetStateCFSnapshotIterator get iterator for column family - stateCF. This iterator
// is based on a snapshot and should be used for long running scans, such as
// reading the entire state. Remember to call iterator.Close() when you are done.
func (openchainDB *OpenchainDB) GetStateCFSnapshotIterator(snapshot *gorocksdb.Snapshot) *gorocksdb.Iterator {
	return openchainDB.getSnapshotIterator(snapshot, openchainDB.stateCF)
}

func (openchainDB *OpenchainDB) DumpGlobalState() {
}

func (openchainDB *OpenchainDB) OpenCheckPoint(dbName string, blockNumber uint64, statehash string) error {
	openchainDB.closeDBHandler()
	targetDir := getDBPath(dbName)
	err := os.RemoveAll(targetDir)
	if err == nil {
		openchainDB.produceDbByCheckPoint(dbName, blockNumber, statehash, columnfamilies)
		openchainDB.closeDBHandler()
		openchainDB.open(dbName, columnfamilies)
	} else {
		dbLogger.Errorf("[%s] Error: %s", printGID, err)
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

	blockBytes, err := orgdb.GetValue(BlockchainCF, EncodeBlockNumberDBKey(blockNumber))
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
