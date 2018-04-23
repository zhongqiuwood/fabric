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
	"github.com/abchain/fabric/protos"
	"github.com/tecbot/gorocksdb"
	"sync"
)

var columnfamilies = []string{
	BlockchainCF, // blocks of the block chain
	StateCF,      // world state
	StateDeltaCF, // open transaction state
	IndexesCF,    // tx uuid -> blockno
	PersistCF,    // persistent per-peer state (consensus)
	StateIndCF,   // state hash -> blockno
}

type openchainCFs struct {
	blockchainCF *gorocksdb.ColumnFamilyHandle
	stateCF      *gorocksdb.ColumnFamilyHandle
	stateDeltaCF *gorocksdb.ColumnFamilyHandle
	indexesCF    *gorocksdb.ColumnFamilyHandle
	persistCF    *gorocksdb.ColumnFamilyHandle
	stateIndCF   *gorocksdb.ColumnFamilyHandle
}

func (c *openchainCFs) feed(cfmap map[string]*gorocksdb.ColumnFamilyHandle) {
	c.blockchainCF = cfmap[BlockchainCF]
	c.stateCF = cfmap[StateCF]
	c.stateDeltaCF = cfmap[StateDeltaCF]
	c.indexesCF = cfmap[IndexesCF]
	c.persistCF = cfmap[PersistCF]
	c.stateIndCF = cfmap[StateIndCF]
}

type OpenchainDB struct {
	baseHandler
	openchainCFs
	sync.RWMutex
	extendedLock chan int //use a channel as locking for opening extend interface
}

var originalDB = &OpenchainDB{}

func (openchainDB *OpenchainDB) open(dbpath string) error {

	cfhandlers := openchainDB.opendb(dbpath, append([]string{"default"}, columnfamilies...), nil)

	if len(cfhandlers) != 7 {
		return errors.New("rocksdb may ruin or not work as expected")
	}

	//0 is default CF and we should close ...
	cfhandlers[0].Destroy()

	//feed cfs
	openchainDB.cfMap = map[string]*gorocksdb.ColumnFamilyHandle{
		BlockchainCF: cfhandlers[1],
		StateCF:      cfhandlers[2],
		StateDeltaCF: cfhandlers[3],
		IndexesCF:    cfhandlers[4],
		PersistCF:    cfhandlers[5],
		StateIndCF:   cfhandlers[6],
	}

	openchainDB.feed(openchainDB.cfMap)

	return nil
}

// override methods with rwlock
func (openchainDB *OpenchainDB) GetValue(cfname string, key []byte) ([]byte, error) {
	openchainDB.RLock()
	defer openchainDB.RUnlock()
	return openchainDB.baseHandler.GetValue(cfname, key)
}

func (openchainDB *OpenchainDB) DeleteKey(cfname string, key []byte) error {
	openchainDB.RLock()
	defer openchainDB.RUnlock()
	return openchainDB.baseHandler.DeleteKey(cfname, key)
}

func (openchainDB *OpenchainDB) PutValue(cfname string, key []byte, value []byte) error {
	openchainDB.RLock()
	defer openchainDB.RUnlock()
	return openchainDB.baseHandler.PutValue(cfname, key, value)
}

func (openchainDB *OpenchainDB) BatchCommit(writeBatch *gorocksdb.WriteBatch) error {
	openchainDB.RLock()
	defer openchainDB.RUnlock()

	return openchainDB.baseHandler.BatchCommit(writeBatch)
}

type ExtHandler struct {
	baseHandler
	openchainCFs

	extendedLock chan int
	snapshot     *gorocksdb.Snapshot
}

func (openchainDB *OpenchainDB) GetExtended() (*ExtHandler, error) {

	openchainDB.RLock()
	defer openchainDB.RUnlock()

	//todo: log this?
	select {
	case openchainDB.extendedLock <- 0:
	default:
		return nil, errors.New("Exceed resource limit for extended handler")
	}

	ret := &ExtHandler{}
	//we basically copy the whole handler except for rwlock
	ret.db = openchainDB.db
	ret.extendedLock = openchainDB.extendedLock
	ret.cfMap = openchainDB.cfMap
	ret.openchainCFs = openchainDB.openchainCFs

	return ret, nil
}

//extend interface
// GetIterator returns an iterator for the given column family
func (e *ExtHandler) Release() {

	if e.snapshot != nil {
		e.db.ReleaseSnapshot(e.snapshot)
		e.snapshot = nil
	}

	//we "absorb" a lock
	select {
	case <-e.extendedLock:
	default:
		dbLogger.Errorf("[%s] Release a extended handler which is not assigned before", printGID)
	}

}

func (e *ExtHandler) Snapshot() {

	if e.snapshot == nil {
		e.snapshot = e.db.NewSnapshot()
	}

}

// Some legacy entries, we make all "fromsnapshot" function becoming simple api (not member func)....
func (e *ExtHandler) FetchBlockchainSizeFromSnapshot() (uint64, error) {

	blockNumberBytes, err := e.GetFromBlockchainCFSnapshot(BlockCountKey)
	if err != nil {
		return 0, err
	}
	var blockNumber uint64
	if blockNumberBytes != nil {
		blockNumber = DecodeToUint64(blockNumberBytes)
	}
	return blockNumber, nil
}

// GetFromBlockchainCFSnapshot get value for given key from column family in a DB snapshot - blockchainCF
func (e *ExtHandler) GetFromBlockchainCFSnapshot(key []byte) ([]byte, error) {

	e.Snapshot()
	return e.getFromSnapshot(e.snapshot, e.blockchainCF, key)
}

// GetStateCFSnapshotIterator get iterator for column family - stateCF. This iterator
// is based on a snapshot and should be used for long running scans, such as
// reading the entire state. Remember to call iterator.Close() when you are done.
func (e *ExtHandler) GetStateCFSnapshotIterator() *gorocksdb.Iterator {

	e.Snapshot()
	return e.getSnapshotIterator(e.snapshot, e.stateCF)
}

// DeleteState delets ALL state keys/values from the DB. This is generally
// only used during state synchronization when creating a new state from
// a snapshot.
func (openchainDB *OpenchainDB) DeleteState() error {

	openchainDB.RLock()
	defer openchainDB.RUnlock()

	err := openchainDB.db.DropColumnFamily(openchainDB.stateCF)
	if err != nil {
		dbLogger.Errorf("Error dropping state CF: %s", err)
		return err
	}
	err = openchainDB.db.DropColumnFamily(openchainDB.stateDeltaCF)
	if err != nil {
		dbLogger.Errorf("Error dropping state delta CF: %s", err)
		return err
	}
	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()
	openchainDB.stateCF, err = openchainDB.db.CreateColumnFamily(opts, StateCF)
	if err != nil {
		dbLogger.Errorf("Error creating state CF: %s", err)
		return err
	}
	openchainDB.stateDeltaCF, err = openchainDB.db.CreateColumnFamily(opts, StateDeltaCF)
	if err != nil {
		dbLogger.Errorf("Error creating state delta CF: %s", err)
		return err
	}

	openchainDB.cfMap[StateCF] = openchainDB.stateCF
	openchainDB.cfMap[StateDeltaCF] = openchainDB.stateDeltaCF

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

func (orgdb *OpenchainDB) FetchBlockFromDB(blockNumber uint64) (*protos.Block, error) {

	orgdb.RLock()
	defer orgdb.RUnlock()

	blockBytes, err := orgdb.get(orgdb.blockchainCF, EncodeBlockNumberDBKey(blockNumber))
	if err != nil {

		return nil, err
	}
	if blockBytes == nil {

		return nil, nil
	}
	block, errUnmarshall := protos.UnmarshallBlock(blockBytes)

	return block, errUnmarshall
}

func (orgdb *OpenchainDB) FetchBlockchainSizeFromDB() (uint64, error) {

	orgdb.RLock()
	defer orgdb.RUnlock()

	bytes, err := orgdb.get(orgdb.blockchainCF, BlockCountKey)
	if err != nil {
		return 0, err
	}
	if bytes == nil {
		return 0, nil
	}
	return DecodeToUint64(bytes), nil
}
