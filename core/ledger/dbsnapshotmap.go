package ledger

import (
	"github.com/abchain/fabric/core/db"
	"sync"
	"github.com/abchain/fabric/protos"
	"github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/core/ledger/statemgmt"
)

type dbsnapshotMap struct {
	sync.RWMutex
	ssmap    map[string]*db.DBSnapshot
}

func newSnapshotMap() *dbsnapshotMap {
	res := &dbsnapshotMap{}
	res.ssmap = make(map[string]*db.DBSnapshot)
	return res
}

//========================================================
//================== state syncing =======================
//========================================================
func (sm *dbsnapshotMap) GetBlockByNumber(syncid string, blockNumber uint64) (*protos.Block, error) {
	size, err := sm.GetBlockchainSize(syncid)

	if err != nil {
		return nil, err
	}

	if blockNumber >= size {
		return nil, ErrOutOfBounds
	}
	return sm.fetchBlockFromDB(syncid, blockNumber)
}

// GetBlockchainSize returns number of blocks in blockchain
func (sm *dbsnapshotMap) GetBlockchainSize(id string) (uint64, error) {
	bytes, err := sm.getFromBlockchainCF(id, blockCountKey)
	if err != nil {
		return 0, err
	}
	if bytes == nil {
		return 0, nil
	}
	return util.DecodeToUint64(bytes), nil
}

func  (sm *dbsnapshotMap) GetStateDelta(id string, blockNumber uint64) (*statemgmt.StateDelta, error) {
	stateDeltaBytes, err := sm.getFromStateDeltaCF(id, util.EncodeUint64(blockNumber))
	if err != nil {
		return nil, err
	}
	if stateDeltaBytes == nil {
		return nil, nil
	}
	stateDelta := statemgmt.NewStateDelta()
	stateDelta.Unmarshal(stateDeltaBytes)
	return stateDelta, nil
}
//==================================================


func (sm *dbsnapshotMap) getSnapshot(id string) (*db.DBSnapshot, error) {

	sm.RLock()
	defer sm.RUnlock()
	dbs, ok := sm.ssmap[id]

	if !ok {
		// todo: return error if hit amount limit
		dbs = db.GetDBHandle().GetSnapshot()
		sm.ssmap[id] = dbs
	}

	return dbs, nil
}

func (sm *dbsnapshotMap) query(id string, cfName string, key []byte) ([]byte, error) {

	dbs, err := sm.getSnapshot(id)
	if err != nil {
		return nil, err
	}
	return 	dbs.GetValue(cfName, key)
}

func (sm *dbsnapshotMap) remove(id string) {

	sm.RLock()
	defer sm.RUnlock()
	delete(sm.ssmap, id)
}


func (sm *dbsnapshotMap) getFromBlockchainCF(id string, key []byte) ([]byte, error) {

	dbs, err := sm.getSnapshot(id)
	if err != nil {
		return nil, err
	}

	return dbs.GetFromBlockchainCFSnapshot(key)
}

func (sm *dbsnapshotMap) getFromStateDeltaCF(id string, key []byte) ([]byte, error) {

	dbs, err := sm.getSnapshot(id)
	if err != nil {
		return nil, err
	}

	return dbs.GetFromStateDeltaCFSnapshot(key)
}


func (sm *dbsnapshotMap)fetchRawBlockFromDB(id string, blockNumber uint64) (*protos.Block, error) {

	blockBytes, err := sm.getFromBlockchainCF(id, util.EncodeUint64(blockNumber))
	if err != nil {
		return nil, err
	}
	if blockBytes == nil {
		return nil, nil
	}
	blk, err := protos.UnmarshallBlock(blockBytes)
	if err != nil {
		return nil, err
	}

	//panic for "legacy" blockbytes, which include transactions data ...
	if blk.Version == 0 && blk.Transactions != nil && !compatibleLegacy {
		panic("DB for blockchain still use legacy bytes, need upgrade first")
	}

	return blk, nil
}


func (sm *dbsnapshotMap)fetchBlockFromDB(id string, blockNumber uint64) (blk *protos.Block, err error) {

	blk, err = sm.fetchRawBlockFromDB(id, blockNumber)
	if err != nil {
		return
	}

	if blk == nil {
		return nil, nil
	}

	if blk.Transactions == nil {
		//blk.Transactions = fetchTxsFromDB(blk.Txids)
	} else if blk.Txids == nil {
		//only for compatible with the legacy block bytes
		blk.Txids = make([]string, len(blk.Transactions))
		for i, tx := range blk.Transactions {
			blk.Txids[i] = tx.Txid
		}
	}

	return
}
