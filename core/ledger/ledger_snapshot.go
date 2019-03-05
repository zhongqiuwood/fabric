package ledger

import (
	"fmt"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/core/ledger/statemgmt/state"
	"github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/protos"
	"sync"
)

type LedgerSnapshot struct {
	l *Ledger
	*db.DBSnapshot
}

func (sledger *LedgerSnapshot) GetParitalRangeIterator(offset *protos.SyncOffset) (statemgmt.PartialRangeIterator, error) {

	partialInf := sledger.l.state.GetDividableState()
	if partialInf == nil {
		return nil, fmt.Errorf("State not support")
	}

	return partialInf.GetPartialRangeIterator(sledger.DBSnapshot)
}

func (sledger *LedgerSnapshot) GetBlockByNumber(blockNumber uint64) (*protos.Block, error) {
	size, err := sledger.GetBlockchainSize()

	if err != nil {
		return nil, err
	}

	if blockNumber >= size {
		return nil, ErrOutOfBounds
	}

	blockBytes, err := sledger.GetFromBlockchainCFSnapshot(encodeBlockNumberDBKey(blockNumber))
	if err != nil {
		return nil, err
	}

	blk, err := bytesToBlock(blockBytes)

	if err != nil {
		return nil, err
	}

	blk = finishFetchedBlock(blk)

	return blk, nil
}

func (sledger *LedgerSnapshot) GetBlockchainSize() (uint64, error) {

	bytes, err := sledger.GetFromBlockchainCFSnapshot(blockCountKey)
	if err != nil {
		return 0, err
	}
	return bytesToBlockNumber(bytes), nil
}

func (sledger *LedgerSnapshot) GetStateDelta(blockNumber uint64) (*statemgmt.StateDelta, error) {

	size, err := sledger.GetBlockchainSize()

	if err != nil {
		return nil, err
	}

	if blockNumber >= size {
		return nil, ErrOutOfBounds
	}

	stateDeltaBytes, err := sledger.GetFromStateDeltaCFSnapshot(util.EncodeUint64(blockNumber))
	if err != nil {
		return nil, err
	}
	if len(stateDeltaBytes) == 0 {
		return nil, nil
	}
	stateDelta := statemgmt.NewStateDelta()
	err = stateDelta.Unmarshal(stateDeltaBytes)
	return stateDelta, err
}

func (sledger *LedgerSnapshot) GetStateSnapshot() (*state.StateSnapshot, error) {
	blockHeight, err := sledger.GetBlockchainSize()
	if err != nil {
		return nil, err
	}
	if 0 == blockHeight {
		return nil, fmt.Errorf("Blockchain has no blocks, cannot determine block number")
	}
	return sledger.l.state.GetSnapshot(blockHeight-1, sledger.DBSnapshot)
}

// ----- will be deprecated -----
// GetStateSnapshot returns a point-in-time view of the global state for the current block. This
// should be used when transferring the state from one peer to another peer. You must call
// stateSnapshot.Release() once you are done with the snapshot to free up resources.
func (ledger *Ledger) GetStateSnapshot() (*state.StateSnapshot, error) {
	snapshotL := ledger.CreateSnapshot()
	blockHeight, err := snapshotL.GetBlockchainSize()
	if err != nil {
		snapshotL.Release()
		return nil, err
	}
	if 0 == blockHeight {
		snapshotL.Release()
		return nil, fmt.Errorf("Blockchain has no blocks, cannot determine block number")
	}
	return ledger.state.GetSnapshot(blockHeight-1, snapshotL.DBSnapshot)
}

//maintain a series of snapshot for state querying
type ledgerHistory struct {
	sync.RWMutex
	db               *db.OpenchainDB
	snapshotInterval int
	beginIntervalNum uint64
	currentNum       uint64
	current          *db.DBSnapshot
	sns              []*db.DBSnapshot
}

const defaultSnapshotTotal = 8
const defaultSnapshotInterval = 16

func initNewLedgerSnapshotManager(db *db.OpenchainDB, blkheight uint64, config *ledgerConfig) *ledgerHistory {
	lsm := new(ledgerHistory)
	//TODO: read config
	lsm.snapshotInterval = defaultSnapshotInterval
	lsm.sns = make([]*db.DBSnapshot, defaultSnapshotTotal)

	lsm.beginIntervalNum = blkheight / uint64(lsm.snapshotInterval)
	lsm.currentNum = blkheight
	lsm.current = db.GetSnapshot()
	lsm.db = db

	return lsm
}

func (lh *ledgerHistory) Release() {
	for _, sn := range lh.sns {
		sn.Release()
	}
}

func (lh *ledgerHistory) GetCurrentSnapshot() *db.DBSnapshot {
	lh.RLock()
	defer lh.RUnlock()

	return lh.current
}

func (lh *ledgerHistory) GetSnapshot(blknum uint64) (*db.DBSnapshot, uint64) {
	lh.RLock()
	defer lh.RUnlock()

	sni, blkn := lh.historyIndex(blknum)
	return lh.sns[sni], blkn
}

func (lh *ledgerHistory) historyIndex(blknum uint64) (int, uint64) {

	sec := blknum / uint64(lh.snapshotInterval)

	if sec < lh.beginIntervalNum {
		return int(lh.beginIntervalNum % uint64(len(lh.sns))), lh.beginIntervalNum * uint64(lh.snapshotInterval)
	} else {
		return int(sec % uint64(len(lh.sns))), sec * uint64(lh.snapshotInterval)
	}
}

func (lh *LedgerHistory) Update(blknum uint64) {
	lh.Lock()
	defer lh.Unlock()

	if blknum <= lh.currentNum {
		ledgerLogger.Errorf("Try update a less blocknumber %d (current %d), not accept", blknum, lh.currentNum)
		return
	}

	//can keep this snapshot
	if lh.currentNum%uint64(lh.snapshotInterval) == 0 {
		sec := blknum / uint64(lh.snapshotInterval)
		indx := int(sec % uint64(len(lh.sns)))
		lh.sns[indx] = lh.current
		ledgerLogger.Debugf("Cache snapshot of %d at %d", lh.currentNum, indx)
		if lh.beginIntervalNum+uint64(len(lh.sns)) <= sec {
			lh.beginIntervalNum = sec - uint64(len(lh.sns)+1)
			ledgerLogger.Debugf("Move update begin to %d", lh.beginIntervalNum)
		}

	}
	lh.current = lh.db.GetSnapshot()
}
