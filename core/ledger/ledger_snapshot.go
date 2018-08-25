package ledger

import (
	"fmt"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/core/ledger/statemgmt/state"
	"github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/protos"
)

// Ledger - the struct for openchain ledger
type LedgerSnapshot struct {
	*db.DBSnapshot
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
	stateDelta.Unmarshal(stateDeltaBytes)
	return stateDelta, nil
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
