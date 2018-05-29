package statesync

import (
	"fmt"
	"github.com/abchain/fabric/core/ledger"
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
	pb "github.com/abchain/fabric/protos"
	"github.com/looplab/fsm"
	"golang.org/x/net/context"
	"time"
	"github.com/spf13/viper"
	"bytes"
	"github.com/abchain/fabric/core/ledger/statemgmt"
)

type syncer struct {
	context.Context
	*pb.StreamHandler
	parent *stateSyncHandler
	ledger *ledger.Ledger
	positionResp chan *pb.SyncStateResp
	blocksResp chan *pb.SyncBlocks
	deltaResp  chan *pb.SyncStateDeltas
	correlationId uint64

	currentStateBlockNumber uint64
	maxStateDeltas     int    // The maximum number of state deltas to attempt to retrieve before giving up and performing a full state snapshot retrieval
	maxBlockRange      uint64 // The maximum number blocks to attempt to retrieve at once, to prevent from overflowing the peer's buffer
	maxStateDeltaRange uint64 // The maximum number of state deltas to attempt to retrieve at once, to prevent from overflowing the peer's buffer
	RecoverDamage        bool          // Whether state transfer should ever modify or delete existing blocks if they are determined to be corrupted

	DiscoveryThrottleTime time.Duration // The amount of time to wait after discovering there are no connected peers
	stateValid bool // Are we currently operating under the assumption that the state is valid?
	inProgress bool // Set when state transfer is in progress so that the state may not be consistent
	blockVerifyChunkSize uint64        // The max block length to attempt to sync at once, this prevents state transfer from being delayed while the blockchain is validated

	BlockRequestTimeout         time.Duration // How long to wait for a peer to respond to a block request
	StateDeltaRequestTimeout    time.Duration // How long to wait for a peer to respond to a state delta request
	StateSnapshotRequestTimeout time.Duration // How long to wait for a peer to respond to a state snapshot request
}

func newSyncer(ctx context.Context, h *stateSyncHandler) (sts *syncer) {

	l, _ := ledger.GetLedger()

	sts = &syncer{positionResp: make(chan *pb.SyncStateResp),
		ledger:        l,
		StreamHandler: pickStreamHandler(h),
		Context:       ctx,
		parent:        h,
	}

	var err error
	sts.RecoverDamage = viper.GetBool("statetransfer.recoverdamage")

	sts.blockVerifyChunkSize = uint64(viper.GetInt("statetransfer.blocksperrequest"))
	if sts.blockVerifyChunkSize == 0 {
		panic(fmt.Errorf("Must set statetransfer.blocksperrequest to be nonzero"))
	}

	sts.DiscoveryThrottleTime = 1 * time.Second

	sts.BlockRequestTimeout, err = time.ParseDuration(viper.GetString("statetransfer.timeout.singleblock"))
	if err != nil {
		panic(fmt.Errorf("Cannot parse statetransfer.timeout.singleblock timeout: %s", err))
	}
	sts.StateDeltaRequestTimeout, err = time.ParseDuration(viper.GetString("statetransfer.timeout.singlestatedelta"))
	if err != nil {
		panic(fmt.Errorf("Cannot parse statetransfer.timeout.singlestatedelta timeout: %s", err))
	}
	sts.StateSnapshotRequestTimeout, err = time.ParseDuration(viper.GetString("statetransfer.timeout.fullstate"))
	if err != nil {
		panic(fmt.Errorf("Cannot parse statetransfer.timeout.fullstate timeout: %s", err))
	}

	sts.maxStateDeltas = viper.GetInt("statetransfer.maxdeltas")
	if sts.maxStateDeltas <= 0 {
		panic(fmt.Errorf("sts.maxdeltas must be greater than 0"))
	}

	tmp := viper.GetInt("peer.sync.blocks.channelSize")
	if tmp <= 0 {
		panic(fmt.Errorf("peer.sync.blocks.channelSize must be greater than 0"))
	}
	sts.maxBlockRange = uint64(tmp)

	tmp = viper.GetInt("peer.sync.state.deltas.channelSize")
	if tmp <= 0 {
		panic(fmt.Errorf("peer.sync.state.deltas.channelSize must be greater than 0"))
	}
	sts.maxStateDeltaRange = uint64(tmp)

	return
}

func (sts *syncer) InitiateSync() error {

	//---------------------------------------------------------------------------
	// 1. query
	//---------------------------------------------------------------------------
	targetBlockNumber, endBlockNumber, err := sts.getSyncTargetBlockNumber()

	if err != nil {
		return err
	}

	//---------------------------------------------------------------------------
	// 2. switch to the right checkpoint
	//---------------------------------------------------------------------------
	// todo: go to the closest checkpoint to the targetBlockNumber


	//---------------------------------------------------------------------------
	// 3. sync blocks
	//---------------------------------------------------------------------------
	// explicitly go to syncblocks state
	sts.parent.fsmHandler.Event(enterGetBlock)
	err = sts.syncBlocks(targetBlockNumber, endBlockNumber)
	if err != nil {
		return err
	}

	//---------------------------------------------------------------------------
	// 4. sync detals
	//---------------------------------------------------------------------------
	// explicitly go to syncdelta state
	sts.parent.fsmHandler.Event(enterGetDelta)
	err = sts.syncDeltas(targetBlockNumber, endBlockNumber)
	if err != nil {
		return err
	}

	//---------------------------------------------------------------------------
	// 5. sync snapshot
	//---------------------------------------------------------------------------
	// ignore


	//---------------------------------------------------------------------------
	// 6. the end
	//---------------------------------------------------------------------------
	// explicitly go to syncfinish state
	sts.parent.fsmHandler.Event(enterSyncFinish)
	err = sts.SendSyncMsg(nil, pb.SyncMsg_SYNC_SESSION_END, nil)
	if err != nil {
		return err
	}

	return nil
}


//---------------------------------------------------------------------------
// 1. receive start confirmed
//---------------------------------------------------------------------------
func (sts *syncer) afterSyncStartResponse(e *fsm.Event) {
	// implicitly go into synclocating state
	payloadMsg := &pb.SyncStateResp{}
	msg := sts.Load(e, payloadMsg)
	if msg == nil {
		return
	}
	sts.positionResp <- payloadMsg
}

//---------------------------------------------------------------------------
// 2. receive query response
//---------------------------------------------------------------------------
func (sts *syncer) afterQueryResponse(e *fsm.Event) {
	payloadMsg := &pb.SyncStateResp{}
	msg := sts.Load(e, payloadMsg)
	if msg == nil {
		return
	}
	sts.positionResp <- payloadMsg
}

//---------------------------------------------------------------------------
// 3. receive block response
//---------------------------------------------------------------------------
func (sts *syncer) afterSyncBlocks(e *fsm.Event) {

	payloadMsg := &pb.SyncBlocks{}
	msg := sts.Load(e, payloadMsg)
	if msg == nil {
		return
	}
	sts.blocksResp <- payloadMsg
}

//---------------------------------------------------------------------------
// 4. receive delta response
//---------------------------------------------------------------------------
func (sts *syncer) afterSyncStateDeltas(e *fsm.Event) {

	payloadMsg := &pb.SyncStateDeltas{}
	msg := sts.Load(e, payloadMsg)
	if msg == nil {
		return
	}
	sts.deltaResp <- payloadMsg
}

//---------------------------------------------------------------------------
// 5. receive snapshot response
//---------------------------------------------------------------------------
func (sts *syncer) afterSyncStateSnapshot(e *fsm.Event) {

}


func (sts *syncer) getSyncTargetBlockNumber() (uint64, uint64, error) {

	targetBlockNumber := uint64(0)
	endBlockNumber := uint64(0)

	sts.parent.fsmHandler.Event(enterSyncBegin)
	// todo create a fake fsm.Event
	err := sts.SendSyncMsg(nil, pb.SyncMsg_SYNC_SESSION_START, nil)

	if err != nil {
		return 0, 0, err
	}

	select {
	case response, ok := <-sts.positionResp:
		if !ok {
			return 0, 0, fmt.Errorf("had block channel close : %s", err)
		}
		logger.Infof("Remote peer Blockchain Height <%d>", response.BlockHeight)

		endBlockNumber = response.BlockHeight - 1

		// if response.BlockHeight < targetHeight {
		// 	targetHeight = response.BlockHeight
		// }
	case <-sts.Done():
		return 0, 0, fmt.Errorf("Timed out during getSyncTargetBlockNumber")
	}

	var start uint64 = 0
	end := endBlockNumber
	for {

		if targetBlockNumber == (start + end) / 2 {
			break
		}

		targetBlockNumber = (start + end) / 2

		err = sts.SendSyncMsg(nil, pb.SyncMsg_SYNC_SESSION_QUERY,
			&pb.SyncStateQuery{uint32(sts.correlationId), targetBlockNumber})

		if err != nil {
			return 0, 0, err
		}

		select {
		case response, ok := <-sts.positionResp:
			if !ok {
				return 0, 0, fmt.Errorf("had block channel close : %s", err)
			}

			if sts.ledger.GetGlobalState(response.Statehash) != nil {
				start = targetBlockNumber
			} else {
				end = targetBlockNumber
				if targetBlockNumber == 1 {
					return 0, 0, fmt.Errorf("Has no identical state hash")
				}
			}

			logger.Infof("start<%d>, end<%d>, targetBlockNumber<%d>",
				start, end, targetBlockNumber)
		case <-sts.Done():
			return 0, 0, fmt.Errorf("Timed out during get SyncTargetBlockNumber")
		}
	}

	return targetBlockNumber, endBlockNumber, nil
}


func (sts *syncer) syncDeltas(startBlockNumber, endBlockNumber uint64) error {
	logger.Debugf("Attempting to play state forward from %v to block %d",
		sts.parent.remotePeerIdName(),
		endBlockNumber)
	var stateHash []byte

	intermediateBlock := endBlockNumber
	sts.currentStateBlockNumber = startBlockNumber

	block, err := sts.ledger.GetBlockByNumber(startBlockNumber - 1)
	if err != nil {
		return err
	}
	lastStateHash := block.StateHash

	logger.Debugf("Requesting state delta range from %d to %d",
		startBlockNumber, intermediateBlock)

	syncBlockRange := &pb.SyncBlockRange {
		sts.correlationId,
		sts.currentStateBlockNumber,
		intermediateBlock,
	}
	payloadMsg := &pb.SyncStateDeltasRequest{Range: syncBlockRange}
	err = sts.SendSyncMsg(nil, pb.SyncMsg_SYNC_SESSION_GET_DELTAS, payloadMsg)

	if err != nil {
		return fmt.Errorf("Received an error while trying to get " +
			"the state deltas for blocks %d through %d from %s",
			sts.currentStateBlockNumber, intermediateBlock, sts.parent.remotePeerIdName())
	}

	for sts.currentStateBlockNumber < intermediateBlock {
		select {
		case deltaMessage, ok := <-sts.deltaResp:
			if !ok {
				return fmt.Errorf("Was only able to recover to block number %d when desired to recover to %d",
					sts.currentStateBlockNumber, endBlockNumber)
			}

			if deltaMessage.Range.Start != sts.currentStateBlockNumber ||
				deltaMessage.Range.End < deltaMessage.Range.Start ||
				deltaMessage.Range.End > endBlockNumber {
				return fmt.Errorf(
					"Received a state delta either in the wrong order (backwards) or " +
						"not next in sequence, aborting, start=%d, end=%d",
					deltaMessage.Range.Start, deltaMessage.Range.End)
			}

			for _, delta := range deltaMessage.Deltas {
				umDelta := &statemgmt.StateDelta{}
				if err := umDelta.Unmarshal(delta); nil != err {
					return fmt.Errorf("Received a corrupt state delta from %s : %s",
						sts.parent.remotePeerIdName(), err)
				}
				sts.ledger.ApplyStateDelta(deltaMessage, umDelta)

				success := false

				testBlock, err := sts.ledger.GetBlockByNumber(sts.currentStateBlockNumber)

				if err != nil {
					logger.Warningf("Could not retrieve block %d, though it should be present",
						deltaMessage.Range.End)
				} else {

					stateHash, err = sts.ledger.GetCurrentStateHash()
					if err != nil {
						logger.Warningf("Could not compute state hash for some reason: %s", err)
					}
					logger.Debugf("Played state forward from %s to block %d with StateHash (%x), " +
						"block has StateHash (%x)",
						sts.parent.remotePeerIdName(),
						deltaMessage.Range.End, stateHash,
						testBlock.StateHash)

					if bytes.Equal(testBlock.StateHash, stateHash) {
						success = true
						//add new statehash, and we omit errors
						sts.ledger.AddGlobalState(lastStateHash, stateHash)
						lastStateHash = stateHash
					}
				}

				if !success {
					if sts.ledger.RollbackStateDelta(deltaMessage) != nil {
						sts.stateValid = false
						return fmt.Errorf(
							"played state forward according to %s, but the state hash did not match, " +
								"failed to roll back, invalidated state",
							sts.parent.remotePeerIdName())
					}
					return fmt.Errorf("Played state forward according to %s, " +
						"but the state hash did not match, rolled back", sts.parent.remotePeerIdName())

				}

				if sts.ledger.CommitStateDelta(deltaMessage) != nil {
					sts.stateValid = false
					return fmt.Errorf("Played state forward according to %s, " +
						"hashes matched, but failed to commit, invalidated state", sts.parent.remotePeerIdName())
				}

				logger.Debugf("Moved state from %d to %d", sts.currentStateBlockNumber,
					sts.currentStateBlockNumber + 1)
				sts.currentStateBlockNumber++

				if sts.currentStateBlockNumber == endBlockNumber {
					logger.Debugf("Caught up to block %d", sts.currentStateBlockNumber)
					return nil
				}
			}

		case <-time.After(sts.StateDeltaRequestTimeout):
			logger.Warningf("Timed out during state delta recovery from %s", sts.parent.remotePeerIdName())
			return fmt.Errorf("timed out during state delta recovery from %s", sts.parent.remotePeerIdName())
		}
	}

	logger.Debugf("State is now valid at block %d and hash %x", sts.currentStateBlockNumber, stateHash)
	return err
}


func (sts *syncer) syncBlocks(highBlock, lowBlock uint64) error {

	logger.Debugf("Syncing blocks from %d to %d", highBlock, lowBlock)
	blockCursor := highBlock
	var block *pb.Block
	var err error

	intermediateBlock := blockCursor + 1

	for {

		if intermediateBlock == blockCursor + 1 {

			if sts.maxBlockRange > blockCursor {
				// Don't underflow
				intermediateBlock = 0
			} else {
				intermediateBlock = blockCursor - sts.maxBlockRange
			}

			if intermediateBlock < lowBlock {
				intermediateBlock = lowBlock
			}
			logger.Debugf("Requesting block range from %d to %d",
				blockCursor, intermediateBlock)

			err = sts.SendSyncMsg(nil, pb.SyncMsg_SYNC_SESSION_GET_BLOCKS,
				&pb.SyncBlockRange{
					sts.correlationId,
					blockCursor,
					intermediateBlock,})
			if err != nil {
				return err
			}
		}

		if nil != err {
			logger.Warningf("Failed to get blocks from %d to %d from %v: %s",
				blockCursor, lowBlock, sts.parent.remotePeerIdName(), err)
			return err
		}

		select {
		case syncBlockMessage, ok := <-sts.blocksResp:

			if !ok {
				return fmt.Errorf("Channel closed before we could finish reading")
			}

			if syncBlockMessage.Range.Start < syncBlockMessage.Range.End {
				// If the message is not replying with blocks backwards, we did not ask for it
				return fmt.Errorf("Received a block with wrong (increasing) order from %v, aborting",
					sts.parent.remotePeerIdName())
			}

			var i int
			for i, block = range syncBlockMessage.Blocks {
				// It no longer correct to get duplication or out of range blocks, so we treat this as an error
				if syncBlockMessage.Range.Start-uint64(i) != blockCursor {
					return fmt.Errorf("Received a block out of order, indicating a buffer " +
						"overflow or other corruption: start=%d, end=%d, wanted %d",
						syncBlockMessage.Range.Start, syncBlockMessage.Range.End, blockCursor)
				}

				logger.Debugf("Putting block %d to with PreviousBlockHash %x and StateHash %x",
					blockCursor, block.PreviousBlockHash, block.StateHash)

				sts.ledger.PutBlock(blockCursor, block)

				if blockCursor == lowBlock {
					logger.Debugf("Successfully synced from block %d to block %d", highBlock, lowBlock)
					return nil
				}
				blockCursor--

			}
		case <-time.After(sts.BlockRequestTimeout):
			return fmt.Errorf("Had block sync request to %s time out", sts.parent.remotePeerIdName())
		}
	}

	if nil != block {
		logger.Debugf("Returned from sync with block %d, state hash %x", blockCursor, block.StateHash)
	} else {
		logger.Debugf("Returned from sync with no new blocks")
	}

	return err
}



func (sts *syncer) leaveSyncLocating(e *fsm.Event) {}

func (sts *syncer) leaveSyncBlocks(e *fsm.Event) {}

func (sts *syncer) leaveSyncStateSnapshot(e *fsm.Event) {}

func (sts *syncer) leaveSyncStateDeltas(e *fsm.Event) {}
