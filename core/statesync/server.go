package statesync

import (
	pb "github.com/abchain/fabric/protos"
	"github.com/abchain/fabric/core/ledger"
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/looplab/fsm"
)

type stateServer struct {
	*pb.StreamHandler
	parent *stateSyncHandler
	ledger *ledger.Ledger
	correlationId uint64
}

func newStateServer(h *stateSyncHandler) (s *stateServer) {
	s = new(stateServer)
	s.ledger, _ = ledger.GetLedger()
	s.parent = h
	return
}


//---------------------------------------------------------------------------
// 1. acknowledge sync start request
//---------------------------------------------------------------------------
func (server *stateServer) beforeSyncStart(e *fsm.Event) {

	// todo ensure ReleaseSnapshot called finally
	syncMsg := server.LoadSyncMsg(e, nil)

	if syncMsg== nil {
		return
	}

	server.correlationId = syncMsg.CorrelationId

	size, err := server.ledger.GetBlockchainSizeBySnapshot(server.parent.remotePeerIdName())
	if err != nil{
		e.Cancel(err)
		return
	}

	resp := &pb.SyncStateResp{}
	resp.BlockHeight = size

	err = server.SendSyncMsg(e, pb.SyncMsg_SYNC_SESSION_START_ACK, resp)
	if err != nil {
		server.ledger.ReleaseSnapshot(server.parent.remotePeerIdName())
	}
}

//---------------------------------------------------------------------------
// 2. acknowledge query request
//---------------------------------------------------------------------------
func (server *stateServer) beforeQuery(e *fsm.Event) {

	payloadMsg := &pb.SyncStateQuery{}
	syncMsg := server.LoadSyncMsg(e, payloadMsg)
	if syncMsg == nil || server.correlationId != syncMsg.CorrelationId {
		return
	}

	block, err := server.ledger.GetBlockByNumberBySnapshot(server.parent.remotePeerIdName(),
		payloadMsg.BlockHeight)
	if err != nil{
		server.ledger.ReleaseSnapshot(server.parent.remotePeerIdName())
		e.Cancel(err)
		return
	}

	resp := &pb.SyncStateResp{}
	resp.BlockHeight = payloadMsg.BlockHeight
	resp.Statehash = block.StateHash

	err = server.SendSyncMsg(e, pb.SyncMsg_SYNC_SESSION_RESPONSE, resp)
	if err != nil {
		server.ledger.ReleaseSnapshot(server.parent.remotePeerIdName())
	}
}

//---------------------------------------------------------------------------
// 3. acknowledge sync block request
//---------------------------------------------------------------------------
func (server *stateServer) beforeGetBlocks(e *fsm.Event) {
	payloadMsg := &pb.SyncBlockRange{}
	syncMsg := server.LoadSyncMsg(e, payloadMsg)
	if syncMsg == nil || server.correlationId != syncMsg.CorrelationId {
		return
	}

	go server.sendBlocks(e, payloadMsg)
}

//---------------------------------------------------------------------------
// 4. acknowledge sync detal request
//---------------------------------------------------------------------------
func (server *stateServer) beforeGetDeltas(e *fsm.Event) {
	payloadMsg := &pb.SyncStateDeltasRequest{}
	syncMsg := server.LoadSyncMsg(e, payloadMsg)
	if syncMsg == nil || server.correlationId != syncMsg.CorrelationId {
		return
	}

	go server.sendStateDeltas(e, payloadMsg)
}

//---------------------------------------------------------------------------
// 5. acknowledge sync end
//---------------------------------------------------------------------------
func (server *stateServer) beforeSyncEnd(e *fsm.Event) {
	syncMsg := server.LoadSyncMsg(e, nil)
	if syncMsg == nil || server.correlationId != syncMsg.CorrelationId {
		return
	}

	server.ledger.ReleaseSnapshot(server.parent.remotePeerIdName())
}

// sendBlocks sends the blocks based upon the supplied SyncBlockRange over the stream.
func (d *stateServer) sendBlocks(e *fsm.Event, syncBlockRange *pb.SyncBlockRange) {
	logger.Debugf("Sending blocks %d-%d", syncBlockRange.Start, syncBlockRange.End)
	var blockNums []uint64
	if syncBlockRange.Start > syncBlockRange.End {
		// Send in reverse order
		// note that i is a uint so decrementing i below 0 results in an underflow
		// (i becomes uint.MaxValue). Always stop after i == 0
		for i := syncBlockRange.Start; i >= syncBlockRange.End && i <= syncBlockRange.Start; i-- {
			blockNums = append(blockNums, i)
		}
	} else {
		for i := syncBlockRange.Start; i <= syncBlockRange.End; i++ {
			logger.Debugf("Appending to blockNums: %d", i)
			blockNums = append(blockNums, i)
		}
	}

	for _, currBlockNum := range blockNums {
		// Get the Block from
		block, err := d.ledger.GetBlockByNumberBySnapshot(d.parent.remotePeerIdName(), currBlockNum)
		if err != nil {
			logger.Errorf("Error sending blockNum %d: %s", currBlockNum, err)
			break
		}
		// Encode a SyncBlocks into the payload
		syncBlocks := &pb.SyncBlocks{Range: &pb.SyncBlockRange{Start: currBlockNum, End: currBlockNum,
			CorrelationId: syncBlockRange.CorrelationId}, Blocks: []*pb.Block{block}}

		err = d.SendSyncMsg(e, pb.SyncMsg_SYNC_SESSION_BLOCKS, syncBlocks)

		if err != nil  {
			logger.Errorf("Error sending blockNum %d: %s", currBlockNum, err)
			break
		}
	}
}

func (d *stateServer) sendStateDeltas(e *fsm.Event, syncStateDeltasRequest *pb.SyncStateDeltasRequest) {
	logger.Debugf("Sending state deltas for block range %d-%d", syncStateDeltasRequest.Range.Start,
		syncStateDeltasRequest.Range.End)
	var blockNums []uint64
	syncBlockRange := syncStateDeltasRequest.Range
	if syncBlockRange.Start > syncBlockRange.End {
		// Send in reverse order
		for i := syncBlockRange.Start; i >= syncBlockRange.End; i-- {
			blockNums = append(blockNums, i)
		}
	} else {
		//
		for i := syncBlockRange.Start; i <= syncBlockRange.End; i++ {
			logger.Debugf("Appending to blockNums: %d", i)
			blockNums = append(blockNums, i)
		}
	}

	for _, currBlockNum := range blockNums {
		// Get the state deltas for Block from coordinator
		stateDelta, err := d.ledger.GetStateDeltaBySnapshot(d.parent.remotePeerIdName(), currBlockNum)
		if err != nil {
			logger.Errorf("Error sending stateDelta for blockNum %d: %s", currBlockNum, err)
			break
		}
		if stateDelta == nil {
			logger.Warningf("Requested to send a stateDelta for blockNum %d which has been discarded",
				currBlockNum)
			break
		}
		// Encode a SyncStateDeltas into the payload
		stateDeltaBytes := stateDelta.Marshal()
		syncStateDeltas := &pb.SyncStateDeltas{Range: &pb.SyncBlockRange{Start: currBlockNum, End: currBlockNum,
			CorrelationId: syncBlockRange.CorrelationId}, Deltas: [][]byte{stateDeltaBytes}}

		if err := d.SendSyncMsg(e, pb.SyncMsg_SYNC_SESSION_DELTAS, syncStateDeltas); err != nil {
			logger.Errorf("Error sending stateDeltas for blockNum %d: %s", currBlockNum, err)
			break
		}
	}
}

func (server *stateServer) beforeGetSnapshot(e *fsm.Event) {}
