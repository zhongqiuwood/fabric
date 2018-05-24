package statesync

import (
	"fmt"
	"github.com/abchain/fabric/core/ledger"
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
	pb "github.com/abchain/fabric/protos"
	"github.com/looplab/fsm"
	"golang.org/x/net/context"
)

type syncer struct {
	context.Context
	*pb.StreamHandler
	ledger       *ledger.Ledger
	positionResp chan *pb.SyncStateResp
}

func newSyncer(ctx context.Context, h *stateSyncHandler) (s *syncer) {

	l, _ := ledger.GetLedger()

	s = &syncer{positionResp: make(chan *pb.SyncStateResp),
		ledger:        l,
		StreamHandler: pickStreamHandler(h),
		Context:       ctx,
	}
	return
}

func (sts *syncer) leaveSyncLocating(e *fsm.Event) {}

func (sts *syncer) leaveSyncBlocks(e *fsm.Event) {}

func (sts *syncer) leaveSyncStateSnapshot(e *fsm.Event) {}

func (sts *syncer) leaveSyncStateDeltas(e *fsm.Event) {}

func (sts *syncer) afterSyncResponse(e *fsm.Event) {}

func (sts *syncer) afterSyncBlocks(e *fsm.Event) {}

func (sts *syncer) afterSyncStateSnapshot(e *fsm.Event) {}

func (sts *syncer) afterSyncStateDeltas(e *fsm.Event) {}

func (sts *syncer) getSyncTargetBlockNumber() (uint64, uint64, error) {

	targetBlockNumber := uint64(0)
	endBlockNumber := uint64(0)

	//TODO
	err := sts.SendMessage(nil)

	if err != nil {
		return 0, 0, err
	}

	//logger.Infof("Local Blockchain Height <%d>", targetHeight)

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

		if targetBlockNumber == (start+end)/2 {
			break
		}

		targetBlockNumber = (start + end) / 2
		//TODO
		err = sts.SendMessage(&pb.SyncStateQuery{})

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
