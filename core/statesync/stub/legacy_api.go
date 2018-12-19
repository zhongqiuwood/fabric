package stub

//define a legacy interface for the old consens module of fabric 0.6

import (
	pb "github.com/abchain/fabric/protos"
)

type StateTransfer interface {
	Start() // Start the block transfer go routine
	Stop()  // Stop up the block transfer go routine

	// SyncToTarget attempts to move the state to the given target, returning an error, and whether this target might succeed if attempted at a later time
	SyncToTarget(blockNumber uint64, blockHash []byte, peerIDs []*pb.PeerID) (error, bool)
}
