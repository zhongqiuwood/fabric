package gossip

// A each-peer (all catalogy wide) policy
type peerPolicy struct {
	id *pb.PeerID

	// message state
	activeTime         int64
	digestSeq          uint64
	digestSendTime     int64
	digestResponseTime int64
	updateSendTime     int64
	updateReceiveTime  int64

	// security state
	totalTxCount   int64
	invalidTxCount int64
	invalidTxTime  int64

	// histories
	messageHistories []*PeerHistoryMessage
}
