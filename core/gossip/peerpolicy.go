package gossip

// A each-peer (all catalogy wide) policy
type peerPolicy struct {
	//id *pb.PeerID

	// message state
	activeTime         int64
	digestSeq          uint64
	digestSendTime     int64
	digestResponseTime int64
	updateSendTime     int64
	updateReceiveTime  int64

	maxMessageSize int64 // bytes

	// histories
	//messageHistories []*PeerHistoryMessage
}

func newPeerPolicy() (ret *peerPolicy) {

	ret = &peerPolicy{}

	ret.maxMessageSize = 100 * 1024 * 1024 // 100MB
	return
}
