package gossip

// TxQuota struct
type TxQuota struct {
	maxDigestRobust int
	maxDigestPeers  int
	maxMessageSize  int64 // bytes
	historyExpired  int64 // seconds
	updateExpired   int64 // seconds
}
