package gossip

type catalogPolicy struct {
	maxDigestRobust int
	maxDigestPeers  int
	historyExpired  int64 // seconds
	updateExpired   int64 // seconds
}

func newCatalogPolicy() (ret *catalogPolicy) {

	ret = &catalogPolicy{}

	ret.maxDigestRobust = 100
	ret.maxDigestPeers = 100

	ret.historyExpired = 600 // 10 minutes
	ret.updateExpired = 30   // 30 seconds

	return
}
