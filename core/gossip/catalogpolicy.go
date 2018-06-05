package gossip

type CatalogPolicies interface {
	PushCount() int   //how many push expected to do in one schedule gossip work
	PullTimeout() int //in seconds
}

type catalogPolicyImpl struct {
	maxDigestRobust int
	maxDigestPeers  int
	historyExpired  int64 // seconds
	updateExpired   int64 // seconds
	pullTimeout     int
	pushCount       int
}

func NewCatalogPolicyDefault() (ret *catalogPolicyImpl) {

	ret = &catalogPolicyImpl{
		pullTimeout: def_PullTimeout,
		pushCount:   def_PushCount,
	}

	ret.maxDigestRobust = 100
	ret.maxDigestPeers = 100

	ret.historyExpired = 600 // 10 minutes
	ret.updateExpired = 30   // 30 seconds

	return
}

const (
	def_PullTimeout = 30
	def_PushCount   = 3
)

func (p *catalogPolicyImpl) PullTimeout() int {
	if p == nil {
		return def_PullTimeout
	}

	return p.pullTimeout
}

func (p *catalogPolicyImpl) PushCount() int {
	if p == nil {
		return def_PushCount
	}

	return p.pushCount
}
