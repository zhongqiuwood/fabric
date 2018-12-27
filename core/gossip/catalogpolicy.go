package gossip

type CatalogPolicies interface {
	AllowRecvUpdate() bool //the catalogy allow response a pull when accepting pulling from far-end
	PushCount() int        //how many push expected to do in one schedule gossip work
	MaxTrackPeers() int    //how many peers the catalog should track
	PullTimeout() int      //in seconds
}

type catalogPolicyImpl struct {
	allowUpdate   bool
	maxTrackPeers int
	pullTimeout   int
	pushCount     int
}

func NewCatalogPolicyDefault() (ret *catalogPolicyImpl) {

	ret = &catalogPolicyImpl{
		allowUpdate:   true,
		pushCount:     def_PushCount,
		maxTrackPeers: def_TrackPeers,
		pullTimeout:   def_PullTimeout,
	}

	return
}

const (
	def_PullTimeout = 30
	def_PushCount   = 3
	def_TrackPeers  = 128
)

func (p *catalogPolicyImpl) SetPullOnly() {
	p.allowUpdate = false
}

func (p *catalogPolicyImpl) AllowRecvUpdate() bool {
	if p == nil {
		return true
	}

	return p.allowUpdate
}

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

func (p *catalogPolicyImpl) MaxTrackPeers() int {
	if p == nil {
		return def_TrackPeers
	}

	return p.maxTrackPeers

}
