package gossip

type CatalogPolicies interface {
	PushCount() int      //how many push expected to do in one schedule gossip work
	MaxTrackPeers() int  //how many peers the catalog should track
	MaxPackedPeers() int //how many digest can a gossip msg include
	PullTimeout() int    //in seconds
}

type catalogPolicyImpl struct {
	maxPackedPeers int
	maxTrackPeers  int
	pullTimeout    int
	pushCount      int
}

func NewCatalogPolicyDefault() (ret *catalogPolicyImpl) {

	ret = &catalogPolicyImpl{
		pullTimeout:    def_PullTimeout,
		pushCount:      def_PushCount,
		maxPackedPeers: def_PackedPeers,
		maxTrackPeers:  def_TrackPeers,
	}

	return
}

const (
	def_PullTimeout = 30
	def_PushCount   = 3
	def_TrackPeers  = 128
	def_PackedPeers = 64
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

func (p *catalogPolicyImpl) MaxTrackPeers() int {
	if p == nil {
		return def_TrackPeers
	}

	return p.maxTrackPeers

}

func (p *catalogPolicyImpl) MaxPackedPeers() int {
	if p == nil {
		return def_PackedPeers
	}

	return p.maxPackedPeers
}
