package gossip

import (
	"time"
)

type tokenBucket struct {
	lastT    int64
	quota    int
	Interval int64 //in unix() time unit (second)
}

func (t *tokenBucket) update() {

	now := time.Now().Unix()
	passed := now - t.lastT
	if passed > t.Interval {
		t.quota = 0
	} else {
		t.quota = int(int64(t.quota) * passed / t.Interval)
	}
	t.lastT = now
}

//add some value and return if the incoming is exceed
func (t *tokenBucket) testIn(in int, limit int) bool {

	t.update()
	t.quota = t.quota + in
	return t.quota > limit
}

func (t *tokenBucket) avaiable(limit int) int {

	//skip update, save some expense
	if t.quota == 0 {
		return limit
	}

	t.update()
	return limit - t.quota
}

type catalogPPOImpl struct {
	messageLimit    int
	recvQuota       tokenBucket
	recvUpdateBytes int64
	sentQuota       tokenBucket
	sentUpdateBytes int64
}

func (p *catalogPPOImpl) AllowPushUpdate() bool {
	return p.recvQuota.avaiable(p.messageLimit) > 0
}

func (p *catalogPPOImpl) RecvUpdate(b int) {
	p.recvUpdateBytes = int64(b) + p.recvUpdateBytes
}

func (p *catalogPPOImpl) PushUpdate(b int) {
	//currently we infact not use pushUpdate (just obey the pull req and
	//this should be safe)
	p.sentUpdateBytes = int64(b) + p.sentUpdateBytes
}

func NewCatalogPeerPolicyDefault() (ret *catalogPPOImpl) {
	ret = &catalogPPOImpl{}
	return
}

type PeerPolicies interface {
	UpdateBytes(int)
	AllowPush() bool
}

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
