package gossip

import (
	"time"
)

type tokenBucket struct {
	lastT    int64
	quota    int
	Interval int64 //in unix() time unit (second)
}

//when an effect is occur before this interval, we simply "forgot" it
//this is useful for avoiding some integer overflowing case
const forgotInterval = 3600

func (t *tokenBucket) update(limit int) {

	now := time.Now().Unix()
	passed := now - t.lastT
	if passed > forgotInterval {
		t.quota = 0
	} else {
		maxquota := int64(limit) * passed / t.Interval
		if maxquota > int64(t.quota) {
			t.quota = 0
		} else {
			t.quota = t.quota - int(maxquota)
		}
	}

	t.lastT = now
}

//add some value and return if the incoming is exceed
func (t *tokenBucket) testIn(in int, limit int) bool {

	t.update(limit)
	t.quota = t.quota + in
	return t.quota > limit
}

//can be minus
func (t *tokenBucket) avaiable(limit int) int {

	//skip update, save some expense
	if t.quota == 0 {
		return limit
	}

	t.update(limit)
	return limit - t.quota
}

type catalogPPOImpl struct {
	errorLimit      int
	messageLimit    int
	recvQuota       tokenBucket
	recvUpdateBytes int64
	sentQuota       tokenBucket
	sentUpdateBytes int64
	errorControl    tokenBucket
	sysError        bool
	totalError      uint
}

func (p *catalogPPOImpl) IsPolicyViolated() bool {
	return p.sysError
}

func (p *catalogPPOImpl) RecordViolation() {
	p.totalError = p.totalError + 1
	p.sysError = p.errorControl.testIn(1, p.errorLimit)
}

func (p *catalogPPOImpl) AllowRecvUpdate() bool {
	return p.recvQuota.avaiable(p.messageLimit) > 0
}

func (p *catalogPPOImpl) RecvUpdate(b int) {
	p.recvUpdateBytes = int64(b) + p.recvUpdateBytes
	if !p.recvQuota.testIn(b, p.messageLimit) {
		p.RecordViolation()
	}
}

func (p *catalogPPOImpl) PushUpdateQuota() int {
	return p.sentQuota.avaiable(p.messageLimit)
}

func (p *catalogPPOImpl) PushUpdate(b int) {
	p.sentUpdateBytes = int64(b) + p.sentUpdateBytes
	p.sentQuota.testIn(b, p.messageLimit)
}

func (p *catalogPPOImpl) Stop() {}

var DefaultInterval = int64(60)
var DefaultErrorLimit = 3                                    //peer is allowed to violate policy 3 times/min
var DefaultMsgSize = 1024 * 1024 * 16 * int(DefaultInterval) //msg limit is 16kB/s

func NewCatalogPeerPolicy() (ret *catalogPPOImpl) {
	ret = &catalogPPOImpl{}
	return
}

func NewCatalogPeerPolicyDefault() (ret *catalogPPOImpl) {
	ret = NewCatalogPeerPolicy()

	ret.errorLimit = DefaultErrorLimit
	ret.messageLimit = DefaultMsgSize
	ret.recvQuota.Interval = DefaultInterval
	ret.sentQuota.Interval = DefaultInterval
	ret.errorControl.Interval = DefaultInterval
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
