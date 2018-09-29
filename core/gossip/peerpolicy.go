package gossip

import (
	"errors"
	"sync"
	"time"
)

type tokenBucket struct {
	lastT    int64
	quota    int
	Interval int64 //in unix() time unit (second)
}

func NewTokenBucket(inv int) *tokenBucket {
	return &tokenBucket{Interval: int64(inv)}
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
func (t *tokenBucket) TestIn(in int, limit int) bool {

	t.update(limit)
	t.quota = t.quota + in
	return t.quota > limit
}

//can be minus
func (t *tokenBucket) Available(limit int) int {

	//skip update, save some expense
	if t.quota == 0 {
		return limit
	}

	t.update(limit)
	return limit - t.quota
}

type PeerPolicies interface {
	GetId() string
	IsPolicyViolated() error
	RecvUpdate(int)
	RecordViolation(e error)
	AllowRecvUpdate() bool
	PushUpdateQuota() int
	PushUpdate(int)
	ScoringPeer(int, uint)
	ResetIntervals(int)
}

type unlimitPolicies string

func (s unlimitPolicies) GetId() string { return string(s) }

func (unlimitPolicies) IsPolicyViolated() error { return nil }

func (unlimitPolicies) RecvUpdate(int) {}

func (unlimitPolicies) RecordViolation(e error) {}

func (unlimitPolicies) AllowRecvUpdate() bool { return true }

func (unlimitPolicies) PushUpdateQuota() int { return DefaultMsgSize }

func (unlimitPolicies) PushUpdate(int) {}

func (unlimitPolicies) ScoringPeer(int, uint) {}

func (unlimitPolicies) ResetIntervals(int) {}

type peerPolicies struct {
	sync.RWMutex
	id               string
	score            int64
	scoreWeightTotal uint64
	errorLimit       int
	messageLimit     int
	recvQuota        tokenBucket
	recvUpdateBytes  int64
	sentQuota        tokenBucket
	sentUpdateBytes  int64
	errorControl     tokenBucket
	sysError         error
	totalError       uint
}

func (p *peerPolicies) recvmsgLimit() int {
	return p.messageLimit * int(p.recvQuota.Interval)
}

func (p *peerPolicies) sentmsgLimit() int {
	return p.messageLimit * int(p.sentQuota.Interval)
}

func (p *peerPolicies) GetId() string { return p.id }

func (p *peerPolicies) IsPolicyViolated() error {
	p.RLock()
	defer p.RUnlock()
	return p.sysError
}

func (p *peerPolicies) RecvUpdate(b int) {
	p.Lock()
	defer p.Unlock()
	p.recvUpdateBytes = int64(b) + p.recvUpdateBytes
	logger.Debugf("receive %d bytes from peer %s , now %v", b, p.id, p.recvUpdateBytes)
	if p.recvQuota.TestIn(b, p.recvmsgLimit()) {
		p.Unlock()
		p.RecordViolation(errors.New("recv bytes exceed quota"))
		p.Lock()
	}
}

func (p *peerPolicies) RecordViolation(e error) {
	p.Lock()
	defer p.Unlock()
	logger.Errorf("Record a violation of policy on peer %s: %s", p.id, e)
	p.totalError = p.totalError + 1
	if p.errorControl.TestIn(1, p.errorLimit) {
		p.sysError = errors.New("Total error count exceed tolerance limit")
	}
}

func (p *peerPolicies) AllowRecvUpdate() bool {
	p.RLock()
	defer p.RUnlock()
	return p.recvQuota.Available(p.recvmsgLimit()) > 0
}

func (p *peerPolicies) PushUpdateQuota() int {
	p.Lock()
	defer p.Unlock()
	return p.sentQuota.Available(p.sentmsgLimit())
}

func (p *peerPolicies) PushUpdate(b int) {
	p.Lock()
	defer p.Unlock()
	p.sentUpdateBytes = int64(b) + p.sentUpdateBytes
	logger.Debugf("pushed %d bytes to peer %s, now %v", b, p.id, p.sentUpdateBytes)
	p.sentQuota.TestIn(b, p.sentmsgLimit())
}

func (p *peerPolicies) ScoringPeer(score int, weight uint) {
	p.Lock()
	defer p.Unlock()

	p.score = p.score + int64(score*int(weight))
	p.scoreWeightTotal = p.scoreWeightTotal + uint64(weight)
}

func (p *peerPolicies) ResetIntervals(inv int) {
	p.Lock()
	defer p.Unlock()

	inv64 := int64(inv)

	p.recvQuota.Interval = inv64
	p.sentQuota.Interval = inv64
	p.errorControl.Interval = inv64
}

var DefaultInterval = int64(60)
var DefaultErrorLimit = 3             //peer is allowed to violate policy 3 times/min
var DefaultMsgSize = 1024 * 1024 * 16 //msg limit is 16kB/s

func NewPeerPolicy(id string) PeerPolicies {

	if disablePeerPolicy {
		return unlimitPolicies(id)
	}

	ret := &peerPolicies{id: id}

	ret.errorLimit = DefaultErrorLimit
	ret.messageLimit = DefaultMsgSize
	ret.recvQuota.Interval = DefaultInterval
	ret.sentQuota.Interval = DefaultInterval
	ret.errorControl.Interval = DefaultInterval

	return ret
}
