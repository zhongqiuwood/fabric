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

type peerPolicies struct {
	sync.RWMutex
	id              string
	errorLimit      int
	messageLimit    int
	recvQuota       tokenBucket
	recvUpdateBytes int64
	sentQuota       tokenBucket
	sentUpdateBytes int64
	errorControl    tokenBucket
	sysError        error
	totalError      uint
}

func (p *peerPolicies) isPolicyViolated() error {
	p.RLock()
	defer p.RUnlock()
	return p.sysError
}

func (p *peerPolicies) recvUpdate(b int) {
	p.Lock()
	defer p.Unlock()
	p.recvUpdateBytes = int64(b) + p.recvUpdateBytes
	if !p.recvQuota.testIn(b, p.messageLimit) {
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
	if p.errorControl.testIn(1, p.errorLimit) {
		p.sysError = errors.New("Total error count exceed tolerance limit")
	}
}

func (p *peerPolicies) AllowRecvUpdate() bool {
	p.RLock()
	defer p.RUnlock()
	return p.recvQuota.avaiable(p.messageLimit) > 0
}

func (p *peerPolicies) PushUpdateQuota() int {
	p.Lock()
	defer p.Unlock()
	return p.sentQuota.avaiable(p.messageLimit)
}

func (p *peerPolicies) PushUpdate(b int) {
	p.Lock()
	defer p.Unlock()
	p.sentUpdateBytes = int64(b) + p.sentUpdateBytes
	p.sentQuota.testIn(b, p.messageLimit)
}

var DefaultInterval = int64(60)
var DefaultErrorLimit = 3                                    //peer is allowed to violate policy 3 times/min
var DefaultMsgSize = 1024 * 1024 * 16 * int(DefaultInterval) //msg limit is 16kB/s

func newPeerPolicy(id string) (ret *peerPolicies) {
	ret = &peerPolicies{id: id}

	ret.errorLimit = DefaultErrorLimit
	ret.messageLimit = DefaultMsgSize
	ret.recvQuota.Interval = DefaultInterval
	ret.sentQuota.Interval = DefaultInterval
	ret.errorControl.Interval = DefaultInterval
	return
}
