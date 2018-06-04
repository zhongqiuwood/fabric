package gossip

import (
	model "github.com/abchain/fabric/core/gossip/model"
	"sync"
	"time"
)

//a "trustable" model provide a verified digest for each status,
//so each status can only be updated under the constraint of the
//provide digest
type trustableStatus interface {
	model.Status
	UpdateProof(model.Digest) error
}

type trustableDigest interface {
	model.Digest
	Merge(trustableDigest) trustableDigest
}

type trustableModel struct {
	*model.Model
	VerifiedDigest map[string]trustableDigest
}

func (m *trustableModel) UpdateVerifiedDigest(digests map[string]trustableDigest) {

	model.Model.Lock()
	defer model.Model.Unlock()

	for id, d := range digests {
		target, ok := m.VerifiedDigest[id]
		if !ok {
			target = d
		} else {
			target = target.Merge(d)
		}

		m.VerifiedDigest[id] = target
		s, ok := m.Peers[id]
		if ok {
			ts, ok := s.(trustableStatus)
			if !ok {
				panice("Type error, can not convert to trustableStatus")
			}

			ts.UpdateProof(target)
		}
	}
}

//a standard vclock use seq
type standardVClock struct {
	oor bool
	n   uint64
}

func (a *standardVClock) Less(b_in model.VClock) bool {
	b, ok := b_in.(*standardVClock)
	if !ok {
		panic("Wrong type, not standardVClock")
	}

	if b.OutOfRange() {
		return false
	}

	return a.n < b.n
}

func (v *standardVClock) OutOfRange() bool {
	return v.oor
}

var globalSeq uint64
var globalSeqLock sync.Mutex

func getGlobalSeq() uint64 {

	globalSeqLock.Lock()
	defer globalSeqLock.Unlock()

	ref := uint64(time.Now().Unix())
	if ref > globalSeq {
		globalSeq = ref

	} else {
		globalSeq++
	}

	return globalSeq
}
