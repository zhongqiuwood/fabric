package gossip

import (
	model "github.com/abchain/fabric/core/gossip/model"
	"sync"
	"time"
)

//a "trustable" model provide a verified digest for each status,
//so each status can only be updated under the constraint of the
//provide digest
type TrustableStatus interface {
	model.Status
	UpdateProof(model.Digest) error
}

type trustableModel struct {
	*model.Model
	verifiedDigest map[string]model.Digest
}

func newTrustableModel(h model.ModelHelper, self *model.Peer) (m trustableModel) {

	m.verifiedDigest = make(map[string]model.Digest)
	m.Model = model.NewGossipModel(h, self)
	return
}

func (m *trustableModel) GetVerifiedDigest() map[string]model.Digest {
	if m == nil {
		return nil
	}
	return m.verifiedDigest
}

//under normal handling sequence, UpdateProofDigest is called after any
//RecvPullDigest (so a status will be inited here), and before RecvUpdate
//(the inited status receive a proof)
func (m *trustableModel) UpdateProofDigest(digests map[string]model.Digest) {

	if m == nil {
		return
	}

	m.Model.Lock()
	defer m.Model.Unlock()

	for id, d := range digests {
		m.verifiedDigest[id] = d
		s, ok := m.Peers[id]
		if ok {
			ts, ok := s.Status.(TrustableStatus)
			if !ok {
				panic("Type error, can not convert to trustableStatus")
			}

			ts.UpdateProof(d)
		}
	}
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
