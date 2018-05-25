package gossip_model

import (
	"sync"
)

//Base concept in gossip protocol
type Digest interface{}

//Any peer we known owns a state (in fact, different peer may share the same status.
//in that case, method MUST prepare for being called with the same update data)
//state can be merged
type Status interface {
	GenDigest() Digest
	Merge(Status) error
	MakeUpdate(Update, Digest) Update
}

//Update is the content of a reconciliation, it can be apply to any local peer
//state with PickUp method
type Update interface {
	PickUp(Digest) Status
}

type Peer struct {
	Id string
	Status
}

//Now we have the model
type Model struct {
	sync.RWMutex
	ModelHelper
	Peers map[string]*Peer
}

//And we need some helper for model
type ModelHelper interface {
	AcceptPeer(string, Digest) (*Peer, error)
}

//It was up to the user to clear peers
func (m *Model) ClearPeer(id string) {
	delete(m.Peers, id)
}

//gen the "push" digest to far-end
func (m *Model) GenPullDigest() (ret map[string]Digest) {

	m.RLock()
	defer m.RUnlock()

	ret = make(map[string]Digest)

	for _, p := range m.Peers {
		ret[p.Id] = p.GenDigest()
	}

	return ret
}

//recv the reconciliation message and update status
func (m *Model) RecvUpdate(r Update) {

	m.RLock()
	defer m.RUnlock()

	for _, p := range m.Peers {
		p.Merge(r.PickUp(p.GenDigest()))

	}
}

//recv the digest from a "pulling" far-end, gen corresponding update
func (m *Model) RecvPullDigest(digests map[string]Digest) (ud Update) {

	m.RLock()
	defer m.RUnlock()

	for id, digest := range digests {
		peer, ok := m.Peers[id]
		if !ok {
			//check if we can add new one
			m.RUnlock()
			m.Lock()
			s, err := m.AcceptPeer(id, digest)
			if err == nil {
				m.Peers[id] = s
			}
			m.Unlock()
			m.RLock()
		} else {
			ud = peer.MakeUpdate(ud, digest)
		}
	}

	return
}

func NewGossipModel(h ModelHelper, self *Peer) *Model {
	return &Model{
		ModelHelper: h,
		Peers:       map[string]*Peer{self.Id: self},
	}
}
