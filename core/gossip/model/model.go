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
	MakeUpdate(Digest) Status
}

//Update is the content of a reconciliation, it can be apply to any local peer
//state with PickUp method
type Update interface {
	PickUp(string) Status
	Add(string, Status) Update
}

type Peer struct {
	Id string
	Status
}

//Now we have the model
type Model struct {
	sync.RWMutex
	Peers map[string]*Peer
	self  string
}

func (m *Model) GetSelf() string { return m.self }

//It was up to the user to clear peers
func (m *Model) NewPeer(peer *Peer) bool {
	m.Lock()
	defer m.Unlock()

	_, ok := m.Peers[peer.Id]

	if ok {
		return false
	} else {
		m.Peers[peer.Id] = peer
		return true
	}

}

//It was up to the user to clear peers
func (m *Model) ClearPeer(id string) {
	m.Lock()
	defer m.Unlock()
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

func (m *Model) LocalUpdate(data Status) {

	m.RLock()
	defer m.RUnlock()

	peer, ok := m.Peers[m.self]
	if ok {
		peer.Merge(data)
	} else {
		panic("Self peer is not set")
	}

}

//recv the reconciliation message and update status
func (m *Model) RecvUpdate(r Update) error {

	m.RLock()
	defer m.RUnlock()

	for id, p := range m.Peers {
		if err := p.Merge(r.PickUp(id)); err != nil {
			return err
		}
	}

	return nil
}

//recv the digest from a "pulling" far-end, gen corresponding update
//any digest model can't recognize (not a peer it have known)
//is also returned in the map
func (m *Model) RecvPullDigest(digests map[string]Digest,
	ud Update) (Update, map[string]Digest) {

	m.RLock()
	defer m.RUnlock()

	resident := make(map[string]Digest)

	for id, digest := range digests {
		peer, ok := m.Peers[id]
		if ok {
			ud = ud.Add(id, peer.MakeUpdate(digest))
		} else {
			resident[id] = digest
		}
	}

	return ud, resident
}

func NewGossipModel(self *Peer) *Model {
	return &Model{
		Peers: map[string]*Peer{self.Id: self},
		self:  self.Id,
	}
}
