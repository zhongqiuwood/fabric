package gossip_model

import (
	"sync"
)

//Base concept in gossip protocol
type Digest interface{}

//Update is the content of a reconciliation
type Update interface {
	Gossip_IsUpdateIn() bool
}

type Status interface {
	GenDigest() Digest
	Update(Update) error
	MakeUpdate(Digest) Update
}

//Now we have the model
type Model struct {
	sync.Mutex
	s Status
}

func (m *Model) Status() Status { return m.s }

//gen the "pull" digest to far-end
func (m *Model) GenPullDigest() Digest {

	m.Lock()
	defer m.Unlock()

	return m.s.GenDigest()
}

//recv the reconciliation message and update status
func (m *Model) RecvUpdate(r Update) error {

	m.Lock()
	defer m.Unlock()

	return m.s.Update(r)
}

//recv the digest from a "pulling" far-end
func (m *Model) RecvPullDigest(digests Digest) Update {

	m.Lock()
	defer m.Unlock()

	return m.s.MakeUpdate(digests)
}

func NewGossipModel(self Status) *Model {
	return &Model{s: self}
}
