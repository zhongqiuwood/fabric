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
	sync.RWMutex
	Status
}

//gen the "pull" digest to far-end
func (m *Model) GenPullDigest() Digest {

	m.RLock()
	defer m.RUnlock()

	return m.GenDigest()
}

//recv the reconciliation message and update status
func (m *Model) RecvUpdate(r Update) error {

	m.RLock()
	defer m.RUnlock()

	return m.Update(r)
}

//recv the digest from a "pulling" far-end
func (m *Model) RecvPullDigest(digests Digest) Update {

	m.RLock()
	defer m.RUnlock()

	return m.MakeUpdate(digests)
}

func NewGossipModel(self Status) *Model {
	return &Model{Status: self}
}
