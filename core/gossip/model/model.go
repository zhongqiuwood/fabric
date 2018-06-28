package gossip_model

import (
	"sync"
)

//Base concept in gossip protocol
type Digest interface {
	Gossip_Digest()
}

//Update is the content of a reconciliation
type UpdateIn interface {
	Gossip_UpdateIn()
}

type UpdateOut interface {
	Gossip_UpdateOut()
}

type Status interface {
	GenDigest() Digest
	Update(UpdateIn) error
	MakeUpdate(Digest) UpdateOut
}

//Now we have the model
type Model struct {
	sync.RWMutex
	Status
}

//gen the "push" digest to far-end
func (m *Model) GenPullDigest() Digest {

	m.RLock()
	defer m.RUnlock()

	return m.GenDigest()
}

//recv the reconciliation message and update status
func (m *Model) RecvUpdate(r UpdateIn) error {

	m.RLock()
	defer m.RUnlock()

	return m.Update(r)
}

//recv the digest from a "pulling" far-end
func (m *Model) RecvPullDigest(digests Digest) UpdateOut {

	m.RLock()
	defer m.RUnlock()

	return m.MakeUpdate(digests)
}

func NewGossipModel(self Status) *Model {
	return &Model{Status: self}
}
