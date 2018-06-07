package gossip_cat

import (
	model "github.com/abchain/fabric/core/gossip/model"
)

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

// VersionMergerInterface interface
type VersionMergerInterface interface {
	NeedMerge(local *StateVersion, remote *StateVersion) bool
}

// VersionMergerDummy struct as default
type VersionMergerDummy struct {
	VersionMergerInterface
}

// NeedMerge for simply number comparasion, without state
func (m *VersionMergerDummy) NeedMerge(local *StateVersion, remote *StateVersion) bool {
	return local.number < remote.number
}
