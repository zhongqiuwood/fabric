package gossip

// VersionMerger interface
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
