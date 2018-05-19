package statesync

type syncOpt struct {
	startState     []byte
	concurrentMode bool
	specifiedPeers []string
}

func NewSyncOption() *syncOpt {
	return new(syncOpt)
}

//require state sync start at a position not earler than specified one
//set nil to clear option
func (opt *syncOpt) SetSyncStartState(state []byte) {
	opt.startState = state
}

//Enable concurrent mode in state syncing, but it is depend to implement
//to actually take the concurrent mode
func (opt *syncOpt) EnableConcurrentSync(b bool) {
	opt.concurrentMode = b
}

//specified a group of peer ID and restrict syncing among these peers
//set nil to clear option
func (opt *syncOpt) SetSyncTargets(ids []string) {
	opt.specifiedPeers = ids
}
