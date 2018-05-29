package gossip_model

import (
	"fmt"
)

//VClock is a type of digest, indicate a partial order nature
type VClock interface {
	Less(VClock) bool
	OutOfRange() bool
}

type scuttlebuttStatus struct {
	VClock
	inner Status
}

type scuttlebuttUpdates struct {
}

func (s *scuttlebuttStatus) GenDigest() Digest { return s.VClock }

func (s *scuttlebuttStatus) MakeUpdate(in Update, v Digest) Update { return in }

func (s *scuttlebuttStatus) Merge(s_in Status) error {

	in, ok := s_in.(*scuttlebuttStatus)
	if !ok {
		panic("Wrong type, not another ScuttlebuttDigest")
	}

	if s.OutOfRange() {
		return fmt.Errorf("Can not merge into wrong clock")
	}

	if in.Less(s.VClock) {
		return fmt.Errorf("Try merge older status")
	}

	s.VClock = in.VClock
	return s.inner.Merge(in.inner)
}
