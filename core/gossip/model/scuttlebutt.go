package gossip_model

import (
	"fmt"
)

//VClock is a type of digest, indicate a partial order nature
type VClock interface {
	Less(VClock) bool
	OutOfRange() bool
}

//scuttlebutt status work with vclock digest, it was in essential an interval,
//and a partial stack of Status (with only Merge method)
type ScuttlebuttStatus interface {
	PickFrom(VClock) ScuttlebuttStatus
	Merge(ScuttlebuttStatus) error
}

type scuttlebuttStatus struct {
	to VClock
	ScuttlebuttStatus
}

func (s *scuttlebuttStatus) GenDigest() Digest { return s.to }

func (s *scuttlebuttStatus) MakeUpdate(d_in Digest) Status {

	v, ok := d_in.(VClock)
	if !ok {
		panic("Wrong type, not vclock")
	}

	//could not return out-of-range status
	if v.OutOfRange() || s.to.Less(v) {
		return nil
	}

	return &scuttlebuttStatus{v, s.PickFrom(v)}

}

func (s *scuttlebuttStatus) Merge(s_in Status) error {

	//NIL not consider as error, we can always merge a nil status
	if s_in == nil {
		return nil
	}

	in, ok := s_in.(*scuttlebuttStatus)
	if !ok {
		panic("Wrong type, not scuttlebuttStatus")
	}

	if in.to.OutOfRange() {
		return fmt.Errorf("Can not merge into wrong clock")
	}

	if in.to.Less(s.to) {
		return fmt.Errorf("Try merge older status")
	}

	ret := s.ScuttlebuttStatus.Merge(in.ScuttlebuttStatus)
	if ret != nil {
		return ret
	}

	//progress our clock
	s.to = in.to
	return nil
}

type ScuttlebuttUpdate interface {
	Merge(*scuttlebuttStatus) *scuttlebuttStatus
}

type scuttlebuttUpdates struct {
	//TODO: an associative data structure for searching is need
	s map[string]*scuttlebuttStatus
	ScuttlebuttUpdate
}

func (u *scuttlebuttUpdates) PickUp(id string) Status {

	if u == nil {
		return nil
	}

	return u.s[id]
}

func (u *scuttlebuttUpdates) Add(id string, s_in Status) Update {

	if s_in == nil {
		return u
	}

	ud_s, ok := s_in.(*scuttlebuttStatus)
	if !ok {
		panic("Wrong type, not scuttlebuttStatus")
	}

	//the update must first be handle by ScuttlebuttUpdate interface
	//(e.g: restrict its size) and then being merged
	u.s[id] = u.Merge(ud_s)
	return u
}
