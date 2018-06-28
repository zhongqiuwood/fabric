package gossip_model

import (
	"fmt"
	"strings"
	"testing"
)

type testVClock int

func (va testVClock) Less(v_in VClock) bool {
	if v_in == nil {
		return false
	}

	return va < transVClock(v_in)
}

func transVClock(i VClock) testVClock {
	v, ok := i.(testVClock)

	if !ok {
		panic("wrong vclock type")
	}

	return v
}

//act as both update and status
type testPeerStatus struct {
	data map[string]int
}

func transUpdate(i ScuttlebuttPeerUpdate) *testPeerStatus {
	v, ok := i.(*testPeerStatus)

	if !ok {
		panic("wrong update type")
	}

	return v
}

func (s *testPeerStatus) From() VClock {

	if len(s.data) == 0 {
		return nil
	}

	var min int
	for _, v := range s.data {
		if min == 0 || v < min {
			min = v
		}
	}

	return testVClock(min)
}

func (s *testPeerStatus) To() VClock {

	if len(s.data) == 0 {
		return nil
	}

	var max int
	for _, v := range s.data {
		if v > max {
			max = v
		}
	}

	return testVClock(max)
}

func (s *testPeerStatus) PickFrom(v_in VClock) ScuttlebuttPeerUpdate {
	ret := &testPeerStatus{make(map[string]int)}
	vclk := transVClock(v_in)

	for k, v := range s.data {

		if v > int(vclk) {
			ret.data[k] = v
		}
	}

	return s
}

func (s *testPeerStatus) Update(u_in ScuttlebuttPeerUpdate, g UpdateIn) error {

	u := transUpdate(u_in)

	for k, v := range u.data {
		vold, ok := s.data[k]
		if ok && v < vold {
			return fmt.Errorf("update give old version for key %s: [local %d vs %d]", k, vold, v)
		}

		s.data[k] = v
	}

	return nil
}

type testStatus struct {
}

func (*testStatus) GenPullDigest() Digest { return nil }

func (*testStatus) RecvUpdate(UpdateIn) error { return nil }

func (*testStatus) RecvPullDigest(Digest) UpdateOut { return nil }

func (*testStatus) NewPeer(string) ScuttlebuttPeerStatus { return &testPeerStatus{make(map[string]int)} }

func (*testStatus) MissedUpdate(string, ScuttlebuttPeerUpdate, UpdateIn) error { return nil }

type testPeer struct {
	id string
}

func TestScuttlebuttModel(t *testing.T) {

}
