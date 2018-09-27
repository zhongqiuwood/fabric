package gossip_model

import (
	"fmt"
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

	if i == nil {
		return 0
	}

	v, ok := i.(testVClock)

	if !ok {
		panic("wrong vclock type")
	}

	return v
}

type testStatus struct {
	t     *testing.T
	gdata map[string]int
}

func transStatus(i ScuttlebuttStatus) *testStatus {
	v, ok := i.(*testStatus)

	if !ok {
		panic("wrong status type")
	}

	return v
}

func (*testStatus) GenDigest() Digest { return nil }

func (*testStatus) Update(Update) error { return nil }

func (*testStatus) MakeUpdate(Digest) Update { return nil }

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

func (s *testPeerStatus) To() VClock {

	var max int
	for _, v := range s.data {
		if v > max {
			max = v
		}
	}

	return testVClock(max)
}

func (s *testPeerStatus) PickFrom(id string, v_in VClock, u Update) (ScuttlebuttPeerUpdate, Update) {
	ret := &testPeerStatus{make(map[string]int)}
	vclk := transVClock(v_in)

	for k, v := range s.data {

		if v > int(vclk) {
			ret.data[k] = v
		}
	}

	return s, u
}

func (s *testPeerStatus) OutDate(ScuttlebuttStatus) {}

func (s *testPeerStatus) Update(id string, u_in ScuttlebuttPeerUpdate, gs_in ScuttlebuttStatus) error {

	u := transUpdate(u_in)
	gs := transStatus(gs_in)

	for k, v := range u.data {
		vold, ok := s.data[k]
		if ok && v < vold {
			return fmt.Errorf("update give old version for key %s: [local %d vs %d]", k, vold, v)
		}

		s.data[k] = v

		vold, ok = gs.gdata[k]
		if !ok || v > vold {
			gs.gdata[k] = v
		}
	}

	return nil
}

func (*testStatus) NewPeer(string) ScuttlebuttPeerStatus { return &testPeerStatus{make(map[string]int)} }

func (*testStatus) MissedUpdate(string, ScuttlebuttPeerUpdate) error { return nil }

func (*testStatus) RemovePeer(string, ScuttlebuttPeerStatus) {}

type TestPeer interface {
	CreateModel() *Model
	LocalUpdate(ks []string)
	DumpPeers() []string
	DumpSelfData() map[string]int
	DumpData() map[string]int
}

type testPeer struct {
	*testStatus
	self *testPeerStatus
	ss   *scuttlebuttStatus
}

func (p *testPeer) CreateModel() *Model { return NewGossipModel(p.ss) }
func (p *testPeer) DumpPeers() (ret []string) {

	for id, _ := range p.ss.Peers {
		if id != "" {
			ret = append(ret, id)
		} else {
			ret = append(ret, p.ss.SelfID)
		}
	}

	return ret
}

func (p *testPeer) DumpSelfData() map[string]int {
	return p.self.data
}

func (p *testPeer) DumpData() map[string]int {
	return p.gdata
}

func (p *testPeer) LocalUpdate(ks []string) {

	now := int(transVClock(p.self.To())) + 1

	for _, k := range ks {
		if gv, ok := p.gdata[k]; ok && gv >= now {
			now = gv + 1
		}
		p.self.data[k] = now
		p.gdata[k] = now
		now++
	}
}

func (p *testPeer) SetExtended() {
	p.ss.Extended = true
}

func NewTestPeer(t *testing.T, id string) *testPeer {

	s := &testStatus{
		t:     t,
		gdata: make(map[string]int),
	}

	self := &testPeerStatus{
		data: make(map[string]int),
	}

	ss := NewScuttlebuttStatus(s)

	ss.SetSelfPeer(id, self)

	return &testPeer{s, self, ss}
}

func DumpUpdate(u_in Update) (ret map[string]map[string]int) {

	u, ok := u_in.(*scuttlebuttUpdate)

	if !ok {
		panic("wrong type, not scuttlebuttUpdate")
	}

	ret = make(map[string]map[string]int)

	for id, s_in := range u.u {
		s, ok := s_in.(*testPeerStatus)

		if !ok {
			panic("wrong type, not testPeerStatus")
		}

		data := make(map[string]int)

		for k, v := range s.data {
			data[k] = v
		}

		ret[id] = data
	}

	return

}

func DumpScuttlebutt(m *Model) *scuttlebuttStatus {

	ret, ok := m.Status.(*scuttlebuttStatus)
	if !ok {
		panic("wrong code, not scuttlebutt status")
	}
	return ret
}
