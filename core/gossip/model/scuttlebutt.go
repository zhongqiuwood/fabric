package gossip_model

import (
	"math"
	"math/rand"
)

//VClock is a type of digest, indicate a partial order nature
type VClock interface {
	Less(VClock) bool //VClock can be NEVER less than nil (indicate to "oldest" time)
}

//a "limit" clock represent the clock in two-side (bottom and top)
type limitClock bool

func (b limitClock) Less(m VClock) bool {
	//need to consider the case if incoming is also the limit clock
	if i, ok := m.(limitClock); ok {
		if bool(b) == bool(i) {
			//if we are the same limit clock ...
			return false
		}
	}

	return bool(b)
}

//any other implement should consider these values before type switch, just like "nil"
var BottomClock = limitClock(true) //earlier than anything
var TopClock = limitClock(false)   //later than anything

//scuttlebutt scheme maintain per-peer status work with vclock digest
type ScuttlebuttPeerUpdate interface {
	To() VClock
}

type ScuttlebuttPeerStatus interface {
	To() VClock
	PickFrom(VClock, Update) (ScuttlebuttPeerUpdate, Update)
	Update(string, ScuttlebuttPeerUpdate, ScuttlebuttStatus) error
}

type peersDig struct {
	Id string
	V  VClock
}

type ScuttlebuttDigest interface {
	GlobalDigest() Digest
	PeerDigest() []peersDig
	IsPartial() bool
}

//elements in scuttlebutt scheme include a per-peer data and global data
type scuttlebuttDigest struct {
	Digest
	d         []peersDig
	isPartial bool
}

func NewscuttlebuttDigest(gd Digest) *scuttlebuttDigest {
	return &scuttlebuttDigest{Digest: gd}
}

func (d *scuttlebuttDigest) GlobalDigest() Digest { return d.Digest }

func (d *scuttlebuttDigest) PeerDigest() []peersDig { return d.d }

func (d *scuttlebuttDigest) IsPartial() bool { return d.isPartial }

func (d *scuttlebuttDigest) SetPeerDigest(id string, dig VClock) {
	d.d = append(d.d, peersDig{id, dig})
}

func (d *scuttlebuttDigest) MarkDigestIsPartial() {
	d.isPartial = true
}

type peersUpdate struct {
	Id string
	U  ScuttlebuttPeerUpdate
}

type ScuttlebuttUpdate interface {
	GlobalUpdate() Update
	PeerUpdate() []peersUpdate
}

type scuttlebuttUpdate struct {
	Update
	u []peersUpdate
}

func (*scuttlebuttUpdate) Gossip_IsUpdateIn() bool { return false }

func (u *scuttlebuttUpdate) GlobalUpdate() Update { return u.Update }

func (u *scuttlebuttUpdate) PeerUpdate() []peersUpdate { return u.u }

type scuttlebuttUpdateIn struct {
	*scuttlebuttUpdate
}

func NewscuttlebuttUpdate(gu Update) *scuttlebuttUpdateIn {
	return &scuttlebuttUpdateIn{&scuttlebuttUpdate{Update: gu}}
}

func (*scuttlebuttUpdateIn) Gossip_IsUpdateIn() bool { return true }

func (u *scuttlebuttUpdateIn) UpdatePeer(id string, pu ScuttlebuttPeerUpdate) {
	//never allow a "" is added, so self peer can be updated by UpdateLocal
	if id == "" {
		return
	}
	u.u = append(u.u, peersUpdate{id, pu})
}

//used for local update
func (u *scuttlebuttUpdateIn) UpdateLocal(pu ScuttlebuttPeerUpdate) {
	u.u = append(u.u, peersUpdate{"", pu})
}

func (u *scuttlebuttUpdateIn) RemovePeers(ids []string) {
	for _, id := range ids {
		u.u = append(u.u, peersUpdate{id, nil})
	}
}

//scuttlebuttStatusHelper provide a per-peer status managing
type ScuttlebuttStatus interface {
	Status
	NewPeer(string) ScuttlebuttPeerStatus
	RemovePeer(string, ScuttlebuttPeerStatus)
	MissedUpdate(string, ScuttlebuttPeerUpdate) error
}

type scuttlebuttStatus struct {
	ScuttlebuttStatus
	Peers  map[string]ScuttlebuttPeerStatus
	SelfID string
	//depress the usage of extended protocol
	Extended bool
	//max items in one digest/update message, 0 is not limit
	MaxUpdateLimit   int
	AdditionalFilter func(string, ScuttlebuttPeerStatus) bool
}

// type noPeerStatusError string

// func (s noPeerStatusError) Error() string {
// 	return string(s) + " is not a known peer"
// }

func (s *scuttlebuttStatus) Update(u_in Update) error {

	u, ok := u_in.(ScuttlebuttUpdate)

	if !ok {
		panic("type error, not scuttlebuttUpdate")
	}

	err := s.ScuttlebuttStatus.Update(u.GlobalUpdate())
	if err != nil {
		return err
	}

	for _, item := range u.PeerUpdate() {
		id := item.Id

		pss, ok := s.Peers[id]
		if !ok {
			//with extended protocol, update can carry unknown peers
			if item.U == nil {
				continue
			}
			//(we not allowed delete self amoung update)
			if id == "" {
				pss, ok = s.Peers[s.SelfID]
				if !ok {
					continue
				}
				id = s.SelfID
			} else {
				pss = s.NewPeer(id)
				if pss == nil {
					continue
				}
				s.Peers[id] = pss
			}

		}
		//remove request
		if item.U == nil {
			s.RemovePeer(id, pss)
			delete(s.Peers, id)
		} else {

			if pss.To().Less(item.U.To()) {
				err = pss.Update(id, item.U, s.ScuttlebuttStatus)
			} else {
				err = s.MissedUpdate(id, item.U)
			}
			// no peer status CAN NOT be consider as an error
			// because far-end may return a update including removed peer
			// just after the digest which far-end received is sent
			// else {
			// 	err = noPeerStatusError(id)
			// }

			if err != nil {
				return err
			}
		}
	}

	return nil
}

//if digest generate from model the digest is always "full" (represent all peers we have known)
// so we mark it as "not partial", it was only unmarked when it has been altered
//**** HOWEVER, we use "extended" flag to depress this for compitable with old codes*****
func (s *scuttlebuttStatus) GenDigest() Digest {
	r := NewscuttlebuttDigest(s.ScuttlebuttStatus.GenDigest())

	scounter := 0
	for id, ss := range s.Peers {
		if s.AdditionalFilter != nil && !s.AdditionalFilter(id, ss) {
			continue
		}
		scounter++

		if scounter > s.MaxUpdateLimit {
			//stream sampling: we consider calling of To() is trival
			//so the cost of wasting a To() calling is small
			//It is not the case in MakeUpdate
			if pos := rand.Intn(scounter); pos >= len(r.d) {
				continue
			} else {
				r.d[pos] = peersDig{id, ss.To()}
			}
		} else {
			r.SetPeerDigest(id, ss.To())
		}

	}

	if !s.Extended || len(r.d) < len(s.Peers) {
		r.isPartial = true
	}

	return r
}

func (s *scuttlebuttStatus) MakeUpdate(dig_in Digest) Update {

	dig, ok := dig_in.(ScuttlebuttDigest)
	if !ok {
		panic("type error, not scuttlebuttDigest")
	}

	r := &scuttlebuttUpdate{Update: s.ScuttlebuttStatus.MakeUpdate(dig.GlobalDigest())}

	digs := map[string]bool{}
	//PickFrom is costful, searching in the map also is, so we purchase time by space
	digsCache := []VClock{}
	ssCache := []ScuttlebuttPeerStatus{}

	req := dig.PeerDigest()
	//simply truncate the request array
	if len(req) > s.MaxUpdateLimit {
		req = req[:s.MaxUpdateLimit]
	}

	for _, dd := range req {
		id := dd.Id
		digs[id] = true
		ss, ok := s.Peers[id]
		if !ok {
			//touch new peer
			if ss = s.NewPeer(id); ss != nil {
				s.Peers[id] = ss
			} else {
				continue
			}
		}

		if !dd.V.Less(ss.To()) {
			continue
		}

		r.u = append(r.u, peersUpdate{id, nil})
		ssCache = append(ssCache, ss)
		digsCache = append(digsCache, dd.V)
	}

	//protocol extended: "full digest" mode means we are
	//in fact search the whole peer space, and need to
	//filter it (notice, filter is not applied on which
	//the far-end query them explicity)
	if !dig.IsPartial() {
		scounter := len(r.u)
		for id, ss := range s.Peers {

			if _, ok := digs[id]; ok {
				continue
			} else if !BottomClock.Less(ss.To()) {
				//ss may also have the lowest clock, which should not count
				continue
			} else if s.AdditionalFilter != nil && !s.AdditionalFilter(id, ss) {
				continue
			}

			scounter++
			if scounter > s.MaxUpdateLimit {
				if pos := rand.Intn(scounter); pos >= len(r.u) {
					continue
				} else {
					r.u[pos] = peersUpdate{id, nil}
					ssCache[pos] = ss
					digsCache[pos] = BottomClock
				}

			} else {
				r.u = append(r.u, peersUpdate{id, nil})
				ssCache = append(ssCache, ss)
				digsCache = append(digsCache, BottomClock)
			}
		}
	}

	//finally we do required PickFrom calling
	var ssgu Update
	for i, _ := range r.u {
		r.u[i].U, ssgu = ssCache[i].PickFrom(digsCache[i], r.Update)
		if ssgu != nil {
			r.Update = ssgu
		}
	}

	return r

}

//set a newid as self, if it has existed, the old peer will be replaced without any warning!
//after reset, the old self peer will become a common peer (and you need to remove it by
//an update)
func (s *scuttlebuttStatus) SetSelfPeer(selfid string,
	self ScuttlebuttPeerStatus) (oldself string) {

	defer func(id string) {
		oldself = id
	}(s.SelfID)

	s.Peers[selfid] = self
	s.SelfID = selfid

	return
}

func NewScuttlebuttStatus(gs ScuttlebuttStatus) *scuttlebuttStatus {

	return &scuttlebuttStatus{
		ScuttlebuttStatus: gs,
		Peers:             make(map[string]ScuttlebuttPeerStatus),
		MaxUpdateLimit:    math.MaxInt32,
	}
}
