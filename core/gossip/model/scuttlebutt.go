package gossip_model

//VClock is a type of digest, indicate a partial order nature
type VClock interface {
	Less(VClock) bool //VClock can be NEVER less than nil (indicate to "oldest" time)
}

//scuttlebutt scheme maintain per-peer status work with vclock digest
type ScuttlebuttPeerUpdate interface {
	To() VClock
}

type ScuttlebuttPeerStatus interface {
	To() VClock
	PickFrom(string, VClock, Update) (ScuttlebuttPeerUpdate, Update)
	Update(string, ScuttlebuttPeerUpdate, ScuttlebuttStatus) error
}

type ScuttlebuttDigest interface {
	GlobalDigest() Digest
	PeerDigest() map[string]VClock
}

//elements in scuttlebutt scheme include a per-peer data and global data
type scuttlebuttDigest struct {
	Digest
	d map[string]VClock
}

func NewscuttlebuttDigest(gd Digest) *scuttlebuttDigest {
	return &scuttlebuttDigest{Digest: gd, d: make(map[string]VClock)}
}

func (d *scuttlebuttDigest) GlobalDigest() Digest { return d.Digest }

func (d *scuttlebuttDigest) PeerDigest() map[string]VClock { return d.d }

func (d *scuttlebuttDigest) SetPeerDigest(id string, dig VClock) {
	d.d[id] = dig
}

type ScuttlebuttUpdate interface {
	GlobalUpdate() Update
	PeerUpdate() map[string]ScuttlebuttPeerUpdate
}

type scuttlebuttUpdate struct {
	Update
	u map[string]ScuttlebuttPeerUpdate
}

func (*scuttlebuttUpdate) Gossip_IsUpdateIn() bool { return false }

func (u *scuttlebuttUpdate) GlobalUpdate() Update { return u.Update }

func (u *scuttlebuttUpdate) PeerUpdate() map[string]ScuttlebuttPeerUpdate { return u.u }

type scuttlebuttUpdateIn struct {
	*scuttlebuttUpdate
}

func NewscuttlebuttUpdate(gu Update) *scuttlebuttUpdateIn {
	return &scuttlebuttUpdateIn{
		&scuttlebuttUpdate{
			u:      make(map[string]ScuttlebuttPeerUpdate),
			Update: gu,
		},
	}
}

func (*scuttlebuttUpdateIn) Gossip_IsUpdateIn() bool { return true }

func (u *scuttlebuttUpdateIn) UpdatePeer(id string, pu ScuttlebuttPeerUpdate) {
	u.u[id] = pu
}

//used for local update
func (u *scuttlebuttUpdateIn) UpdateLocal(pu ScuttlebuttPeerUpdate) {
	u.u[""] = pu
}

func (u *scuttlebuttUpdateIn) RemovePeers(ids []string) {
	for _, id := range ids {
		u.u[id] = nil
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

	for id, ss := range u.PeerUpdate() {

		pss, ok := s.Peers[id]
		if !ok {
			continue
		}
		//remove request
		if ss == nil {
			s.RemovePeer(id, pss)
			delete(s.Peers, id)
		} else {

			if pss.To().Less(ss.To()) {
				err = pss.Update(id, ss, s.ScuttlebuttStatus)
			} else {
				err = s.MissedUpdate(id, ss)
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

func (s *scuttlebuttStatus) GenDigest() Digest {
	r := NewscuttlebuttDigest(s.ScuttlebuttStatus.GenDigest())
	for id, ss := range s.Peers {
		if id == "" {
			r.SetPeerDigest(s.SelfID, ss.To())
		} else {
			r.SetPeerDigest(id, ss.To())
		}

	}

	return r
}

func (s *scuttlebuttStatus) MakeUpdate(dig_in Digest) Update {

	dig, ok := dig_in.(ScuttlebuttDigest)
	if !ok {
		panic("type error, not scuttlebuttDigest")
	}

	r := &scuttlebuttUpdate{
		Update: s.ScuttlebuttStatus.MakeUpdate(dig.GlobalDigest()),
		u:      make(map[string]ScuttlebuttPeerUpdate),
	}

	digs := dig.PeerDigest()

	//special handle self id
	if dd, ok := digs[s.SelfID]; ok {
		if ss, ok := s.Peers[""]; ok && dd.Less(ss.To()) {
			if ssu, ssgu := ss.PickFrom("", dd, r.Update); ssu != nil {
				r.u[s.SelfID] = ssu
				if ssgu != nil {
					r.Update = ssgu
				}
			}
		}
		delete(digs, s.SelfID)
		defer func(v VClock) { digs[s.SelfID] = v }(dd)
	}

	for id, dd := range digs {
		ss, ok := s.Peers[id]
		if !ok {
			ss = s.NewPeer(id)
			if ss != nil {
				s.Peers[id] = ss
			}
		} else if dd.Less(ss.To()) {
			if ssu, ssgu := ss.PickFrom(id, dd, r.Update); ssu != nil {
				r.u[id] = ssu
				if ssgu != nil {
					r.Update = ssgu
				}
			}
		}
	}

	return r

}

func (s *scuttlebuttStatus) SetSelfPeer(selfid string,
	self ScuttlebuttPeerStatus) (oldself string) {

	defer func(id string) {
		oldself = id
	}(s.SelfID)

	s.SelfID = selfid
	s.Peers[""] = self
	return
}

func NewScuttlebuttStatus(gs ScuttlebuttStatus) *scuttlebuttStatus {

	return &scuttlebuttStatus{
		ScuttlebuttStatus: gs,
		Peers:             make(map[string]ScuttlebuttPeerStatus),
	}
}
