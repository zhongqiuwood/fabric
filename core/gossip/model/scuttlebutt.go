package gossip_model

//VClock is a type of digest, indicate a partial order nature
type VClock interface {
	Less(VClock) bool //VClock can be NEVER less than nil (indicate to "oldest" time)
}

//scuttlebutt scheme maintain per-peer status work with vclock digest
type ScuttlebuttPeerUpdate interface {
	From() VClock
	To() VClock
}

type ScuttlebuttPeerStatus interface {
	To() VClock
	PickFrom(VClock) ScuttlebuttPeerUpdate
	Update(ScuttlebuttPeerUpdate, UpdateIn) error
}

//elements in scuttlebutt scheme include a per-peer data and global data
type scuttlebuttDigest struct {
	Digest
	d map[string]VClock
}

type scuttlebuttUpdateOut struct {
	UpdateOut
	u map[string]ScuttlebuttPeerUpdate
}

type scuttlebuttUpdateIn struct {
	UpdateIn
	u map[string]ScuttlebuttPeerUpdate
}

func MakeScuttlebuttLocalUpdate(gu UpdateIn) *scuttlebuttUpdateIn {
	return &scuttlebuttUpdateIn{
		u:        make(map[string]ScuttlebuttPeerUpdate),
		UpdateIn: gu,
	}
}

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
	MissedUpdate(string, ScuttlebuttPeerUpdate, UpdateIn) error
}

type scuttlebuttStatus struct {
	ScuttlebuttStatus
	Peers map[string]ScuttlebuttPeerStatus
}

func (s *scuttlebuttStatus) Merge(u_in UpdateIn) error {

	u, ok := u_in.(*scuttlebuttUpdateIn)

	if !ok {
		panic("type error, not scuttlebuttUpdateIn")
	}

	err := s.Update(u.UpdateIn)
	if err != nil {
		return err
	}

	for id, ss := range u.u {
		//remove request
		if ss == nil {
			delete(s.Peers, id)
		} else {

			pss, ok := s.Peers[id]
			if ok {
				err = pss.Update(ss, u.UpdateIn)
			} else {
				err = s.MissedUpdate(id, ss, u.UpdateIn)
			}

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *scuttlebuttStatus) GenDigest() Digest {
	r := &scuttlebuttDigest{
		d:      make(map[string]VClock),
		Digest: s.GenDigest(),
	}

	for id, ss := range s.Peers {
		r.d[id] = ss.To()
	}

	return r
}

func (s *scuttlebuttStatus) MakeUpdate(dig_in Digest) UpdateOut {

	dig, ok := dig_in.(*scuttlebuttDigest)
	if !ok {
		panic("type error, not scuttlebuttDigest")
	}

	r := &scuttlebuttUpdateOut{
		u:         make(map[string]ScuttlebuttPeerUpdate),
		UpdateOut: s.MakeUpdate(dig.Digest),
	}

	for id, dd := range dig.d {
		ss, ok := s.Peers[id]
		if !ok {
			ss = s.NewPeer(id)
			if ss != nil {
				s.Peers[id] = ss
			}
		} else if dd.Less(ss.To()) {
			if ssu := ss.PickFrom(dd); ssu != nil {
				r.u[id] = ssu
			}
		}
	}

	return r

}

func (s *scuttlebuttStatus) SetSelfPeer(selfid string,
	self ScuttlebuttPeerStatus) {

	//we add "self peer" on both selfid and default value(""),
	//so an localupdate need not to know the self peer id
	s.Peers[""] = self
	s.Peers[selfid] = self
}

func NewScuttlebuttStatus(gs ScuttlebuttStatus) *scuttlebuttStatus {

	return &scuttlebuttStatus{
		ScuttlebuttStatus: gs,
		Peers:             make(map[string]ScuttlebuttPeerStatus),
	}
}
