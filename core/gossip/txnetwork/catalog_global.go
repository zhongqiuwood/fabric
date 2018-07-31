package txnetwork

import (
	"container/list"
	"github.com/abchain/fabric/core/gossip"
	"github.com/abchain/fabric/core/util"
	pb "github.com/abchain/fabric/protos"
	"sync"
)

var global *txNetworkGlobal

type peerTxStatusUpdate struct {
	*pb.Gossip_Digest_PeerState
}

func (u peerTxStatusUpdate) To() model.VClock {
	return standardVClock(u.GetNum())
}

type peerStatus struct {
	peerId string
	peerTxStatusUpdate
	chainState []byte
}

func (s *peerStatus) createPeerTxItem() *txMemPoolItem {
	return &txMemPoolItem{
		digest:       s.GetState(),
		digestSeries: s.GetNum(),
	}
}

func (s *peerStatus) PickFrom(d_in model.VClock, u_in model.Update) (model.ScuttlebuttPeerUpdate, model.Update) {
	d, ok := d_in.(standardVClock)
	if !ok {
		panic("Type error, not standardVClock")
	}

	return peerTxStatusUpdate, u_in
}

func (s *peerStatus) Update(u_in model.ScuttlebuttPeerUpdate, _ model.ScuttlebuttStatus) error {

	u, ok := u_in.(peerTxStatusUpdate)
	if !ok {
		panic("Type error, not peerTxStatusUpdate")
	}

	//scuttlebutt mode should avoiding this
	if u.GetNum() < s.GetNum() {
		panic("Wrong series, model error")
	}

	//TODO: verify the signature of incoming data

	//lite-copy data (not just obtain the pointer)
	//we must lock global part
	global.Lock()
	defer global.Unlock()
	//DO WE NEED COPY?
	cpy := *u.Gossip_Digest_PeerState
	s.Gossip_Digest_PeerState = &cpy

	return nil
}

type selfPeerStatus struct {
	peerStatus
	sync.Once
}

type txNetworkGlobal struct {
	sync.RWMutex
	lruQueue  *list.List
	lruIndex  map[string]*list.Element
	selfPeer  *selfPeerStatus
	onevicted []func([]string)
}

func (*txNetworkGlobal) GenDigest() model.Digest                { return nil }
func (*txNetworkGlobal) MakeUpdate(_ model.Digest) model.Update { return nil }
func (*txNetworkGlobal) Update(_ model.Update) error            { return nil }

func (*txNetworkGlobal) NewPeer(id string) model.ScuttlebuttPeerStatus {

}

func (*txNetworkGlobal) RemovePeer(model.ScuttlebuttPeerStatus) {

}

func (*txNetworkGlobal) MissedUpdate(string, model.ScuttlebuttPeerUpdate) error { return nil }

func GetNetworkStatus() *txNetworkGlobal { return global }

func init() {

	global = &txNetworkGlobal{
		lruQueue: list.New(),
		lruIndex: make(map[string]*list.Element),
		selfPeer: new(selfPeerStatus),
	}

	gossip.RegisterCat = append(gossip.RegisterCat, initNetworkStatus)
}

func initNetworkStatus(stub *gossip.GossipStub) {
}

func (g *txNetworkGlobal) getSelfStatus() *selfPeerStatus {

	g.selfPeer.Do(
		func() {
			//TODO: we generate id and endorse it
			g.selfPeer.peerId = util.GenerateUUID()
			g.selfPeer.beginTxDigest = util.GenerateBytesUUID()
		})

	return g.selfPeer
}

func (g *txNetworkGlobal) regNotify(f func([]string)) {
	g.Lock()
	defer g.Unlock()
	g.onevicted = append(g.onevicted, f)
}

func (g *txNetworkGlobal) notifyEvict(peers []*peerStatus) {
	g.RLock()
	defer g.RUnlock()

	ids := make([]string, len(peers))

	for i, p := range peers {
		ids[i] = p.peerId
	}

	for _, f := range g.onevicted {
		f(ids)
	}
}

func (g *txNetworkGlobal) addNewPeer(id string) *peerStatus {
	g.Lock()
	defer g.Unlock()

	item, ok := g.lruIndex[id]

	if ok {
		return item.Value.(*peerStatus)
	}

	g.lruIndex[id] = g.lruQueue.PushBack(&peerStatus{peerId: id})

	return g.lruQueue.Back().Value.(*peerStatus)
}

func (g *txNetworkGlobal) queryPeer(id string) *peerStatus {
	g.RLock()
	defer g.RUnlock()

	i, ok := g.lruIndex[id]
	if ok {
		return i.Value.(*peerStatus)
	}

	return nil
}

func (g *txNetworkGlobal) accessPeer(id string) {

	g.Lock()
	defer g.Unlock()

	i, ok := g.lruIndex[id]

	if ok {
		g.lruQueue.MoveToFront(i)
	}
}

func (g *txNetworkGlobal) updatePeer(id string, u *peerStatus) {

	//YES, it is read-lock
	g.RLock()
	defer g.RUnlock()

	i, ok := g.lruIndex[id]
	if ok {
		i.Value = u
	}
}

func (g *txNetworkGlobal) truncateTailPeer(cnt int) (ret []*peerStatus) {

	g.Lock()
	defer g.Unlock()

	for ; cnt > 0; cnt-- {
		v, ok := g.lruQueue.Remove(g.lruQueue.Back()).(*peerStatus)
		if !ok {
			panic("Type error, not peerStatus")
		}
		delete(g.lruIndex, v.peerId)
		ret = append(ret, v)
	}

	return
}

func (g *txNetworkGlobal) peerSize() int {
	g.RLock()
	defer g.RUnlock()

	return len(g.lruIndex)
}
