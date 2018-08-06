package txnetwork

import (
	"container/list"
	"fmt"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/util"
	pb "github.com/abchain/fabric/protos"
	proto "github.com/golang/protobuf/proto"
	"sync"
	"time"
)

var global *txNetworkGlobal

type peerTxStatusUpdate struct {
	*pb.PeerTxState
}

func (u peerTxStatusUpdate) To() model.VClock {
	return standardVClock(u.GetNum())
}

type peerStatus struct {
	peerId string
	peerTxStatusUpdate
	lastAccess time.Time
}

func (s *peerStatus) PickFrom(d_in model.VClock, u_in model.Update) (model.ScuttlebuttPeerUpdate, model.Update) {
	d, ok := d_in.(standardVClock)
	if !ok {
		panic("Type error, not standardVClock")
	}

	//we only copy endorsment in first picking (with d is 0)
	if uint64(d) == 0 {
		return s.peerTxStatusUpdate, u_in
	} else {
		ret := peerTxStatusUpdate{new(pb.PeerTxState)}
		ret.Num = s.GetNum()
		ret.Digest = s.GetDigest()
		ret.Signature = s.GetSignature()
		return ret, u_in
	}

}

func (s *peerStatus) Update(u_in model.ScuttlebuttPeerUpdate, g_in model.ScuttlebuttStatus) error {

	u, ok := u_in.(peerTxStatusUpdate)
	if !ok {
		panic("Type error, not peerTxStatusUpdate")
	}

	g, ok := g_in.(*txNetworkGlobal)
	if !ok {
		panic("Type error, not txNetworkGlobal")
	}

	//scuttlebutt mode should avoiding this
	if u.GetNum() < s.GetNum() {
		panic("Wrong series, model error")
	}

	//TODO: we must obtain endorsement in the first pulling
	if len(s.Endorsement) == 0 {
		if len(u.GetEndorsement()) == 0 {
			return fmt.Errorf("Update do not include endorsement for our pulling")
		}
	} else {
		//copy endorsement to u and later we will just keep u
		u.Endorsement = s.Endorsement
	}

	//TODO: verify the signature of incoming data

	//we must lock global part
	g.Lock()
	defer g.Unlock()
	//DO WE NEED COPY?
	s.peerTxStatusUpdate = u
	s.lastAccess = time.Now()

	g.accessPeer(s.peerId)
	return nil
}

type selfPeerStatus struct {
	sync.Once
	*peerStatus
}

func (s *selfPeerStatus) create() *peerStatus {

	s.Do(func() {
		s.peerStatus = &peerStatus{
			util.GenerateUUID(),
			peerTxStatusUpdate{&pb.PeerTxState{
				Digest: util.GenerateBytesUUID(),
				//TODO: we must endorse it
				Endorsement: []byte{1},
			}},
			time.Now(),
		}

		logger.Infof("Create self peer [%s]", s.peerStatus.peerId)
	})

	return s.peerStatus
}

//txnetworkglobal manage all the peers across whole networks (mutiple peers)
type txNetworkGlobal struct {
	maxPeers int
	sync.RWMutex
	lruQueue  *list.List
	lruIndex  map[string]*list.Element
	onevicted []func([]string)

	selfpeers map[*gossip.GossipStub]*selfPeerStatus
}

func (*txNetworkGlobal) GenDigest() model.Digest                                { return nil }
func (*txNetworkGlobal) MakeUpdate(_ model.Digest) model.Update                 { return nil }
func (*txNetworkGlobal) Update(_ model.Update) error                            { return nil }
func (*txNetworkGlobal) MissedUpdate(string, model.ScuttlebuttPeerUpdate) error { return nil }

func (g *txNetworkGlobal) accessPeer(id string) {

	i, ok := g.lruIndex[id]

	if ok {
		g.lruQueue.MoveToFront(i)
	}
}

func (g *txNetworkGlobal) truncateTailPeer(cnt int) (ret []string) {

	for ; cnt > 0; cnt-- {
		v, ok := g.lruQueue.Remove(g.lruQueue.Back()).(*peerStatus)
		if !ok {
			panic("Type error, not peerStatus")
		}
		delete(g.lruIndex, v.peerId)
		ret = append(ret, v.peerId)
	}

	return
}

func (g *txNetworkGlobal) addNewPeer(id string) *peerStatus {

	if _, ok := g.lruIndex[id]; ok {
		logger.Errorf("Request add duplicated peer [%s]", id)
		return nil
	}

	if len(g.lruIndex) > g.maxPeers {
		g.truncateTailPeer(len(g.lruIndex) - g.maxPeers)
	}

	//if we can't truncate anypeer, just give up
	if len(g.lruIndex) > g.maxPeers {
		logger.Errorf("Can't not add peer [%s], exceed limit %d", id, g.maxPeers)
		return nil
	}

	ret := &peerStatus{
		id,
		peerTxStatusUpdate{&pb.PeerTxState{}},
		time.Time{},
	}
	g.lruIndex[id] = g.lruQueue.PushBack(ret)

	return ret
}

func (g *txNetworkGlobal) NewPeer(id string) model.ScuttlebuttPeerStatus {
	g.Lock()
	defer g.Unlock()

	return g.addNewPeer(id)
}

func (g *txNetworkGlobal) RemovePeer(peer_in model.ScuttlebuttPeerStatus) {

	peer, ok := peer_in.(*peerStatus)
	if !ok {
		panic("Type error, not peerStatus")
	}

	g.Lock()
	defer g.Unlock()

	item, ok := g.lruIndex[peer.peerId]

	if ok {
		g.lruQueue.Remove(item)
		delete(g.lruIndex, peer.peerId)
	}
}

func (g *txNetworkGlobal) QueryPeer(id string) *pb.PeerTxState {
	g.RLock()
	defer g.RUnlock()

	i, ok := g.lruIndex[id]
	if ok {
		s := i.Value.(*peerStatus).PeerTxState
		if len(s.Endorsement) == 0 {
			//never return an peer which is not inited
			return nil
		}
		return s
	}

	return nil
}

func (g *txNetworkGlobal) BlockPeer(id string) {
	g.RLock()
	defer g.RUnlock()
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

func (g *txNetworkGlobal) SelfPeer(stub *gossip.GossipStub) *peerStatus {
	g.Lock()
	defer g.Unlock()

	if s, ok := g.selfpeers[stub]; !ok {
		g.selfpeers[stub] = &selfPeerStatus{}
	} else {
		return s.peerStatus
	}

	self := g.selfpeers[stub].create()
	//*** self peer never come into LRU!
	g.lruIndex[self.peerId] = &list.Element{Value: self}

	return self
}

func GetNetworkStatus() *txNetworkGlobal { return global }

type globalCat struct {
	policy gossip.CatalogPolicies
}

func init() {

	global = &txNetworkGlobal{
		maxPeers:  def_maxPeer,
		lruQueue:  list.New(),
		lruIndex:  make(map[string]*list.Element),
		selfpeers: make(map[*gossip.GossipStub]*selfPeerStatus),
	}

	gossip.RegisterCat = append(gossip.RegisterCat, initNetworkStatus)
}

func initNetworkStatus(stub *gossip.GossipStub) {

	self := global.SelfPeer(stub)
	selfstatus := model.NewScuttlebuttStatus(global)
	selfstatus.SetSelfPeer(self.peerId, self)
	m := model.NewGossipModel(selfstatus)

	globalcat := new(globalCat)
	globalcat.policy = gossip.NewCatalogPolicyDefault()

	stub.AddCatalogHandler(globalCatName,
		gossip.NewCatalogHandlerImpl(stub.GetSStub(),
			stub.GetStubContext(), globalcat, m))

}

const (
	globalCatName = "global"
	def_maxPeer   = 1024
)

//Implement for CatalogHelper
func (c *globalCat) Name() string                        { return hotTxCatName }
func (c *globalCat) GetPolicies() gossip.CatalogPolicies { return c.policy }

func (c *globalCat) TransDigestToPb(d_in model.Digest) *pb.Gossip_Digest {
	d, ok := d_in.(model.ScuttlebuttDigest)
	if !ok {
		panic("Type error, not ScuttlebuttDigest")
	}
	return toPbDigestStd(d, nil)
}

func (c *globalCat) TransPbToDigest(msg *pb.Gossip_Digest) model.Digest {
	return parsePbDigestStd(msg, nil)
}

func (c *globalCat) UpdateMessage() proto.Message { return new(pb.Gossip_TxState) }

func (c *globalCat) EncodeUpdate(cpo gossip.CatalogPeerPolicies, u_in model.Update, msg_in proto.Message) proto.Message {

	u, ok := u_in.(model.ScuttlebuttUpdate)

	if !ok {
		panic("Type error, not ScuttlebuttUpdate")
	}

	msg := &pb.Gossip_TxState{make(map[string]*pb.PeerTxState)}

	for id, iu_in := range u.PeerUpdate() {
		iu, ok := iu_in.(peerTxStatusUpdate)
		if !ok {
			panic("Type error, not peerTxStatusUpdate")
		}
		msg.Txs[id] = iu.PeerTxState
	}

	return msg
}

func (c *globalCat) DecodeUpdate(cpo gossip.CatalogPeerPolicies, msg_in proto.Message) (model.Update, error) {
	msg, ok := msg_in.(*pb.Gossip_TxState)
	if !ok {
		panic("Type error, not Gossip_TxState")
	}

	u := model.NewscuttlebuttUpdate(nil)
	for id, iu := range msg.Txs {
		u.UpdatePeer(id, peerTxStatusUpdate{iu})
	}

	return u, nil
}
