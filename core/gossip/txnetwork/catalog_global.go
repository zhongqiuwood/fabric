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

type peerStatus struct {
	*pb.PeerTxState
}

func (s peerStatus) To() model.VClock {
	//an "empty" status has the lowest clock
	//But notice "endorsement" could not
	//be use because when object is used as update
	//endorsement field may be omitted.
	//so we judge the empty status by digest
	if len(s.GetDigest()) == 0 {
		return model.BottomClock
	}
	//shift clock by 1 so every "valid" clock is start from 1, and "unknown" is from bottomclock
	//(respresent by 0 in protobuf-struct)
	return standardVClock(s.GetNum() + 1)
}

func (s peerStatus) PickFrom(id string, d_in model.VClock, u_in model.Update) (model.ScuttlebuttPeerUpdate, model.Update) {

	d := toStandardVClock(d_in)

	//we only copy endorsment in first picking (with d is 0)
	if uint64(d) == 0 {
		return peerStatus{s.PeerTxState}, u_in
	} else {
		ret := new(pb.PeerTxState)
		ret.Num = s.GetNum()
		ret.Digest = s.GetDigest()
		ret.Signature = s.GetSignature()
		return peerStatus{ret}, u_in
	}

}

func (s *peerStatus) Update(id string, u_in model.ScuttlebuttPeerUpdate, g_in model.ScuttlebuttStatus) error {

	u, ok := u_in.(peerStatus)
	if !ok {
		panic("Type error, not peerStatus")
	}

	g, ok := g_in.(*txNetworkGlobal)
	if !ok {
		panic("Type error, not txNetworkGlobal")
	}

	//scuttlebutt mode should avoiding this
	if u.GetNum() < s.GetNum() {
		panic("Wrong series, model error")
	}

	lastSeries := s.PeerTxState.GetNum()
	established := len(s.Endorsement) == 0

	//TODO: we must obtain endorsement in the first pulling
	if established {
		if len(u.GetEndorsement()) == 0 {
			return fmt.Errorf("Update do not include endorsement for our pulling")
		}
		logger.Infof("We have establish new gossip peer [%s]:[%d:%x]", id, u.GetNum(), u.GetDigest())
	} else {
		//copy endorsement to u and later we will just keep u
		u.Endorsement = s.Endorsement
		logger.Infof("We have update gossip peer [%s] from %d to [%d:%x]", id, lastSeries, u.GetNum(), u.GetDigest())
	}

	//TODO: verify the signature of incoming data
	s.PeerTxState = u.PeerTxState

	//update global part ...
	g.Lock()
	//in case we are update local
	if id == "" {
		id = g.selfId
	}

	if item, ok := g.lruIndex[id]; ok {

		if s, ok := item.Value.(*peerStatusItem); ok {
			s.PeerTxState = u.PeerTxState
			s.lastAccess = time.Now()
			g.lruQueue.MoveToFront(item)
		}

	}

	f := g.onupdate
	g.Unlock()

	//so if the number has "cross" a border of a cache queue row
	//we will trigger an update notify
	if lastSeries+uint64(peerTxQueueLen) < s.PeerTxState.GetNum() ||
		uint(lastSeries)&peerTxQueueMask > uint(s.PeerTxState.GetNum())&peerTxQueueMask {
		handleUpdate(f, id, false)
	} else if established {
		handleUpdate(f, id, true)
	}

	return nil
}

type peerStatusItem struct {
	peerId string
	*pb.PeerTxState
	lastAccess time.Time
}

//txnetworkglobal manage all the peers across whole networks (mutiple peers)
type txNetworkGlobal struct {
	maxPeers int
	sync.RWMutex
	selfId    string
	lruQueue  *list.List
	lruIndex  map[string]*list.Element
	onupdate  []func(string, bool) error
	onevicted []func([]string) error
}

func (*txNetworkGlobal) GenDigest() model.Digest                                { return nil }
func (*txNetworkGlobal) MakeUpdate(_ model.Digest) model.Update                 { return nil }
func (*txNetworkGlobal) Update(_ model.Update) error                            { return nil }
func (*txNetworkGlobal) MissedUpdate(string, model.ScuttlebuttPeerUpdate) error { return nil }

func (g *txNetworkGlobal) truncateTailPeer(cnt int) (ret []string) {

	for ; cnt > 0; cnt-- {
		v, ok := g.lruQueue.Remove(g.lruQueue.Back()).(*peerStatusItem)
		if !ok {
			panic("Type error, not peerStatus")
		}
		delete(g.lruIndex, v.peerId)
		ret = append(ret, v.peerId)
	}

	return
}

func handleUpdate(onupdate []func(string, bool) error, id string, created bool) {
	ret := make(chan error)
	go func() {
		for _, f := range onupdate {
			ret <- f(id, created)
		}

		close(ret)
	}()

	for e := range ret {
		if e != nil {
			logger.Errorf("Handle update function fail: %s", e)
		}
	}
}

func handleEvict(onevicted []func([]string) error, ids []string) {
	ret := make(chan error)
	go func() {
		for _, f := range onevicted {
			ret <- f(ids)
		}

		close(ret)
	}()

	for e := range ret {
		if e != nil {
			logger.Errorf("Handle evicted function fail: %s", e)
		}
	}
}

func (g *txNetworkGlobal) addNewPeer(id string) (ret *peerStatus, ids []string) {

	if _, ok := g.lruIndex[id]; ok {
		logger.Errorf("Request add duplicated peer [%s]", id)
		return
	}

	if len(g.lruIndex) > g.maxPeers {
		ids = g.truncateTailPeer(len(g.lruIndex) - g.maxPeers)
	}

	//if we can't truncate anypeer, just give up
	if len(g.lruIndex) > g.maxPeers {
		logger.Errorf("Can't not add peer [%s], exceed limit %d", id, g.maxPeers)
		return
	}

	ret = &peerStatus{new(pb.PeerTxState)}

	g.lruIndex[id] = g.lruQueue.PushBack(
		&peerStatusItem{
			id,
			ret.PeerTxState,
			time.Time{},
		})

	logger.Infof("We have known new gossip peer [%s]", id)

	return
}

func (g *txNetworkGlobal) NewPeer(id string) model.ScuttlebuttPeerStatus {
	g.Lock()
	ret, rmids := g.addNewPeer(id)
	frm := g.onevicted
	g.Unlock()

	if rmids != nil {
		handleEvict(frm, rmids)
	}

	return ret
}

func (g *txNetworkGlobal) removePeer(id string) bool {

	item, ok := g.lruIndex[id]

	if ok {
		logger.Infof("gossip peer [%s] is removed", id)
		g.lruQueue.Remove(item)
		delete(g.lruIndex, id)
	}
	return ok
}

func (g *txNetworkGlobal) RemovePeer(id string, _ model.ScuttlebuttPeerStatus) {

	g.Lock()
	ok := g.removePeer(id)
	f := g.onevicted
	g.Unlock()

	if ok {
		handleEvict(f, []string{id})
	}
}

func (g *txNetworkGlobal) QueryPeer(id string) *pb.PeerTxState {
	g.RLock()
	defer g.RUnlock()

	i, ok := g.lruIndex[id]
	if ok {
		s := i.Value.(*peerStatusItem).PeerTxState
		if len(s.Endorsement) == 0 {
			//never return an peer which is not inited
			return nil
		}
		return s
	}

	return nil
}

func (g *txNetworkGlobal) QuerySelf() *pb.PeerTxState {
	ret := g.QueryPeer(g.selfId)
	if ret == nil {
		panic(fmt.Errorf("Do not create self peer [%s] in network", g.selfId))
	}
	return ret
}

func (g *txNetworkGlobal) BlockPeer(id string) {
	g.RLock()
	defer g.RUnlock()
}

func (g *txNetworkGlobal) RegEvictNotify(f func([]string) error) {
	g.Lock()
	defer g.Unlock()
	g.onevicted = append(g.onevicted, f)
}

func (g *txNetworkGlobal) RegUpdateNotify(f func(string, bool) error) {
	g.Lock()
	defer g.Unlock()
	g.onupdate = append(g.onupdate, f)
}

func CreateTxNetworkGlobal() *txNetworkGlobal {

	sid := util.GenerateBytesUUID()
	if len(sid) < TxDigestVerifyLen {
		panic("Wrong code generate uuid less than 16 bytes [128bit]")
	}

	ret := &txNetworkGlobal{
		maxPeers: def_maxPeer,
		lruQueue: list.New(),
		lruIndex: make(map[string]*list.Element),
		selfId:   fmt.Sprintf("%x", sid),
	}

	//also create self peer
	self := &peerStatusItem{
		"",
		&pb.PeerTxState{
			Digest: sid[:TxDigestVerifyLen],
			//TODO: we must endorse it
			Endorsement: []byte{1},
		},
		time.Now(),
	}

	logger.Infof("Create self peer [%s]", ret.selfId)
	item := &list.Element{Value: self}
	ret.lruIndex[ret.selfId] = item

	return ret
}

type globalCat struct {
	policy gossip.CatalogPolicies
}

type newPeerNotify struct {
	gossip.CatalogHandler
}

//exhandler
func (n newPeerNotify) OnConnectNewPeer(id *pb.PeerID) {
	logger.Infof("Notify peer [%s] is connected", id.GetName())
	//TODO: now we just trigger an global update ...
	n.SelfUpdate()
}

func init() {
	gossip.RegisterCat = append(gossip.RegisterCat, initNetworkStatus)
}

func initNetworkStatus(stub *gossip.GossipStub) {

	network := global.CreateNetwork(stub)
	selfstatus := model.NewScuttlebuttStatus(network)
	//use extended mode of scuttlebutt scheme, see code and wiki
	selfstatus.Extended = true

	selfstatus.SetSelfPeer(network.selfId, &peerStatus{network.QuerySelf()})
	m := model.NewGossipModel(selfstatus)

	globalcat := new(globalCat)
	globalcat.policy = gossip.NewCatalogPolicyDefault()

	h := gossip.NewCatalogHandlerImpl(stub.GetSStub(),
		stub.GetStubContext(), globalcat, m)
	stub.AddCatalogHandler(globalCatName, h)
	stub.SubScribeNewPeerNotify(newPeerNotify{h})

}

const (
	globalCatName = "global"
	def_maxPeer   = 1024
)

//Implement for CatalogHelper
func (c *globalCat) Name() string                        { return globalCatName }
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
		iu, ok := iu_in.(peerStatus)
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
	if len(msg.Txs) == 0 {
		return nil, nil
	}
	for id, iu := range msg.Txs {
		u.UpdatePeer(id, peerStatus{iu})
	}

	return u, nil
}

func UpdateLocalEpoch(stub *gossip.GossipStub, series uint64, digest []byte) error {
	globalcat := stub.GetCatalogHandler(globalCatName)
	if globalcat == nil {
		return fmt.Errorf("Can't not found corresponding global catalogHandler")
	}

	newstate := &pb.PeerTxState{Digest: digest, Num: series}
	//TODO: make signature

	selfUpdate := model.NewscuttlebuttUpdate(nil)
	selfUpdate.UpdateLocal(peerStatus{newstate})

	if err := globalcat.Model().Update(selfUpdate); err != nil {
		return err
	} else {
		globalcat.SelfUpdate()
		return nil
	}
}

func UpdateLocalPeer(stub *gossip.GossipStub) (*pb.PeerTxState, error) {
	return nil, fmt.Errorf("Not implied")
}
