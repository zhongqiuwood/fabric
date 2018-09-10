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
	return standardVClock(s.GetNum())
}

func (s peerStatus) PickFrom(d_in model.VClock, u_in model.Update) (model.ScuttlebuttPeerUpdate, model.Update) {
	d, ok := d_in.(standardVClock)
	if !ok {
		panic("Type error, not standardVClock")
	}

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

	s.PeerTxState = u.PeerTxState

	//update global part ...
	g.Lock()
	defer g.Unlock()
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
	onevicted []func([]string)
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

func (g *txNetworkGlobal) addNewPeer(id string) *peerStatus {

	if _, ok := g.lruIndex[id]; ok {
		logger.Errorf("Request add duplicated peer [%s]", id)
		return nil
	}

	if len(g.lruIndex) > g.maxPeers {
		ids := g.truncateTailPeer(len(g.lruIndex) - g.maxPeers)
		//notify evicted
		for _, f := range g.onevicted {
			f(ids)
		}
	}

	//if we can't truncate anypeer, just give up
	if len(g.lruIndex) > g.maxPeers {
		logger.Errorf("Can't not add peer [%s], exceed limit %d", id, g.maxPeers)
		return nil
	}

	ret := &peerStatusItem{
		id,
		new(pb.PeerTxState),
		time.Time{},
	}
	g.lruIndex[id] = g.lruQueue.PushBack(ret)

	return &peerStatus{ret.PeerTxState}
}

func (g *txNetworkGlobal) NewPeer(id string) model.ScuttlebuttPeerStatus {
	g.Lock()
	defer g.Unlock()

	return g.addNewPeer(id)
}

func (g *txNetworkGlobal) RemovePeer(id string, _ model.ScuttlebuttPeerStatus) {

	g.Lock()
	defer g.Unlock()

	item, ok := g.lruIndex[id]

	if ok {
		g.lruQueue.Remove(item)
		delete(g.lruIndex, id)

		for _, f := range g.onevicted {
			f([]string{id})
		}
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

func (g *txNetworkGlobal) RegNotify(f func([]string)) {
	g.Lock()
	defer g.Unlock()
	g.onevicted = append(g.onevicted, f)
}

type networkIndexs struct {
	sync.Mutex
	ind map[*gossip.GossipStub]*txNetworkGlobal
}

func (g *networkIndexs) GetNetwork(stub *gossip.GossipStub) *txNetworkGlobal {
	g.Lock()
	defer g.Unlock()

	if n, ok := g.ind[stub]; ok {
		return n
	} else {
		return nil
	}
}

func CreateTxNetworkGlobal() *txNetworkGlobal {
	ret := &txNetworkGlobal{
		maxPeers: def_maxPeer,
		lruQueue: list.New(),
		lruIndex: make(map[string]*list.Element),
		selfId:   util.GenerateUUID(),
	}

	//also create self peer
	self := &peerStatusItem{
		"",
		&pb.PeerTxState{
			Digest: util.InitCryptoChainedBytes(),
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

func (g *networkIndexs) CreateNetwork(stub *gossip.GossipStub) *txNetworkGlobal {
	g.Lock()
	defer g.Unlock()

	if n, ok := g.ind[stub]; ok {
		return n
	}

	ret := CreateTxNetworkGlobal()
	g.ind[stub] = ret
	return ret
}

var global networkIndexs

func GetNetworkStatus() *networkIndexs { return &global }

type globalCat struct {
	policy gossip.CatalogPolicies
}

func init() {

	global.ind = make(map[*gossip.GossipStub]*txNetworkGlobal)
	gossip.RegisterCat = append(gossip.RegisterCat, initNetworkStatus)
}

func initNetworkStatus(stub *gossip.GossipStub) {

	network := global.CreateNetwork(stub)
	selfstatus := model.NewScuttlebuttStatus(network)

	selfstatus.SetSelfPeer(network.selfId, &peerStatus{network.QuerySelf()})
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
	for id, iu := range msg.Txs {
		u.UpdatePeer(id, peerStatus{iu})
	}

	return u, nil
}
