package txnetwork

import (
	"container/list"
	"fmt"
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/util"
	pb "github.com/abchain/fabric/protos"
	"sync"
	"time"
)

const (
	def_maxPeer = 1024
)

//txnetworkglobal manage datas required by single gossip-base txnetwork: (peers, txs, etc...)
type txNetworkGlobal struct {
	notifies
	peers         *txNetworkPeers
	txPool        *transactionPool
	credvalidator cred.TxHandlerFactory
}

//if this is set, network will be created with default peer status,
//which is useful in testing and some other purpose (i.e. create a simple instance)
var DefaultInitPeer struct {
	Id    string
	State *pb.PeerTxState
}

func ToStringId(id []byte) string {
	return fmt.Sprintf("%x", id)
}

func CreateSimplePeer() (string, *pb.PeerTxState) {
	id := util.GenerateBytesUUID()
	if len(id) < TxDigestVerifyLen {
		panic("Wrong code generate uuid less than 16 bytes [128bit]")
	}

	return ToStringId(id), &pb.PeerTxState{
		Digest: id[:TxDigestVerifyLen],
		//add one byte to indicate this peer is endorsed
		Endorsement: []byte{1},
	}
}

func CreateTxNetworkGlobal() *txNetworkGlobal {

	sid := util.GenerateBytesUUID()
	if len(sid) < TxDigestVerifyLen {
		panic("Wrong code generate uuid less than 16 bytes [128bit]")
	}

	peers := &txNetworkPeers{
		maxPeers: def_maxPeer,
		lruQueue: list.New(),
		lruIndex: make(map[string]*list.Element),
	}

	l, err := ledger.GetLedger()
	if err != nil {
		logger.Warning("Could not get default ledger", err)
	}

	txPool := &transactionPool{
		ledger:  l,
		cCaches: make(map[string]*commitData),
	}

	if DefaultInitPeer.Id != "" {
		peers.selfId = DefaultInitPeer.Id
		logger.Infof("Create self peer [%s]", peers.selfId)
		//also create self peer
		peers.lruIndex[peers.selfId] = &list.Element{Value: &peerStatusItem{"", DefaultInitPeer.State, time.Now()}}
	}

	return &txNetworkGlobal{
		peers:  peers,
		txPool: txPool,
	}
}

type notifies struct {
	sync.RWMutex
	onupdate  []func(string, bool) error
	onevicted []func([]string) error
	onsetself []func(string, *pb.PeerTxState)
}

func (g *notifies) RegSetSelfPeer(f func(string, *pb.PeerTxState)) {
	g.Lock()
	defer g.Unlock()
	g.onsetself = append(g.onsetself, f)
}

func (g *notifies) RegEvictNotify(f func([]string) error) {
	g.Lock()
	defer g.Unlock()
	g.onevicted = append(g.onevicted, f)
}

func (g *notifies) RegUpdateNotify(f func(string, bool) error) {
	g.Lock()
	defer g.Unlock()
	g.onupdate = append(g.onupdate, f)
}

func (g *notifies) handleUpdate(id string, created bool) {
	ret := make(chan error)
	g.RLock()
	go func(onupdate []func(string, bool) error) {
		logger.Debugf("Trigger %d notifies for updating peer [%s], (create mode %v)", len(onupdate), id, created)
		for _, f := range onupdate {
			ret <- f(id, created)
		}

		close(ret)
	}(g.onupdate)
	g.RUnlock()

	for e := range ret {
		if e != nil {
			logger.Errorf("Handle update function fail: %s", e)
		}
	}
}

func (g *notifies) handleEvict(ids []string) {
	ret := make(chan error)
	g.RLock()
	go func(onevicted []func([]string) error) {
		logger.Debugf("Trigger %d notifies for evicting peers [%v]", len(onevicted), ids)
		for _, f := range onevicted {
			ret <- f(ids)
		}

		close(ret)
	}(g.onevicted)
	g.RUnlock()

	for e := range ret {
		if e != nil {
			logger.Errorf("Handle evicted function fail: %s", e)
		}
	}
}

func (g *notifies) handleSetSelf(id string, state *pb.PeerTxState) {
	g.RLock()
	fs := g.onsetself
	g.RUnlock()

	//set self is not need to spawn a thread because it always is called outside any model
	for _, f := range fs {
		f(id, state)
	}
}

type txNetworkPeers struct {
	maxPeers int
	sync.RWMutex
	selfId      string
	lruQueue    *list.List
	lruIndex    map[string]*list.Element
	peerHandler cred.TxHandlerFactory
}

func (g *txNetworkPeers) truncateTailPeer(cnt int) (ret []string) {

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

func (g *txNetworkPeers) AddNewPeer(id string) (ret *peerStatus, ids []string) {

	g.Lock()
	defer g.Unlock()

	//lruIndex has included self ID so we simply have blocked a malcious behavior
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

func (g *txNetworkPeers) RemovePeer(id string) bool {

	g.Lock()
	defer g.Unlock()

	item, ok := g.lruIndex[id]

	if ok {
		logger.Infof("gossip peer [%s] is removed", id)
		g.lruQueue.Remove(item)
		delete(g.lruIndex, id)
	}
	return ok
}

func (g *txNetworkPeers) ChangeSelf(id []byte) error {

	if len(id) < TxDigestVerifyLen {
		return fmt.Errorf("Endorser do not has a id long enough (%d bytes)", TxDigestVerifyLen)
	}

	g.Lock()
	defer g.Unlock()

	sid := ToStringId(id)

	_, ok := g.lruIndex[sid]
	if ok {
		return fmt.Errorf("ID %s has existed and can't not be use", id)
	}

	//old self peer is always being kept
	old, ok := g.lruIndex[g.selfId]
	g.lruQueue.MoveToFront(old)

	//also create self peer, notice we do not endorse it so new self
	//peer will not be propagated before a local-peer updating
	newself := &peerStatusItem{
		"",
		&pb.PeerTxState{
			Digest: id[:TxDigestVerifyLen],
		},
		time.Now()}

	logger.Infof("Set self peer to [%s]", sid)
	g.selfId = sid
	g.lruIndex[g.selfId] = &list.Element{Value: newself}

	return nil
}

func (g *txNetworkPeers) QueryPeer(id string) *pb.PeerTxState {
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

func (g *txNetworkPeers) QuerySelf() (*pb.PeerTxState, string) {

	g.RLock()
	defer g.RUnlock()
	if g.selfId == "" {
		return nil, ""
	}

	i, ok := g.lruIndex[g.selfId]
	if !ok {
		return nil, ""
	}

	//self is supposed to always own endorsements
	return i.Value.(*peerStatusItem).PeerTxState, g.selfId
}

func (g *txNetworkPeers) BlockPeer(id string) {
	g.RLock()
	defer g.RUnlock()
}

func (g *txNetworkPeers) TouchPeer(id string, status *pb.PeerTxState) {
	g.Lock()
	defer g.Unlock()

	if id == "" {
		//self is only updated from index by not in lru
		item := g.lruIndex[g.selfId]
		self := item.Value.(*peerStatusItem)
		self.PeerTxState = status

	} else {
		//notice scuttlebutt mode has blocked the real selfID being updated from outside
		if item, ok := g.lruIndex[id]; ok {
			if s, ok := item.Value.(*peerStatusItem); ok {
				s.PeerTxState = status
				s.lastAccess = time.Now()
				g.lruQueue.MoveToFront(item)
			}

		}
	}

}

type transactionPool struct {
	sync.RWMutex
	ledger    *ledger.Ledger
	txHandler cred.TxHandlerFactory
	cCaches   map[string]*commitData
}

func newTransactionPool(ledger *ledger.Ledger) *transactionPool {
	ret := new(transactionPool)
	ret.cCaches = make(map[string]*commitData)
	ret.ledger = ledger

	return ret
}

func (tp *transactionPool) AcquireCaches(peer string) TxCache {

	tp.RLock()

	c, ok := tp.cCaches[peer]

	if !ok {
		tp.RUnlock()

		c = new(commitData)
		tp.Lock()
		defer tp.Unlock()
		tp.cCaches[peer] = c
		return &txCache{c, peer, tp}
	}

	tp.RUnlock()
	return &txCache{c, peer, tp}
}

func (tp *transactionPool) RemoveCaches(peer string) {
	tp.Lock()
	defer tp.Unlock()

	delete(tp.cCaches, peer)
}

func (tp *transactionPool) ResetLedger() {
	tp.Lock()
	defer tp.Unlock()

	tp.cCaches = make(map[string]*commitData)
}

func (tp *transactionPool) ClearCaches() {
	tp.Lock()
	defer tp.Unlock()

	tp.cCaches = make(map[string]*commitData)
}
