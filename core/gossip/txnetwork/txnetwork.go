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

func CreateTxNetworkGlobal() *txNetworkGlobal {

	sid := util.GenerateBytesUUID()
	if len(sid) < TxDigestVerifyLen {
		panic("Wrong code generate uuid less than 16 bytes [128bit]")
	}

	peers := &txNetworkPeers{
		maxPeers: def_maxPeer,
		lruQueue: list.New(),
		lruIndex: make(map[string]*list.Element),
		selfId:   fmt.Sprintf("%x", sid),
	}

	l, err := ledger.GetLedger()
	if err != nil {
		logger.Warning("Could not get default ledger", err)
	}

	txPool := &transactionPool{
		ledger:  l,
		cCaches: make(map[string]*commitData),
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

	logger.Infof("Create self peer [%s]", peers.selfId)
	item := &list.Element{Value: self}
	peers.lruIndex[peers.selfId] = item

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

func (g *txNetworkPeers) QuerySelf() *pb.PeerTxState {
	ret := g.QueryPeer(g.selfId)
	if ret == nil {
		panic(fmt.Errorf("Do not create self peer [%s] in network", g.selfId))
	}
	return ret
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
