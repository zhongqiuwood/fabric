package txnetwork

import (
	"container/list"
	"sync"
)

var global *txNetworkGlobal

type peerStatus struct {
	peerId        string
	beginTxDigest []byte
	status        []byte
}

type txNetworkGlobal struct {
	sync.RWMutex
	lruQueue  *list.List
	lruIndex  map[string]*list.Element
	onevicted []func([]string)
}

func GetNetworkStatus() *txNetworkGlobal { return global }

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
