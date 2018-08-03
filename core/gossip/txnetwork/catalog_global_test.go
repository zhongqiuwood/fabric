package txnetwork

import (
	"container/list"
	"github.com/abchain/fabric/core/gossip"
)

func initGlobalStatus() {
	global = &txNetworkGlobal{
		maxPeers:  def_maxPeer,
		lruQueue:  list.New(),
		lruIndex:  make(map[string]*list.Element),
		selfpeers: make(map[*gossip.GossipStub]*selfPeerStatus),
	}
}
