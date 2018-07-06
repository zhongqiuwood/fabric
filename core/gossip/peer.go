package gossip

import (
	"github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/core/peer"
	peerACL "github.com/abchain/fabric/core/peer/acl"
	pb "github.com/abchain/fabric/protos"
	logging "github.com/op/go-logging"
	"sync"
	"time"
)

var logger = logging.MustGetLogger("gossip")

var globalSeq uint64
var globalSeqLock sync.Mutex

func getGlobalSeq() uint64 {

	globalSeqLock.Lock()
	defer globalSeqLock.Unlock()

	ref := uint64(time.Now().Unix())
	if ref > globalSeq {
		globalSeq = ref

	} else {
		globalSeq++
	}

	return globalSeq
}

//GossipStub struct
type GossipStub struct {
	self            *pb.PeerID
	disc            peer.Discoverer
	catalogHandlers map[string]CatalogHandler

	*pb.StreamStub
	peerACL.AccessControl
}

var gossipMapper = map[*pb.StreamStub]*GossipStub{}

//an default collection can be used ...
var defaultCatalogHandlers = map[string]CatalogHandler{}

func init() {
	stub.DefaultFactory = func(id *pb.PeerID, sstub *pb.StreamStub) stub.GossipHandler {
		logger.Debug("create handler for peer", id)

		ss, ok := gossipMapper[sstub]
		if !ok {
			logger.Warningf("Stream stub for peer [%s] is not registered in gossip module", id.GetName())
			return newHandler(id, defaultCatalogHandlers)
		} else {
			return newHandler(id, ss.catalogHandlers)
		}
	}
}

func (g *GossipStub) GetSelf() *pb.PeerID {
	return g.self
}

func (g *GossipStub) GetSStub() *pb.StreamStub {
	return g.StreamStub
}

func (g *GossipStub) GetCatalogHandler(cat string) CatalogHandler {
	return g.catalogHandlers[cat]
}

func (g *GossipStub) AddCatalogHandler(cat string, h CatalogHandler) {
	_, ok := g.catalogHandlers[cat]
	g.catalogHandlers[cat] = h

	if ok {
		logger.Errorf("Duplicated add handler for catalog %s", cat)
	} else {
		logger.Infof("Add handler for catalog %s", cat)
	}
}

//each call of NewGossipWithPeer will travel register collections to create the corresponding catalogy handlers
var RegisterCat []func(*GossipStub)

func NewGossipWithPeer(p peer.Peer) *GossipStub {

	self, err := p.GetPeerEndpoint()
	if err != nil {
		panic("No self endpoint")
	}

	gossipStub := &GossipStub{
		self:            self.ID,
		catalogHandlers: make(map[string]CatalogHandler),
		StreamStub:      p.GetStreamStub("gossip"),
	}

	gossipMapper[gossipStub.StreamStub] = gossipStub

	nb, err := p.GetNeighbour()

	if err != nil {
		logger.Errorf("No neighbour for this peer (%s), gossip run without access control", err)
	} else {
		gossipStub.disc, _ = nb.GetDiscoverer()
		gossipStub.AccessControl, _ = nb.GetACL()
	}

	//reg all catalogs
	for _, f := range RegisterCat {
		f(gossipStub)
	}

	logger.Info("A Gossip module inited")

	return gossipStub

}

var defaultGossipStub *GossipStub
var defaultGossipInit sync.Once

// GetGossip, pass nil to obtain the "default" gossip singleton
// *** current this method can only get the default one
func GetGossip(p peer.Peer) *GossipStub {

	defaultGossipInit.Do(func() {
		defaultGossipStub = NewGossipWithPeer(p)
	})

	return defaultGossipStub
}
