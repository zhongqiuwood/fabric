package gossip

import (
	"github.com/abchain/fabric/core/peer"
	peerACL "github.com/abchain/fabric/core/peer/acl"
	pb "github.com/abchain/fabric/protos"
	logging "github.com/op/go-logging"
	"golang.org/x/net/context"
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

var GossipFactory func(*GossipStub) pb.StreamHandlerFactory

//the simplified stream handler
type GossipHandler interface {
	HandleMessage(*pb.Gossip) error
	Stop()
}

//GossipStub struct
type GossipStub struct {
	self            *pb.PeerID
	disc            peer.Discoverer
	sec             peer.SecurityAccessor
	catalogHandlers map[string]CatalogHandler
	newPeerNotify   []CatalogHandlerEx

	*pb.StreamStub
	peerACL.AccessControl
	globalCtx context.Context
}

func (g *GossipStub) CreateGossipHandler(id *pb.PeerID) GossipHandler {
	logger.Debug("create handler for peer", g.self)
	return newHandler(id, g.catalogHandlers)
}

func (g *GossipStub) GetSelf() *pb.PeerID {
	return g.self
}

func (g *GossipStub) GetSStub() *pb.StreamStub {
	return g.StreamStub
}

func (g *GossipStub) GetStubContext() context.Context {
	return g.globalCtx
}

func (g *GossipStub) GetSecurity() peer.SecurityAccessor {
	return g.sec
}

func (g *GossipStub) GetCatalogHandler(cat string) CatalogHandler {
	return g.catalogHandlers[cat]
}

func (g *GossipStub) AddCatalogHandler(h CatalogHandler) {
	_, ok := g.catalogHandlers[h.Name()]
	g.catalogHandlers[h.Name()] = h

	if ok {
		logger.Errorf("Duplicated add handler for catalog %s", h.Name())
	} else {
		logger.Infof("Add handler for catalog %s", h.Name())
	}
}

func (g *GossipStub) SubScribeNewPeerNotify(h CatalogHandler) {
	ex, ok := h.(CatalogHandlerEx)
	if !ok {
		panic("handler did not imply ex interface, wrong code")
	}

	g.newPeerNotify = append(g.newPeerNotify, ex)
}

func (g *GossipStub) NotifyNewPeer(peerid *pb.PeerID) {
	for _, h := range g.newPeerNotify {
		h.OnConnectNewPeer(peerid)
	}
}

//each call of NewGossipWithPeer will travel register collections to create the corresponding catalogy handlers
var RegisterCat []func(*GossipStub)

func NewGossipWithPeer(p peer.Peer) *GossipStub {

	cache.Do(cacheConfiguration)

	self, err := p.GetPeerEndpoint()
	if err != nil {
		panic("No self endpoint")
	}

	gctx, _ := context.WithCancel(p.GetPeerCtx())

	gossipStub := &GossipStub{
		self:            self.ID,
		catalogHandlers: make(map[string]CatalogHandler),
		//		StreamStub:      p.GetStreamStub("gossip"),
		sec:       p,
		globalCtx: gctx,
	}

	nb, err := p.GetNeighbour()

	if err != nil {
		logger.Errorf("No neighbour for this peer (%s), gossip run without access control", err)
	} else {
		gossipStub.disc, _ = nb.GetDiscoverer()
		gossipStub.AccessControl, _ = nb.GetACL()
	}

	//gossipStub itself is also a posthandler
	err = p.AddStreamStub("gossip", GossipFactory(gossipStub), gossipStub)
	if err != nil {
		logger.Error("Bind gossip stub to peer fail: ", err)
		return nil
	}

	gossipStub.StreamStub = p.GetStreamStub("gossip")
	if gossipStub.StreamStub == nil {
		//sanity check
		panic("When streamstub is succefully added, it should not vanish here")
	}

	//reg all catalogs
	for _, f := range RegisterCat {
		f(gossipStub)
	}

	logger.Infof("A Gossip module created on peer %s", self.ID)

	return gossipStub

}
