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

func init() {
	stub.DefaultFactory = func(id *pb.PeerID) stub.GossipHandler {
		logger.Debug("create handler for peer", id)

		return newHandler(id, gossipStub.catalogHandlers)
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

func (g *GossipStub) AddDefaultCatalogHandler(helper CatalogHelper) CatalogHandler {

	h := NewCatalogHandlerImpl(g.StreamStub, helper)

	g.AddCatalogHandler(helper.Name(), h)

	return h
}

var gossipStub *GossipStub
var RegisterCat []func(*GossipStub)

// NewGossip : init the singleton of gossipstub
func NewGossip(p peer.Peer) {

	self, err := p.GetPeerEndpoint()
	if err != nil {
		panic("No self endpoint")
	}

	gossipStub = &GossipStub{
		self:       self.ID,
		StreamStub: p.GetStreamStub("gossip"),
	}

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

	logger.Debug("Gossip module inited")

}

// GetGossip - gives a reference to a 'singleton' GossipStub
func GetGossip() *GossipStub {

	return gossipStub
}

// // NotifyTxError method
// func (s *GossipStub) NotifyTxError(txid string, err error) {
// 	// TODO: markup tx
// 	if err == nil {
// 		return
// 	}

// 	markup, ok := s.txMarkupStates[txid]
// 	if !ok {
// 		return
// 	}

// 	delete(s.txMarkupStates, txid)
// 	peer, ok := s.peerActions[markup.peerID]
// 	if !ok {
// 		return
// 	}

// 	peer.invalidTxCount++
// 	peer.invalidTxTime = time.Now().Unix()
// }

// // SetTxQuota method, return old quota
// func (s *GossipStub) SetTxQuota(quota TxQuota) TxQuota {
// 	old := s.txQuota
// 	s.txQuota = quota
// 	return old
// }

// // SetModelMerger method
// func (s *GossipStub) SetModelMerger(merger VersionMergerInterface) {
// 	s.model.setMerger(merger)
// }

// func (s *GossipStub) sendTxDigests(refer *PeerAction, maxn int) {
// 	var now = time.Now().Unix()
// 	var targetIDs []*pb.PeerID
// 	if refer != nil && refer.digestSendTime+10 < now {
// 		targetIDs = append(targetIDs, refer.id)
// 	}

// 	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	for len(targetIDs) < maxn {
// 		number := 0
// 		rindex := rnd.Intn(len(s.peerActions))
// 		macthed := false
// 		for _, peer := range s.peerActions {
// 			if refer != nil && peer.id == refer.id {
// 				number++
// 				continue
// 			}
// 			if number == rindex {
// 				macthed = true
// 				targetIDs = append(targetIDs, peer.id)
// 			}
// 			number++
// 		}
// 		if !macthed {
// 			break
// 		}
// 	}

// 	referID := ""
// 	if refer != nil {
// 		referID = refer.id.String()
// 	}

// 	if len(targetIDs) == 0 {
// 		logger.Debugf("No digest need to send to any peers, with refer(%s)", referID)
// 		return
// 	}

// 	handlers := s.PickHandlers(targetIDs)
// 	message := s.model.digestMessage("tx", 0)
// 	for i, handler := range handlers {
// 		id := targetIDs[i] // TODO: len(handlers) < len(targetIDs)
// 		err := handler.SendMessage(message)
// 		if err != nil {
// 			logger.Errorf("Send digest to peer(%s) failed: %s", id.String(), err)
// 		} else {
// 			s.peerActions[id.String()].digestSendTime = now
// 		}
// 	}
// }

// func (s *GossipStub) sendTxUpdates(referer *PeerAction, txs []*pb.Transaction, maxn int) error {
// 	var now = time.Now().Unix()
// 	var targetIDs []*pb.PeerID
// 	if referer != nil && referer.updateSendTime+10 < now {
// 		targetIDs = append(targetIDs, referer.id)
// 	}

// 	if len(targetIDs) == 0 || len(txs) > 0 {
// 		// no targets or txs not empty
// 		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
// 		for len(targetIDs) < maxn {
// 			number := 0
// 			rindex := rnd.Intn(len(s.peerActions))
// 			macthed := false
// 			for _, peer := range s.peerActions {
// 				if referer != nil && peer.id == referer.id {
// 					number++
// 					continue
// 				}
// 				if number == rindex {
// 					macthed = true
// 					targetIDs = append(targetIDs, peer.id)
// 				}
// 				number++
// 			}
// 			if !macthed {
// 				break
// 			}
// 		}
// 	}

// 	refererID := ""
// 	if referer != nil {
// 		refererID = referer.id.String()
// 	}
// 	if len(targetIDs) == 0 {
// 		logger.Debugf("No update need to send to any peers, with referer(%s)", refererID)
// 		return fmt.Errorf("No peers")
// 	}

// 	hash, err := s.ledger.GetCurrentStateHash()
// 	if err != nil {
// 		return err
// 	}
// 	if len(targetIDs) == 1 && len(txs) == 0 {
// 		// fill transactions with state
// 		txstate, err := s.model.getPeerTxState(targetIDs[0].String())
// 		if err != nil {
// 			logger.Debugf("No update need to send to peer(%s), with error(%s)", targetIDs[0], err)
// 			return err
// 		}
// 		// at most 3 txs
// 		ntxs, err := s.ledger.GetTransactionsByRange(txstate.hash, int(txstate.number)+1, int(txstate.number)+4)
// 		if err != nil || len(ntxs) == 0 {
// 			logger.Debugf("No update need to send to peer(%s), with error(%s)", targetIDs[0], err)
// 			return err
// 		}
// 		txs = ntxs
// 		hash = txstate.hash
// 	}

// 	if len(txs) == 0 {
// 		// no txs send
// 		return fmt.Errorf("No txs send")
// 	}

// 	handlers := s.PickHandlers(targetIDs)
// 	message := s.model.gossipTxMessage(hash, txs)
// 	for i, handler := range handlers {
// 		id := targetIDs[i] // TODO: len(handlers) < len(targetIDs)
// 		err := handler.SendMessage(message)
// 		if err != nil {
// 			logger.Errorf("Send update to peer(%s) failed: %s", id.String(), err)
// 		} else {
// 			s.peerActions[id.String()].updateSendTime = now
// 		}
// 	}

// 	return nil
// }

// func (s *GossipStub) validatePeerMessage(peer *PeerAction, message *pb.Gossip) error {

// 	size := int64(0)
// 	now := time.Now().Unix()

// 	// block for robust consideration
// 	if (peer.invalidTxCount > 3 && peer.invalidTxTime+s.txQuota.historyExpired > now) ||
// 		(peer.invalidTxCount > 10 && peer.invalidTxCount*5 > peer.totalTxCount) {
// 		// more than 3 invalid txs and the last invalid tx during 1 hour
// 		// or
// 		// more than 10 invalid txs and more than 20% invalid txs comparing with total txs
// 		// then add this peer to black list
// 		gossipStub.blackPeerIDs[peer.id.String()] = now
// 		return fmt.Errorf("Peer blocked by robust consideration")
// 	}

// 	if message.GetUpdate() != nil {
// 		size = int64(len(message.GetUpdate().Payload))
// 	} else if message.GetDigest() != nil {
// 		if len(message.GetDigest().Data) > s.txQuota.maxDigestPeers {
// 			return fmt.Errorf("Message blocked by max digest count(%d) overflow", s.txQuota.maxDigestPeers)
// 		}
// 	}

// 	// clear expired histories
// 	deleted := -1
// 	for i, item := range peer.messageHistories {
// 		if item.time+s.txQuota.updateExpired > now {
// 			break
// 		}
// 		deleted = i
// 	}
// 	if deleted >= 0 {
// 		peer.messageHistories = peer.messageHistories[deleted+1:]
// 	}

// 	// check total size quota
// 	totalSize := int64(0)
// 	digestCount := 0
// 	for _, item := range peer.messageHistories {
// 		totalSize += item.size
// 		if !item.updated {
// 			digestCount++
// 		}
// 	}
// 	if totalSize >= s.txQuota.maxMessageSize {
// 		return fmt.Errorf("Message blocked by total message size overflow quota")
// 	} else if digestCount > s.txQuota.maxDigestRobust {
// 		return fmt.Errorf("Message blocked by digest robust overflow quota")
// 	}

// 	// add to history
// 	peer.messageHistories = append(peer.messageHistories, &PeerHistoryMessage{
// 		time:    now,
// 		size:    size,
// 		updated: message.GetUpdate() != nil,
// 	})

// 	return nil
// }

// func (s *GossipStub) updatePeerQuota(peer *PeerAction, catalog string, size int64, txs []*pb.Transaction) {

// 	now := time.Now().Unix()
// 	expireTime := now - s.txQuota.historyExpired

// 	// clear expired
// 	expiredTxids := []string{}
// 	for _, markup := range s.txMarkupStates {
// 		if markup.time < expireTime {
// 			expiredTxids = append(expiredTxids, markup.txid)
// 		}
// 	}
// 	if len(expiredTxids) > 0 {
// 		logger.Debugf("Clear %d expired tx markup state items", len(expiredTxids))
// 		for _, txid := range expiredTxids {
// 			delete(s.txMarkupStates, txid)
// 		}
// 	}

// 	// update
// 	for _, tx := range txs {
// 		markup := &TxMarkupState{
// 			peerID:  peer.id.String(),
// 			txid:    tx.Txid,
// 			catalog: catalog,
// 			time:    now,
// 		}
// 		s.txMarkupStates[tx.Txid] = markup
// 	}
// }
