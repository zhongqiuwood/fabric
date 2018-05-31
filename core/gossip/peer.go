package gossip

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	logging "github.com/op/go-logging"
)

var logger = logging.MustGetLogger("gossip")

// // Gossip interface
// type Gossip interface {
// 	BroadcastTx([]*pb.Transaction) error
// 	SetTxQuota(quota TxQuota) TxQuota
// 	NotifyTxError(txid string, err error)
// }

// // PeerHistoryMessage struct
// type PeerHistoryMessage struct {
// 	time    int64
// 	size    int64
// 	updated bool
// }

// // TxMarkupState struct
// type TxMarkupState struct {
// 	peerID  string
// 	txid    string
// 	catalog string
// 	time    int64
// }

//GossipStub struct
type GossipStub struct {
	*pb.StreamStub
	peer.Discoverer
	ledger          *ledger.Ledger
	catalogHandlers map[string]*catalogHandler
	peerActions     map[string]*PeerAction
	blackPeerIDs    map[string]int64
	txMarkupStates  map[string]*TxMarkupState
	txQuota         TxQuota
}

func init() {
	stub.DefaultFactory = func(id *pb.PeerID) stub.GossipHandler {
		logger.Debug("create handler for peer", id)

		// check black list
		btime, ok := gossipStub.blackPeerIDs[id.String()]
		if ok {
			logger.Infof("Block peer(%s) connection by black list from time(%d)", id, btime)
			return nil
		}

		action := &PeerAction{
			id:               id,
			messageHistories: []*PeerHistoryMessage{},
		}
		gossipStub.peerActions[id.String()] = action
		// send initial digests to target
		gossipStub.sendTxDigests(action, 1)
		return newHandler(id, gossipStub.StreamStub, gossipStub.catalogHandlers)
	}
}

// // HandleMessage method
// func (t *GossipHandler) HandleMessage(m *pb.Gossip) error {
// 	now := time.Now().Unix()
// 	p, ok := gossipStub.peerActions[t.peerID.String()]
// 	if !ok {
// 		return fmt.Errorf("Peer not found")
// 	}

// 	err := gossipStub.validatePeerMessage(p, m)
// 	if err != nil {
// 		return err
// 	}

// 	p.activeTime = now
// 	if m.GetDigest() != nil {
// 		// process digest
// 		err := gossipStub.model.applyDigest(t.peerID, m)
// 		if err != nil {
// 			p.invalidTxCount++
// 			p.invalidTxTime = now
// 		} else {
// 			// send digest if last diest send time ok
// 			gossipStub.sendTxDigests(p, 1)

// 			// mark and send update to peer
// 			p.digestResponseTime = now
// 			empty := []*pb.Transaction{}
// 			gossipStub.sendTxUpdates(p, empty, 1)
// 		}
// 	} else if m.GetUpdate() != nil {
// 		// process update
// 		txs, err := gossipStub.model.applyUpdate(t.peerID, m)
// 		if err != nil {
// 			p.invalidTxCount++
// 			p.invalidTxTime = now
// 		} else {
// 			p.digestResponseTime = now
// 			p.totalTxCount += int64(len(txs))
// 			gossipStub.ledger.PutTransactions(txs)
// 			gossipStub.updatePeerQuota(p, m.Catalog, int64(len(m.GetUpdate().Payload)), txs)
// 		}
// 	}
// 	return nil
// }

// // Stop method
// func (t *GossipHandler) Stop() {
// 	// remove peer from actions
// 	_, ok := gossipStub.peerActions[t.peerID.String()]
// 	if ok {
// 		delete(gossipStub.peerActions, t.peerID.String())
// 	}
// }

var gossipStub *GossipStub

// NewGossip : init the singleton of gossipstub
func NewGossip(p peer.Peer) {

	nb, err := p.GetNeighbour()
	logger.Debug("Gossip module inited")

	if err != nil {

		logger.Errorf("No neighbour for this peer (%s), gossip run without access control", err)
		gossipStub = &GossipStub{
			StreamStub: p.GetStreamStub("gossip"),
			model:      model,
		}

	} else {

		dis, err := nb.GetDiscoverer()
		if err != nil {
			logger.Errorf("No discovery for this peer (%s), gossip run without access control", err)
		}

		gossipStub = &GossipStub{
			StreamStub: p.GetStreamStub("gossip"),
			Discoverer: dis,
			model:      model,
		}
	}

	peerID := ""
	mypeer, err := p.GetPeerEndpoint()
	if err != nil {
		peerID = mypeer.ID.String()
	}

	lg, err := ledger.GetLedger()
	if err != nil {
		logger.Errorf("Get ledger error: %s", err)
		return
	}
	state, err := lg.GetCurrentStateHash()
	if err != nil {
		logger.Errorf("Get current state hash error: %s", err)
		return
	}
	chain, err := lg.GetBlockchainInfo()
	if err != nil {
		logger.Errorf("Get block chain info error: %s", err)
		return
	}
	block, err := lg.GetBlockByNumber(chain.Height - 1)
	if err != nil {
		logger.Errorf("Get block info info error: %s", err)
		return
	}

	number := len(block.Txids)
	logger.Infof("Peer(%s), current state hash(%x/%x), block height(%d), number(%d)",
		peerID, state, chain.CurrentBlockHash, chain.Height, number)

	gossipStub.ledger = lg
	gossipStub.peerActions = map[string]*PeerAction{}
	gossipStub.blackPeerIDs = map[string]int64{}
	gossipStub.txMarkupStates = map[string]*TxMarkupState{}
	gossipStub.model.init(peerID, state, uint64(number))

	// default quota
	gossipStub.txQuota.maxDigestRobust = 100
	gossipStub.txQuota.maxDigestPeers = 100
	gossipStub.txQuota.maxMessageSize = 100 * 1024 * 1024 // 100MB
	gossipStub.txQuota.historyExpired = 600               // 10 minutes
	gossipStub.txQuota.updateExpired = 30                 // 30 seconds
}

// GetGossip - gives a reference to a 'singleton' GossipStub
func GetGossip() Gossip {

	return gossipStub
}

// // BroadcastTx method
// func (s *GossipStub) BroadcastTx(txs []*pb.Transaction) error {
// 	state, err := s.ledger.GetCurrentStateHash()
// 	if err != nil {
// 		return err
// 	}

// 	// update self state
// 	s.model.updateSelfTxs(state, txs)

// 	// update self state to 3 other peers
// 	s.sendTxDigests(nil, 3)

// 	// broadcast tx to other peers
// 	s.sendTxUpdates(nil, txs, 3)
// 	return nil
// }

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
