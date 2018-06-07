package gossip_cat

import (
	"github.com/abchain/fabric/core/ledger"
)

// TxQuota struct
type HotTxCat struct {
	ledger *ledger.Ledger
	//	txMarkupStates map[string]*TxMarkupState

	// security state
	totalTxCount   int64
	invalidTxCount int64
	invalidTxTime  int64

	historyExpired int64 // seconds
	updateExpired  int64 // seconds
}

// // TxMarkupState struct
// type TxMarkupState struct {
// 	peerID  string
// 	txid    string
// 	catalog string
// 	time    int64
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
