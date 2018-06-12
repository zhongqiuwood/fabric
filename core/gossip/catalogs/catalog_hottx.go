package gossip_cat

import (
	_ "bytes"
	"fmt"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
)

type txMemPoolItem struct {
	tx         *pb.Transaction //cache of the tx (may just the txid)
	committedH uint64          //0 means not commited

	//so we have a simple list structure
	next *txMemPoolItem
}

type peerTxs struct {
	head *txMemPoolItem
	last *txMemPoolItem
}

func txToDigestState(tx *pb.Transaction) []byte {
	return []byte(tx.GetTxid())
}

func digestToTxid(d *pb.Gossip_Digest_PeerState) string {
	return string(d.GetState())
}

//return whether tx2 is precede of tx1
func txIsPrecede(tx1 *pb.Transaction, tx2 *pb.Transaction) bool {
	//TODO: check tx2's nonce and tx1's txid
	return false
}

func (p *peerTxs) GenDigest() model.Digest {
	return &pb.Gossip_Digest_PeerState{State: txToDigestState(p.last.tx)}
}

func (p *peerTxs) Merge(s_in model.Status) error {

	if s_in == nil {
		return nil
	}

	s, ok := s_in.(*peerTxs)
	if !ok {
		panic("Type error, not peerTxs")
	}

	//scan txs ...
	for beg := s.head; beg != nil; beg = beg.next {
		if txIsPrecede(p.last.tx, beg.tx) {
			p.last.next = beg
			p.last = s.last
			break
		}
	}

	// //scoring the peer (how many new txs have the update provided )
	// s.cpo.ScoringPeer((s.size-duplicatedCnt)*100/s.size,
	// 	cat_hottx_one_merge_weight)

	return nil
}

func (p *peerTxs) MakeUpdate(d_in model.Digest) model.Status {

	d, ok := d_in.(*pb.Gossip_Digest_PeerState)
	if !ok {
		panic("Type error, not Gossip_Digest_PeerState")
	}

	beg := p.head
	for ; beg != nil; beg = beg.next {
		if digestToTxid(d) == beg.tx.GetTxid() {
			beg = beg.next
			break
		}
	}

	if beg == nil {
		return nil
	} else {
		return &peerTxs{head: beg, last: p.last}
	}

}

type peerTxMemPool struct {
	peerTxs
	//index help us to seek by a txid, it do not need to consensus with the size
	//field in peerTxs
	index map[string]*txMemPoolItem
}

//overload MakeUpdate@peerTxs to achieve an O(1) process
func (p *peerTxMemPool) MakeUpdate(d_in model.Digest) model.Status {

	d, ok := d_in.(*pb.Gossip_Digest_PeerState)
	if !ok {
		panic("Type error, not Gossip_Digest_PeerState")
	}

	pos, ok := p.index[digestToTxid(d)]

	if !ok || pos == p.last {
		//we have a up-to-date or out-of-range state so no update can provided
		return nil
	} else {
		return &peerTxs{head: pos.next, last: p.last}
	}
}

//update structure for each known peer used in recving
type peerTxPoolUpdate struct {
	cpo  gossip.CatalogPeerPolicies
	size int
	*peerTxs
}

func (p *peerTxMemPool) Merge(s_in model.Status) error {

	if s_in == nil {
		return nil
	}

	s, ok := s_in.(*peerTxPoolUpdate)
	if !ok {
		panic("Type error, not peerTxPoolUpdate")
	}

	oldlast := p.last

	err := p.peerTxs.Merge(s.peerTxs)
	if err != nil {
		return err
	}

	var mergeCnt int
	//we need to construct the index ...
	for beg := oldlast; beg != nil; beg = beg.next {
		p.index[beg.tx.GetTxid()] = beg
		mergeCnt++
	}

	if mergeCnt > s.size {
		return fmt.Errorf("Wrong update size, have %d but merge %d", s.size, mergeCnt)
	}

	//scoring the peer (how many new txs have the update provided )
	s.cpo.ScoringPeer(mergeCnt*100/s.size, cat_hottx_one_merge_weight)

	return nil
}

//update structure used for recving
type txPoolUpdate struct {
	*pb.Gossip_Tx
	cpo    gossip.CatalogPeerPolicies
	epochH uint64
}

func (u *txPoolUpdate) PickUp(id string) model.Status {
	txs, ok := u.Txs[id]

	if !ok {
		return nil
	}

	if len(txs.Transactions) == 0 {
		//providing empty update is somewhat evil so it take a low score
		u.cpo.ScoringPeer(50, cat_hottx_one_merge_weight)
		return nil
	}

	head := &txMemPoolItem{tx: txs.Transactions[0]}

	ret := &peerTxs{
		head: head,
		last: head,
	}

	//construct a peerTxMemPool from txs, and do verification
	for _, tx := range txs.Transactions[1:] {
		//TODO: verify tx, if fail, we must return error
		//(if tx is commited, we should pick it from ledger and do verify)
		//

		ret.last.next = &txMemPoolItem{tx: tx}
		ret.last = ret.last.next
	}

	return &peerTxPoolUpdate{u.cpo, len(txs.Transactions), ret}
}

func getLiteTx(tx *pb.Transaction) *pb.Transaction {
	return &pb.Transaction{Txid: tx.GetTxid()}
}

func (u *txPoolUpdate) Add(id string, s_in model.Status) model.Update {

	s, ok := s_in.(*peerTxs)

	if !ok {
		panic("Type error, not peerTxs")
	}

	//we need to tailer the incoming peerTxs: clear the tx content for
	//tx which have commited before epoch, and restrict the size of
	//total update
	rec := &pb.TransactionBlock{}
	for beg := s.head; beg != nil; beg = beg.next {

		//TODO: estimate the size of update and interrupt it if size
		//is exceeded

		if beg.committedH <= u.epochH {
			rec.Transactions = append(rec.Transactions, getLiteTx(beg.tx))
		} else {
			rec.Transactions = append(rec.Transactions, beg.tx)
		}
	}

	return u
}

type hotTxCat struct {
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
