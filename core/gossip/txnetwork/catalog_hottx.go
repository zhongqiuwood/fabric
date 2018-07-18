package txnetwork

import (
	"fmt"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
	proto "github.com/golang/protobuf/proto"
)

type txMemPoolItem struct {
	digest       []byte
	digestSeries uint64
	tx           *pb.Transaction //cache of the tx (may just the txid)
	committedH   uint64          //0 means not commited

	//so we have a simple list structure
	next *txMemPoolItem
}

type peerTxs struct {
	head *txMemPoolItem
	last *txMemPoolItem
}

type txPoolGlobalUpdateOut struct {
	*pb.Gossip_Digest
}

func (txPoolGlobalUpdateOut) Gossip_IsUpdateIn() bool { return false }

type txPoolGlobalUpdate struct {
	gossip.CatalogPeerPolicies
}

func (txPoolGlobalUpdate) Gossip_IsUpdateIn() bool {
	return true
}

type txPoolGlobal struct {
	ind        map[string]*txMemPoolItem
	currentCpo gossip.CatalogPeerPolicies
}

func (g *txPoolGlobal) GenDigest() model.Digest { return nil }
func (g *txPoolGlobal) MakeUpdate(d_in model.Digest) model.Update {

	d, ok := d_in.(*pb.Gossip_Digest)

	if !ok {
		panic("Type error, not Gossip_Digest")
	}

	return txPoolGlobalUpdateOut{d}
}

func (g *txPoolGlobal) Update(u_in model.Update) error {
	u, ok := u_in.(*txPoolGlobalUpdate)
	if !ok {
		panic("Type error, not txPoolGlobalUpdate")
	}

	g.currentCpo = u.CatalogPeerPolicies
	return nil
}

func (g *txPoolGlobal) NewPeer(id string) model.ScuttlebuttPeerStatus {

	//check if we have known this peer
	peerStatus := GetNetworkStatus().queryPeer(id)
	if peerStatus == nil {
		return nil
	}

	ret := new(peerTxMemPool)
	ret.reset(peerStatus.createPeerTxItem())
	return ret
}

func (g *txPoolGlobal) MissedUpdate(string, model.ScuttlebuttPeerUpdate) error {
	return nil
}

func (g *txPoolGlobal) index(i *txMemPoolItem) {
	g.ind[i.tx.GetTxid()] = i
}

func (g *txPoolGlobal) query(txid string) *txMemPoolItem {
	return g.ind[txid]
}

//return whether tx2 is precede of the digest of tx1
func txIsPrecede(digest []byte, tx2 *pb.Transaction) bool {
	//TODO: check tx2's nonce and tx1's txid
	return true
}

func (p *peerTxs) To() model.VClock {
	return standardVClock(p.last.digestSeries)
}

func (p *peerTxs) inRange(n uint64) bool {

	if p.head == nil {
		return false
	}

	return p.head.digestSeries <= n && p.last.digestSeries >= n

}

func (p *peerTxs) merge(s *peerTxs) {

	if p.last == nil {
		p.head = s.head
		p.last = s.last
		return
	}

	//scan txs ...
	for beg := s.head; beg != nil; beg = beg.next {
		if txIsPrecede(p.last.digest, beg.tx) {
			p.last.next = beg
			p.last = s.last
			break
		}
	}
}

func (p *peerTxs) fetch(d uint64, beg *txMemPoolItem) *peerTxs {

	if beg == nil {
		beg = p.head
	}

	for ; beg != nil; beg = beg.next {
		if beg.digestSeries != d {
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

func txToDigestState(tx *pb.Transaction) []byte {
	return tx.GetNonce()
}

//update structure used for recving
type txPeerUpdateIn struct {
	*pb.HotTransactionBlock
}

func (u *txPeerUpdateIn) To() model.VClock {
	if u.HotTransactionBlock == nil {
		return nil
	}

	return standardVClock(u.BeginSeries + uint64(len(u.Transactions)))
}

func (u *txPeerUpdateIn) toTxs(ledger *ledger.Ledger) (*peerTxs, error) {

	if len(u.Transactions) == 0 {
		return nil, nil
	}

	head := &txMemPoolItem{
		//		tx:           txs.Transactions[0],
		//		digest:       txToDigestState(txs.Transactions[0]),
		digestSeries: u.BeginSeries,
	}

	current := head
	var last *txMemPoolItem
	var err error

	//construct a peerTxMemPool from txs, and do verification
	for _, tx := range u.Transactions {

		if isLiteTx(tx) {
			tx, err = ledger.GetCommitedTransaction(tx.GetTxid())
			if err != nil {
				// this is not consider as error of update
				logger.Error("Checking tx from db fail", err)
				return nil, nil
			} else if tx == nil {
				return nil, fmt.Errorf("update give uncommited transactions")
			}
		}

		if txcommon.secHelper != nil {
			tx, err = txcommon.secHelper.TransactionPreValidation(tx)
			if err != nil {
				return nil, fmt.Errorf("update have invalid transactions: %s", err)
			}
		}

		last = current
		current.tx = tx
		current.digest = txToDigestState(tx)
		current = &txMemPoolItem{digestSeries: current.digestSeries + 1}
		last.next = current
	}
	last.next = nil //seal the tail

	return &peerTxs{head, last}, nil
}

//so we make two indexes for pooling txs: the global one (by txid) and a per-peer
//jumping list (by seqnum) with [1/jumplistInterval] size of the global indexs,
//and finally we have a 100/jumplistInterval % overhead
const (
	jumplistInterval = uint64(8)
)

type peerTxMemPool struct {
	*peerTxs
	peerId string
	//index help us to seek by a txid, it do not need to consensus with the size
	//field in peerTxs
	jlindex map[uint64]*txMemPoolItem
}

func (p *peerTxMemPool) reset(txbeg *txMemPoolItem) {
	p.peerTxs = &peerTxs{txbeg, nil}
	p.jlindex = make(map[uint64]*txMemPoolItem)

	if txbeg != nil {
		for ; txbeg != nil; txbeg = txbeg.next {
			if txbeg.digestSeries == (txbeg.digestSeries/jumplistInterval)*jumplistInterval {
				p.jlindex[txbeg.digestSeries/jumplistInterval] = txbeg
			}
		}

		p.peerTxs.last = txbeg
	}
}

func (p *peerTxMemPool) PickFrom(d_in model.VClock, _notused model.Update) (model.ScuttlebuttPeerUpdate, model.Update) {

	d, ok := d_in.(standardVClock)
	if !ok {
		panic("Type error, not Gossip_Digest_PeerState")
	}

	if !p.inRange(uint64(d)) {
		//we have a up-to-date or out-of-range state so no update can provided
		return nil, _notused
	}

	return p.fetch(uint64(d), p.jlindex[uint64(d)/jumplistInterval]), _notused
}

func (p *peerTxMemPool) Update(u_in model.ScuttlebuttPeerUpdate, g_in model.ScuttlebuttStatus) error {

	if u_in == nil {
		return nil
	}

	u, ok := u_in.(*txPeerUpdateIn)
	if !ok {
		panic("Type error, not txPeerUpdateIn")
	}

	g, ok := g_in.(*txPoolGlobal)
	if !ok {
		panic("Type error, not txPoolGlobal")
	}

	ledger, err := ledger.GetLedger()
	if err != nil {
		return err
	}

	inTxs, err := u.toTxs(ledger)

	if err != nil {
		return err
	} else if inTxs != nil {
		//there is many possible for a far-end provided nonsense update for you
		//(caused by itself or the original peer) so we just simply omit it
		return nil
	}

	//response said our data is outdate
	if inTxs.head.digestSeries > p.last.digestSeries {
		//check if we need to update
		peerStatus := GetNetworkStatus().queryPeer(p.peerId)
		if peerStatus == nil {
			//boom ...
			return fmt.Errorf("Unknown status in global for peer %s", p.peerId)
		}

		//the incoming MAYBE an valid, known digest, we just update our status
		if peerStatus.beginTxSeries >= inTxs.head.digestSeries {
			logger.Warningf("Tx chain in peer %s is outdate (%x@[%v]), reset it",
				p.peerId, p.last.digest, p.last.digestSeries)
			//all data we cache is clear ....
			p.reset(peerStatus.createPeerTxItem())
		}
	}

	oldlast := p.last
	p.merge(inTxs)

	var mergeCnt int
	//we need to construct the index ...
	for beg := oldlast.next; beg != nil; beg = beg.next {
		g.index(beg)
		if beg.digestSeries == (beg.digestSeries/jumplistInterval)*jumplistInterval {
			p.jlindex[beg.digestSeries/jumplistInterval] = beg
		}
		mergeCnt++

		//also add transaction into ledger
		ledger.PoolTransaction(beg.tx)
	}

	var mergeW uint
	if len(u.Transactions) < int(cat_hottx_one_merge_weight) {
		mergeW = uint(len(u.Transactions))
	} else {
		mergeW = cat_hottx_one_merge_weight
	}
	//scoring the peer (how many new txs have the update provided )
	g.currentCpo.ScoringPeer(mergeCnt*100/len(u.Transactions), mergeW)

	return nil
}

// func (u *txPoolUpdate) Add(id string, s_in model.Status) model.Update {

// 	s, ok := s_in.(*peerTxs)

// 	if !ok {
// 		panic("Type error, not peerTxs")
// 	}

// 	if s.head == nil {
// 		panic("Code give us empty status")
// 	}

// 	//we need to tailer the incoming peerTxs: clear the tx content for
// 	//tx which have commited before epoch, and restrict the size of
// 	//total update
// 	rec := &pb.HotTransactionBlock{BeginSeries: s.head.digestSeries}
// 	for beg := s.head; beg != nil; beg = beg.next {

// 		//TODO: estimate the size of update and interrupt it if size
// 		//is exceeded

// 		if beg.committedH <= u.epochH {
// 			rec.Transactions = append(rec.Transactions, getLiteTx(beg.tx))
// 		} else {
// 			rec.Transactions = append(rec.Transactions, beg.tx)
// 		}
// 	}

// 	u.Txs[id] = rec
// 	return u
// }

type hotTxCat struct {
	policy gossip.CatalogPolicies
	self   gossip.CatalogHandler
	//	txMarkupStates map[string]*TxMarkupState
	*txPoolGlobal

	// security state
	totalTxCount   int64
	invalidTxCount int64
	invalidTxTime  int64

	historyExpired int64 // seconds
	updateExpired  int64 // seconds
}

func init() {
	gossip.RegisterCat = append(gossip.RegisterCat, initHotTx)
}

func initHotTx(stub *gossip.GossipStub) {

	hotTx := new(hotTxCat)
	hotTx.policy = gossip.NewCatalogPolicyDefault()

	m := model.NewGossipModel(model.NewScuttlebuttStatus(hotTx))

	gossip.NewCatalogHandlerImpl(stub.GetSStub(),
		stub.GetStubContext(), nil, m)

	// hotTx.self = stub.AddDefaultCatalogHandler(hotTx)
	// registerEvictFunc(hotTx.self)
}

const (
	hotTxCatName = "openedTx"
)

func AddTransaction(tx *pb.Transaction) error {

	// h := gossip.GetGossip().GetCatalogHandler(hotTxCatName)
	// if h == nil {
	// 	return fmt.Error("Gossip handler for hot (open) tx is not availiable")
	// }

	return nil
}

//Implement for CatalogHelper
func (c *hotTxCat) Name() string                        { return hotTxCatName }
func (c *hotTxCat) GetPolicies() gossip.CatalogPolicies { return c.policy }

func (c *hotTxCat) TransDigestToPb(d_in model.Digest) *pb.Gossip_Digest {

	d, ok := d_in.(model.ScuttlebuttDigest)
	if !ok {
		panic("Type error, not ScuttlebuttDigest")
	}

	msg := new(pb.Gossip_Digest)

	ledger, err := ledger.GetLedger()
	if err == nil {
		msg.Epoch, err = ledger.GetCurrentStateHash()
	}

	if err != nil {
		//we can still emit the digest, but should log the problem
		logger.Error("Ledger get current statehash fail:", err)
	}

	msg.Data = make(map[string]*pb.Gossip_Digest_PeerState)

	for id, pd := range d.PeerDigest() {

		msg.Data[id] = &pb.Gossip_Digest_PeerState{
			Num: uint64(pd.(standardVClock)),
		}
	}

	return msg

}

func (c *hotTxCat) TransPbToDigest(msg *pb.Gossip_Digest) model.Digest {

	dout := model.NewscuttlebuttDigest(msg)

	for id, ps := range msg.Data {
		dout.SetPeerDigest(id, standardVClock(ps.GetNum()))
	}

	return dout
}

func (c *hotTxCat) UpdateMessage() proto.Message { return new(pb.Gossip_Tx) }

func getLiteTx(tx *pb.Transaction) *pb.Transaction {
	return &pb.Transaction{Txid: tx.GetTxid()}
}

func isLiteTx(tx *pb.Transaction) bool {
	return tx.GetNonce() == nil
}

func (c *hotTxCat) EncodeUpdate(cpo gossip.CatalogPeerPolicies, u_in model.Update, msg_in proto.Message) proto.Message {

	u, ok := u_in.(model.ScuttlebuttUpdate)

	if !ok {
		panic("Type error, not ScuttlebuttUpdate")
	}

	gu, ok := u.GlobalUpdate().(txPoolGlobalUpdateOut)

	if !ok {
		panic("Type error, not txPoolGlobalUpdateOut")
	}

	msg, ok := msg_in.(*pb.Gossip_Tx)
	if !ok {
		panic("Type error, not Gossip_Tx")
	}

	msg.Txs = make(map[string]*pb.HotTransactionBlock)

	var epochH uint64

	ledger, err := ledger.GetLedger()
	if err == nil {
		epochH, err = ledger.GetBlockNumberByState(gu.GetEpoch())
	}

	if err != nil {
		logger.Warning("Get epoch height fail, encode all transactions")
	}

	//encode txs
	for id, pu_in := range u.PeerUpdate() {
		pu, ok := pu_in.(*peerTxs)
		if !ok {
			panic("Type error, not peerTxs")
		}

		if pu.head == nil {
			continue
		}

		ptxs := new(pb.HotTransactionBlock)
		ptxs.BeginSeries = pu.head.digestSeries

		//TODO: we still need to tailer the incoming peerTxs: restrict the size of
		//total update by cpo

		for beg := pu.head; beg != nil; beg = beg.next {

			if beg.committedH <= epochH {
				ptxs.Transactions = append(ptxs.Transactions, getLiteTx(beg.tx))
			} else {
				ptxs.Transactions = append(ptxs.Transactions, beg.tx)
			}

		}

		msg.Txs[id] = ptxs
	}

	return msg
}

func (c *hotTxCat) DecodeUpdate(cpo gossip.CatalogPeerPolicies, msg_in proto.Message) (model.Update, error) {

	msg, ok := msg_in.(*pb.Gossip_Tx)
	if !ok {
		panic("Type error, not Gossip_Tx")
	}

	var err error
	ledger, err := ledger.GetLedger()
	if err != nil {
		logger.Error("Get ledger fail!", err)
		//not the fault of message
		return nil, nil
	}

	u := model.NewscuttlebuttUpdate(txPoolGlobalUpdate{cpo})

	for id, txs := range msg.Txs {
		for _, tx := range txs.Transactions {
			if isLiteTx(tx) {
				tx, err = ledger.GetCommitedTransaction(tx.GetTxid())
			}
		}

		u.UpdatePeer(id, nil)
	}

	return u, nil
}

// func (c *hotTxCat) UpdateNewPeer(id string, d model.Digest) model.Status {

// 	peer := GetNetworkStatus().queryPeer(id)
// 	if peer == nil {
// 		//peer id is unknown for global, so reject it
// 		return nil
// 	}

// 	ret := &peerTxMemPool{peerId: id}
// 	ret.init(peer.createPeerTxs())

// 	return ret
// }

// func (c *hotTxCat) SelfStatus() (string, model.Status) {

// 	self := GetNetworkStatus().getSelfStatus()
// 	ret := &peerTxMemPool{peerId: self.peerId}
// 	ret.init(self.createPeerTxs())

// 	return self.peerId, ret
// }

// func (c *hotTxCat) AssignUpdate(cpo gossip.CatalogPeerPolicies, d *pb.Gossip_Digest) model.Update {

// 	var height uint64
// 	ledger, err := ledger.GetLedger()
// 	if err == nil {
// 		height, err = ledger.GetBlockNumberByState(d.GetEpoch())
// 		if err != nil {
// 			height = uint64(0)
// 		}
// 	}

// 	if err != nil {
// 		logger.Error("Obtain blocknumber fail:", err)
// 	}

// 	return &txPoolUpdate{
// 		Gossip_Tx: &pb.Gossip_Tx{},
// 		cpo:       cpo,
// 		epochH:    height,
// 	}
// }

// func (c *hotTxCat) EncodeUpdate(u_in model.Update) ([]byte, error) {

// 	u, ok := u_in.(*txPoolUpdate)
// 	if !ok {
// 		panic("type error, not txPoolUpdate")
// 	}

// 	return proto.Marshal(u.Gossip_Tx)
// }

// func (c *hotTxCat) DecodeUpdate(cpo gossip.CatalogPeerPolicies, b []byte) (model.Update, error) {

// 	gossipTx := &pb.Gossip_Tx{}
// 	if err := proto.Unmarshal(b, gossipTx); err != nil {
// 		return nil, err
// 	}

// 	return &txPoolUpdate{
// 		Gossip_Tx: gossipTx,
// 		cpo:       cpo,
// 	}, nil
// }

// func (c *hotTxCat) ToProtoDigest(dm map[string]model.Digest) *pb.Gossip_Digest {

// 	d_out := &pb.Gossip_Digest{Data: make(map[string]*pb.Gossip_Digest_PeerState)}

// 	var ok bool
// 	for id, d := range dm {
// 		d_out.Data[id], ok = d.(*pb.Gossip_Digest_PeerState)
// 		if !ok {
// 			panic("type error, not Gossip_Digest_PeerState")
// 		}
// 	}

// 	ledger, err := ledger.GetLedger()
// 	if err == nil {
// 		d_out.Epoch, err = ledger.GetCurrentStateHash()
// 	}

// 	if err != nil {
// 		//we can still emit the digest, but should log the problem
// 		logger.Error("Ledger get current statehash fail:", err)
// 	}

// 	return d_out
// }

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
