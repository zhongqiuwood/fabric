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

//a item is cloned without next
func (t *txMemPoolItem) clone() *txMemPoolItem {
	n := new(txMemPoolItem)
	*n = *t
	n.next = nil
	return n
}

type peerTxs struct {
	head *txMemPoolItem
	last *txMemPoolItem
}

//return whether tx2 is precede of the digest of tx1
func txIsPrecede(digest []byte, tx2 *pb.Transaction) bool {
	//TODO: check tx2's nonce and tx1's txid
	return true
}

func buildPrecededTx(digest []byte, tx *pb.Transaction) *pb.Transaction {
	//TODO: now we just put something in the nonce ...
	tx.Nonce = []byte{2, 3, 3}
	return tx
}

func (p *peerTxs) lastSeries() uint64 {
	if p == nil || p.last == nil {
		return 0
	} else {
		return p.last.digestSeries
	}
}

func (p *peerTxs) inRange(n uint64) bool {

	if p.head == nil {
		return false
	}

	return p.head.digestSeries <= n && p.last.digestSeries >= n

}

func (p *peerTxs) concat(s *peerTxs) error {

	if p.head == nil {
		p.head = s.head
		p.last = s.last
		return nil
	}

	if s.head.digestSeries != p.lastSeries()+1 {
		return fmt.Errorf("Wrong next series: %d", s.head.digestSeries)
	}

	if !txIsPrecede(p.last.digest, s.head.tx) {
		return fmt.Errorf("Wrong next section for digest %x", p.last.digest)
	}

	p.last.next = s.head
	p.last = s.last
	return nil
}

//have extra cost and we should avoid using it
func (p *peerTxs) merge(s *peerTxs) error {

	if p.head == nil {
		return p.concat(s)
	}

	if !s.inRange(p.last.digestSeries + 1) {
		//nothing to merge, give up
		return nil
	}

	//scan txs ...
	for beg := s.head; beg != nil; beg = beg.next {
		if beg.digestSeries > p.last.digestSeries {
			return p.concat(&peerTxs{beg, s.last})
		}
	}

	return nil
}

func (p *peerTxs) fetch(d uint64, beg *txMemPoolItem) *peerTxs {

	if beg == nil {
		beg = p.head
	}

	for ; beg != nil; beg = beg.next {
		if beg.digestSeries == d {
			return &peerTxs{head: beg, last: p.last}
		}
	}

	return nil

}

type txPoolGlobalUpdateOut uint64

func (txPoolGlobalUpdateOut) Gossip_IsUpdateIn() bool { return false }

type txPoolGlobalUpdate struct {
	gossip.CatalogPeerPolicies
}

func (txPoolGlobalUpdate) Gossip_IsUpdateIn() bool { return true }

type txPoolCommited struct {
	txs       []string
	commitedH uint64
}

func (*txPoolCommited) Gossip_IsUpdateIn() bool { return true }

type txPoolGlobal struct {
	ind        map[string]*txMemPoolItem
	ledger     *ledger.Ledger
	currentCpo gossip.CatalogPeerPolicies
}

type txPoolGlobalDigest struct {
	epoch []byte
}

func (g *txPoolGlobal) GenDigest() model.Digest {

	epoch, err := g.ledger.GetCurrentStateHash()
	if err != nil {
		logger.Errorf("Could not get epoch: %s", err)
		return nil
	}

	return txPoolGlobalDigest{epoch}
}
func (g *txPoolGlobal) MakeUpdate(d_in model.Digest) model.Update {

	d, ok := d_in.(*pb.Gossip_Digest)

	if !ok {
		panic("Type error, not Gossip_Digest")
	}

	epochH, err := g.ledger.GetBlockNumberByState(d.GetEpoch())
	if err != nil {
		logger.Warning("Get epoch height fail, encode all transactions")
		epochH = 0
	}

	return txPoolGlobalUpdateOut(epochH)
}

func (g *txPoolGlobal) Update(u_in model.Update) error {

	switch u := u_in.(type) {
	case txPoolGlobalUpdate:
		g.currentCpo = u.CatalogPeerPolicies
	case *txPoolCommited:
		for _, txid := range u.txs {
			if i, ok := g.ind[txid]; ok {
				i.committedH = u.commitedH
			}
		}
	default:
		return fmt.Errorf("Type error, not expected update type")
	}

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

func (g *txPoolGlobal) RemovePeer(s_in model.ScuttlebuttPeerStatus) {
	if s_in == nil {
		return
	}
	s, ok := s_in.(*peerTxMemPool)
	if !ok {
		panic("Type error, not peerTxMemPool")
	}

	//purge index ...
	for beg := s.head; beg != nil; beg = beg.next {
		delete(g.ind, beg.tx.GetTxid())
	}
}

func (g *txPoolGlobal) index(i *txMemPoolItem) {
	g.ind[i.tx.GetTxid()] = i
}

func (g *txPoolGlobal) query(txid string) *txMemPoolItem {
	return g.ind[txid]
}

func txToDigestState(tx *pb.Transaction) []byte {
	return tx.GetNonce()
}

//update structure used for recving
type txPeerUpdate struct {
	*pb.HotTransactionBlock
}

func (u txPeerUpdate) To() model.VClock {
	if u.HotTransactionBlock == nil {
		return nil
	}

	return standardVClock(u.BeginSeries + uint64(len(u.Transactions)))
}

func getLiteTx(tx *pb.Transaction) *pb.Transaction {
	return &pb.Transaction{Txid: tx.GetTxid()}
}

func isLiteTx(tx *pb.Transaction) bool {
	return tx.GetNonce() == nil
}

func (u txPeerUpdate) fromTxs(s *peerTxs, epochH uint64) {

	if s == nil {
		return
	}

	u.BeginSeries = s.head.digestSeries

	for beg := s.head; beg != nil; beg = beg.next {
		if beg.committedH != 0 && beg.committedH <= epochH {
			u.Transactions = append(u.Transactions, getLiteTx(beg.tx))
		} else {
			u.Transactions = append(u.Transactions, beg.tx)
		}
	}
}

func (u txPeerUpdate) toTxs(ledger *ledger.Ledger, refSeries uint64) (*peerTxs, error) {

	if len(u.Transactions) == 0 {
		return nil, nil
	}

	var headpos int
	if u.BeginSeries < refSeries {
		//possible panic if headpos exceed len of u.Transactions ?
		//scuttlebutt scheme will check To() first and avoid this case
		headpos = int(refSeries - u.BeginSeries)
	} else {
		//start from head, headpos is 0
		refSeries = u.BeginSeries
	}

	head := &txMemPoolItem{
		//		tx:           txs.Transactions[0],
		//		digest:       txToDigestState(txs.Transactions[0]),
		digestSeries: refSeries,
	}

	current := head
	var last *txMemPoolItem
	var err error

	//construct a peerTxMemPool from txs, and do verification
	for _, tx := range u.Transactions[headpos:] {

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
			p.last = txbeg
		}

	}
}

func (p *peerTxMemPool) To() model.VClock {
	return standardVClock(p.lastSeries())
}

func (p *peerTxMemPool) PickFrom(d_in model.VClock, gu_in model.Update) (model.ScuttlebuttPeerUpdate, model.Update) {

	d, ok := d_in.(standardVClock)
	if !ok {
		panic("Type error, not Gossip_Digest_PeerState")
	}

	gu, ok := gu_in.(txPoolGlobalUpdateOut)
	if !ok {
		panic("Type error, not txPoolGlobalUpdateOut")
	}

	expectedH := uint64(d) + 1
	ret := txPeerUpdate{new(pb.HotTransactionBlock)}

	if !p.inRange(expectedH) {
		//we have a too up-to-date range, so we return a single record to
		//remind far-end they are late
		nh := p.head.clone()
		ret.fromTxs(&peerTxs{nh, nh}, uint64(gu))
	} else {
		ret.fromTxs(p.fetch(expectedH, p.jlindex[uint64(d)/jumplistInterval]), uint64(gu))
	}

	return ret, gu_in
}

func (p *peerTxMemPool) purge(purgeto uint64, g *txPoolGlobal) {

	//never purge at begin
	if p.head == nil || purgeto <= p.head.digestSeries {
		return
	}

	//purge the outdate part
	//for zero, we just get UINT64_MAX and no index
	preserved := p.fetch(purgeto, p.jlindex[purgeto/jumplistInterval-1])
	if preserved == nil {
		logger.Warningf("got empty preserved, wrong purge request:", purgeto)
		return
	}

	//purge global index first
	for beg := p.head; beg != preserved.head; beg = beg.next {
		delete(g.ind, beg.tx.GetTxid())
	}

	//also the jumping index must be purged
	for i := p.head.digestSeries / jumplistInterval; i < purgeto/jumplistInterval; i++ {
		delete(p.jlindex, i)
	}

	//handling the "edge jumpling index"
	if purgeto/jumplistInterval*jumplistInterval < purgeto {
		delete(p.jlindex, purgeto/jumplistInterval)
	}

	p.peerTxs = preserved

}

// there are too many ways that a far-end can waste our bandwidth (and
// limited computational cost) without provding useful information,
// for example, fill some update under un-existed peer name
// so we given up scoring the peer until we have more solid process
// to verify the correctness of data from far-end
func (p *peerTxMemPool) Update(u_in model.ScuttlebuttPeerUpdate, g_in model.ScuttlebuttStatus) error {

	if u_in == nil {
		return nil
	}

	u, ok := u_in.(txPeerUpdate)
	if !ok {
		panic("Type error, not txPeerUpdate")
	}

	g, ok := g_in.(*txPoolGlobal)
	if !ok {
		panic("Type error, not txPoolGlobal")
	}

	//checkout global status
	peerStatus := GetNetworkStatus().queryPeer(p.peerId)
	if peerStatus == nil {
		//boom ..., but it may be caused by an stale peer-removing so not
		//consider as error
		logger.Warningf("Unknown status in global for peer %s", p.peerId)
		return nil
	}

	//purge on each update first
	//our data is outdate, all cache is clear .... ...
	if peerStatus.beginTxSeries > p.lastSeries() {
		logger.Warningf("Tx chain in peer %s is outdate (%x@[%v]), reset it",
			p.peerId, p.last.digest, p.last.digestSeries)
		p.reset(peerStatus.createPeerTxItem())
	} else {
		p.purge(peerStatus.beginTxSeries, g)
	}

	inTxs, err := u.toTxs(g.ledger, p.lastSeries())

	if err != nil {
		return err
	} else if inTxs == nil {
		return nil
	}

	oldlast := p.last
	err = p.concat(inTxs)
	if err != nil {
		//there is many possible for a far-end provided nonsense update for you
		//(caused by itself or the original peer) so we just simply omit it
		logger.Errorf("We may obtain branched data from peer %s: %s", p.peerId, err)
		return nil
	}

	var mergeCnt int
	//we need to construct the index ...
	for beg := oldlast.next; beg != nil; beg = beg.next {
		g.index(beg)
		if beg.digestSeries == (beg.digestSeries/jumplistInterval)*jumplistInterval {
			p.jlindex[beg.digestSeries/jumplistInterval] = beg
		}
		mergeCnt++

		//also add transaction into ledger
		g.ledger.PoolTransaction(beg.tx)
	}

	// var mergeW uint
	// if len(u.Transactions) < int(cat_hottx_one_merge_weight) {
	// 	mergeW = uint(len(u.Transactions))
	// } else {
	// 	mergeW = cat_hottx_one_merge_weight
	// }
	// //scoring the peer (how many new txs have the update provided )
	// g.currentCpo.ScoringPeer(mergeCnt*100/len(u.Transactions), mergeW)

	return nil
}

type hotTxCat struct {
	policy gossip.CatalogPolicies
}

func init() {
	gossip.RegisterCat = append(gossip.RegisterCat, initHotTx)
}

func initHotTx(stub *gossip.GossipStub) {

	l, err := ledger.GetLedger()
	if err != nil {
		logger.Errorf("Get error fail, could not create hotx tx catalogy")
		return
	}

	txglobal := new(txPoolGlobal)
	txglobal.ind = make(map[string]*txMemPoolItem)
	txglobal.ledger = l

	hotTx := new(hotTxCat)
	hotTx.policy = gossip.NewCatalogPolicyDefault()

	m := model.NewGossipModel(model.NewScuttlebuttStatus(txglobal))

	stub.AddCatalogHandler(hotTxCatName,
		gossip.NewCatalogHandlerImpl(stub.GetSStub(),
			stub.GetStubContext(), hotTx, m))
	registerEvictFunc(hotTxCatName, m)
}

const (
	hotTxCatName = "openedTx"
)

//Implement for CatalogHelper
func (c *hotTxCat) Name() string                        { return hotTxCatName }
func (c *hotTxCat) GetPolicies() gossip.CatalogPolicies { return c.policy }

func (c *hotTxCat) TransDigestToPb(d_in model.Digest) *pb.Gossip_Digest {

	d, ok := d_in.(model.ScuttlebuttDigest)
	if !ok {
		panic("Type error, not ScuttlebuttDigest")
	}

	msg := new(pb.Gossip_Digest)

	gd, ok := d.GlobalDigest().(txPoolGlobalDigest)
	if ok {
		msg.Epoch = gd.epoch
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

func (c *hotTxCat) EncodeUpdate(cpo gossip.CatalogPeerPolicies, u_in model.Update, msg_in proto.Message) proto.Message {

	u, ok := u_in.(model.ScuttlebuttUpdate)

	if !ok {
		panic("Type error, not ScuttlebuttUpdate")
	}

	//global part is not need to be encoded ...
	//gu, ok := u.GlobalUpdate().(txPoolGlobalUpdateOut)

	// if !ok {
	// 	panic("Type error, not txPoolGlobalUpdateOut")
	// }

	msg, ok := msg_in.(*pb.Gossip_Tx)
	if !ok {
		panic("Type error, not Gossip_Tx")
	}

	msg.Txs = make(map[string]*pb.HotTransactionBlock)

	//encode txs
	for id, pu_in := range u.PeerUpdate() {
		pu, ok := pu_in.(txPeerUpdate)
		if !ok {
			panic("Type error, not peerTxs")
		}

		msg.Txs[id] = pu.HotTransactionBlock
	}

	return msg
}

func (c *hotTxCat) DecodeUpdate(cpo gossip.CatalogPeerPolicies, msg_in proto.Message) (model.Update, error) {

	msg, ok := msg_in.(*pb.Gossip_Tx)
	if !ok {
		panic("Type error, not Gossip_Tx")
	}

	u := model.NewscuttlebuttUpdate(txPoolGlobalUpdate{cpo})

	for id, txs := range msg.Txs {
		u.UpdatePeer(id, txPeerUpdate{txs})
	}

	return u, nil
}
