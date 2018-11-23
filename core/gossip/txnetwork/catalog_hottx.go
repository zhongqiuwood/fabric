package txnetwork

import (
	"bytes"
	"fmt"

	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/gossip/stub"
	pb "github.com/abchain/fabric/protos"
	proto "github.com/golang/protobuf/proto"
)

type txMemPoolItem struct {
	digest       []byte
	digestSeries uint64
	tx           *pb.Transaction //cache of the tx

	//so we have a simple list structure
	next *txMemPoolItem
}

func createPeerTxItem(s *pb.PeerTxState) *txMemPoolItem {
	return &txMemPoolItem{
		digest:       s.GetDigest(),
		digestSeries: s.GetNum(),
	}
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

const (
	TxDigestVerifyLen = 16
	failDig           = "__FAILTXDIGEST__"
)

func getTxDigest(tx *pb.Transaction) []byte {
	dig, err := tx.Digest()
	if err != nil {
		dig = []byte(failDig)
	}

	if len(dig) < TxDigestVerifyLen {
		panic("Wrong code generate digest less than 16 bytes [128bit]")
	}

	return dig[:TxDigestVerifyLen]
}

func GetTxDigest(tx *pb.Transaction) []byte {
	return getTxDigest(tx)
}

func GetPrecededTx(digest []byte, tx *pb.Transaction) *pb.Transaction {
	return buildPrecededTx(digest, tx)
}

//return whether tx is match to the digest
func txIsMatch(digest []byte, tx *pb.Transaction) bool {

	return bytes.Compare(digest, getTxDigest(tx)) == 0
}

func buildPrecededTx(digest []byte, tx *pb.Transaction) *pb.Transaction {

	if len(digest) < TxDigestVerifyLen {
		digest = append(digest, failDig[len(digest):]...)
	}

	tx.Nonce = digest
	return tx
}

//return whether tx2 is precede of the digest
func txIsPrecede(digest []byte, tx2 *pb.Transaction) bool {
	n := tx2.GetNonce()
	if len(n) < TxDigestVerifyLen {
		return false
	} else if len(digest) < TxDigestVerifyLen {
		digest = append(digest, failDig[len(digest):]...)
	}
	return bytes.Compare(digest, n[:TxDigestVerifyLen]) == 0
}

func (p *peerTxs) firstSeries() uint64 {
	if p == nil || p.head == nil {
		return 0
	} else {
		return p.head.digestSeries
	}
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
		return fmt.Errorf("Wrong next series: %d (expected %d)", s.head.digestSeries, p.lastSeries()+1)
	}

	p.last.next = s.head
	p.last = s.last
	return nil
}

//have extra cost and we should avoid using it
func (p *peerTxs) merge(s *peerTxs) (*txMemPoolItem, error) {

	if p.head == nil {
		return s.head, p.concat(s)
	}

	if !s.inRange(p.last.digestSeries + 1) {
		//nothing to merge, give up
		return nil, nil
	}

	//scan txs ...
	for beg := s.head; beg != nil; beg = beg.next {
		if beg.digestSeries > p.last.digestSeries {
			return beg, p.concat(&peerTxs{beg, s.last})
		}
	}

	return nil, nil
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

type txPoolGlobalUpdateOut struct {
	*txPoolGlobal
	epoch uint64
}

func (txPoolGlobalUpdateOut) Gossip_IsUpdateIn() bool { return false }

type txPoolGlobalUpdate struct {
	gossip.CatalogPeerPolicies
}

func (txPoolGlobalUpdate) Gossip_IsUpdateIn() bool { return true }

type txPoolGlobal struct {
	*transactionPool
	//	ind        map[string]*txMemPoolItem
	network *txNetworkGlobal
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

	if d_in == nil {
		return txPoolGlobalUpdateOut{g, 0}
	}

	d, ok := d_in.(*pb.GossipMsg_Digest)

	if !ok {
		panic("Type error, not Gossip_Digest")
	}

	epochH, err := g.ledger.GetBlockNumberByState(d.GetEpoch())
	if err != nil {
		logger.Warning("Get epoch height fail, encode all transactions:", err)
		epochH = 0
	}

	return txPoolGlobalUpdateOut{g, epochH}
}

func (g *txPoolGlobal) Update(_ model.Update) error { return nil }

func (g *txPoolGlobal) NewPeer(id string) model.ScuttlebuttPeerStatus {

	//check if we have known this peer
	peerStatus := g.network.peers.QueryPeer(id)
	if peerStatus == nil {
		logger.Infof("We hear of peer %s before it is known by us", id)
		return nil
	}

	logger.Debugf("txpool now know new peer [%s]", id)
	ret := new(peerTxMemPool)
	ret.reset(createPeerTxItem(peerStatus))
	return ret
}

func (g *txPoolGlobal) MissedUpdate(string, model.ScuttlebuttPeerUpdate) error {
	return nil
}

func (g *txPoolGlobal) RemovePeer(string, model.ScuttlebuttPeerStatus) {}

func txToDigestState(tx *pb.Transaction) []byte {
	return tx.GetNonce()
}

//update structure used for recving
type txPeerUpdate struct {
	*pb.HotTransactionBlock
}

func (u txPeerUpdate) To() model.VClock {
	if u.HotTransactionBlock == nil {
		return model.BottomClock
	}

	return standardVClock(u.BeginSeries + uint64(len(u.Transactions)) - 1)
}

func getLiteTx(tx *pb.Transaction) *pb.Transaction {
	return &pb.Transaction{Txid: tx.GetTxid()}
}

func isLiteTx(tx *pb.Transaction) bool {
	return tx.GetNonce() == nil
}

func (u txPeerUpdate) fromTxs(s *peerTxs) {

	if s == nil {
		return
	}

	u.BeginSeries = s.head.digestSeries

	for beg := s.head; beg != nil; beg = beg.next {
		u.Transactions = append(u.Transactions, beg.tx)
	}
}

func (u txPeerUpdate) getRef(refSeries uint64) txPeerUpdate {

	if u.BeginSeries < refSeries {
		//possible panic if headpos exceed len of u.Transactions ?
		//scuttlebutt scheme will check To() first and avoid this case
		return txPeerUpdate{&pb.HotTransactionBlock{u.Transactions[int(refSeries-u.BeginSeries):], refSeries}}
	} else {
		//start from head
		return u
	}
}

func (u txPeerUpdate) pruneTxs(epochH uint64, ccache TxCache) txPeerUpdate {
	for i, tx := range u.Transactions {
		commitedH := ccache.GetCommit(u.BeginSeries+uint64(i), tx)

		if commitedH != 0 && commitedH <= epochH {
			u.Transactions[i] = getLiteTx(tx)
		}
	}
	return u
}

func (u txPeerUpdate) toTxs(reflastDigest []byte) (*peerTxs, error) {

	if len(u.Transactions) == 0 {
		return nil, nil
	}

	tx := u.Transactions[0]
	if reflastDigest != nil && !txIsPrecede(reflastDigest, tx) {
		return nil, fmt.Errorf("update have invalid transactions chain for tx at %d", u.BeginSeries)
	}

	head := &txMemPoolItem{
		tx:           tx,
		digest:       getTxDigest(tx),
		digestSeries: u.BeginSeries,
	}

	current := head

	//construct a peerTxMemPool from txs, and do verification
	for _, tx := range u.Transactions[1:] {

		if !txIsPrecede(current.digest, tx) {
			return nil, fmt.Errorf("update have invalid transactions chain for tx at %d", current.digestSeries+1)
		}

		current.next = &txMemPoolItem{
			tx:           tx,
			digest:       getTxDigest(tx),
			digestSeries: current.digestSeries + 1,
		}
		current = current.next
	}

	return &peerTxs{head, current}, nil
}

//so we make two indexes for pooling txs: the global one (by txid) and a per-peer
//jumping list (by seqnum) with [1/jumplistInterval] size of the global indexs,
//and finally we have a 100/jumplistInterval % overhead
const (
	jumplistInterval = uint64(8)
)

type peerTxMemPool struct {
	*peerTxs
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

	if p.lastSeries() == 0 {
		return model.BottomClock
	}

	return standardVClock(p.lastSeries())
}

func (p *peerTxMemPool) PickFrom(id string, d_in model.VClock, gu_in model.Update) (model.ScuttlebuttPeerUpdate, model.Update) {

	d := toStandardVClock(d_in)
	expectedH := uint64(d) + 1
	ret := txPeerUpdate{new(pb.HotTransactionBlock)}

	if !p.inRange(expectedH) {
		//we can not provide required data for this request, just give up
		return nil, gu_in
	} else {
		ret.fromTxs(p.fetch(expectedH, p.jlindex[uint64(d)/jumplistInterval]))
	}

	return ret, gu_in
}

func purgePool(p *peerTxMemPool, purgeto uint64) {
	//purge the outdate part
	//for zero, we just get UINT64_MAX and no index
	preserved := p.fetch(purgeto, p.jlindex[purgeto/jumplistInterval-1])
	if preserved == nil {
		logger.Warning("got empty preserved, wrong purge request:", purgeto)
		return
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

func (p *peerTxMemPool) purge(id string, purgeto uint64, g *txPoolGlobal) {

	//never purge at begin
	if p.head == nil || purgeto <= p.head.digestSeries {
		return
	}

	logger.Debugf("peer [%s] try to prune cache from %d to %d", id, p.firstSeries(), purgeto)
	g.AcquireCaches(id).Pruning(p.firstSeries(), purgeto)

	purgePool(p, purgeto)

}

func (p *peerTxMemPool) handlePeerCoVar(id string, peerStatus *pb.PeerTxState, g *txPoolGlobal) error {

	to := peerStatus.GetNum()
	if to <= p.firstSeries() {

		return fmt.Errorf("covar occurs on %s with a peer status %d earlier than current (%d)",
			id, to, p.firstSeries())
	}

	p.purge(id, to, g)
	return nil
}

func (p *peerTxMemPool) handlePeerUpdate(u txPeerUpdate, id string, g *txPoolGlobal) error {
	logger.Debugf("peer [%s] try to updated %d incoming txs from series %d", id, len(u.Transactions), u.BeginSeries)

	if u.BeginSeries > p.lastSeries()+1 {
		return fmt.Errorf("Get gapped update for [%s] start from %d, current %d", id, u.BeginSeries, p.lastSeries())
	}

	u = u.getRef(p.lastSeries() + 1)
	err := g.AcquireCaches(id).AddTxs(u.BeginSeries, u.Transactions, id == "")
	if err != nil {
		return err
	}

	inTxs, err := u.toTxs(p.last.digest)
	if err != nil {
		//NEVER report this as the fraud of far-end because the original peer can build
		//branch results (by update peer status or sending branched tx chains) deliberately
		//we just skip the update data
		logger.Warningf("***** PROBLEM ****** peer %s update found branched incoming", id)
		return nil
	} else if inTxs == nil {
		logger.Infof("peer %s update nothing in %d txs and quit", id, len(u.Transactions))
		return nil
	}

	//sanity check
	err = p.concat(inTxs)
	if err != nil {
		panic(fmt.Errorf("toTxs method should has verified everything, wrong code:", err))
	}

	var mergeCnt int
	//we need to construct the index ...
	for beg := inTxs.head; beg != nil; beg = beg.next {

		if beg.digestSeries == (beg.digestSeries/jumplistInterval)*jumplistInterval {
			p.jlindex[beg.digestSeries/jumplistInterval] = beg
		}
		mergeCnt++

	}

	logger.Debugf("peer %s have updated %d txs", id, mergeCnt)

	//finally we handle the case if pool's cache is overflowed
	if int(p.lastSeries()-p.firstSeries())+1 > PeerTxQueueLimit() {
		to := p.firstSeries() + uint64(PeerTxQueueLen())
		logger.Warningf("peer %s's cache has reach limit and we have to prune it to %d", id, to)
		//notice, the cache has been full so we do not need to prune it
		//(the older part has been overwritten)
		purgePool(p, to)
	}

	return nil
}

// there are too many ways that a far-end can waste our bandwidth (and
// limited computational cost) without provding useful information,
// for example, fill some update under un-existed peer name
// so we given up scoring the peer until we have more solid process
// to verify the correctness of data from far-end
func (p *peerTxMemPool) Update(id string, u_in model.ScuttlebuttPeerUpdate, g_in model.ScuttlebuttStatus) error {

	if u_in == nil {
		return nil
	}

	g, ok := g_in.(*txPoolGlobal)
	if !ok {
		panic("Type error, not txPoolGlobal")
	}

	//checkout global status
	var peerStatus *pb.PeerTxState
	if id == "" {
		peerStatus, _ = g.network.peers.QuerySelf()
	} else {
		peerStatus = g.network.peers.QueryPeer(id)
	}
	if peerStatus == nil {
		//boom ..., but it may be caused by an stale peer-removing so not
		//consider as error
		logger.Warningf("Unknown status in global for peer [%s]", id)
		return nil
	}

	switch u := u_in.(type) {
	case txPeerUpdate:
		return p.handlePeerUpdate(u, id, g)
	case coVarUpdate:
		return p.handlePeerCoVar(id, peerStatus, g)
	default:
		return fmt.Errorf("Type error, not expected update [%v]", u_in)
	}

}

type hotTxCat struct {
	policy gossip.CatalogPolicies
}

func init() {
	stub.RegisterCat = append(stub.RegisterCat, initHotTx)
}

func initHotTx(stub *gossip.GossipStub) {

	txglobal := new(txPoolGlobal)
	//	txglobal.ind = make(map[string]*txMemPoolItem)
	txglobal.network = getTxNetwork(stub)
	txglobal.transactionPool = txglobal.network.txPool

	hotTx := new(hotTxCat)
	hotTx.policy = gossip.NewCatalogPolicyDefault()

	selfStatus := model.NewScuttlebuttStatus(txglobal)

	peers := txglobal.network.peers
	if selfs, _ := peers.QuerySelf(); selfs != nil {
		self := &peerTxMemPool{}
		self.reset(createPeerTxItem(selfs))

		selfStatus.SetSelfPeer(peers.selfId, self)
	}

	m := model.NewGossipModel(selfStatus)

	ch := gossip.NewCatalogHandlerImpl(stub.GetSStub(),
		stub.GetStubContext(), hotTx, m)
	stub.AddCatalogHandler(ch)

	txglobal.network.RegUpdateNotify(standardUpdateFunc(ch))
	txglobal.network.RegEvictNotify(standardEvictFunc(ch))
	txglobal.network.RegSetSelfPeer(func(newID string, state *pb.PeerTxState) {
		m.Lock()
		defer m.Unlock()
		self := &peerTxMemPool{}
		self.reset(createPeerTxItem(state))
		selfStatus.SetSelfPeer(newID, self)
		logger.Infof("Hottx cat reset self peer to %s", newID)
	})

}

const (
	hotTxCatName = "openedTx"
)

//Implement for CatalogHelper
func (c *hotTxCat) Name() string                        { return hotTxCatName }
func (c *hotTxCat) GetPolicies() gossip.CatalogPolicies { return c.policy }

func (c *hotTxCat) TransDigestToPb(d_in model.Digest) *pb.GossipMsg_Digest {

	d, ok := d_in.(model.ScuttlebuttDigest)
	if !ok {
		panic("Type error, not ScuttlebuttDigest")
	}

	gd := d.GlobalDigest().(txPoolGlobalDigest)
	return toPbDigestStd(d, gd.epoch)

}

func (c *hotTxCat) TransPbToDigest(msg *pb.GossipMsg_Digest) model.Digest {

	return parsePbDigestStd(msg, msg)

}

func (c *hotTxCat) UpdateMessage() proto.Message { return new(pb.Gossip_Tx) }

func (c *hotTxCat) EncodeUpdate(cpo gossip.CatalogPeerPolicies, u_in model.Update, msg_in proto.Message) proto.Message {

	u, ok := u_in.(model.ScuttlebuttUpdate)

	if !ok {
		panic("Type error, not ScuttlebuttUpdate")
	}

	//global part is not need to be encoded ...
	gu, ok := u.GlobalUpdate().(txPoolGlobalUpdateOut)

	if !ok {
		panic("Type error, not txPoolGlobalUpdateOut")
	}

	msg, ok := msg_in.(*pb.Gossip_Tx)
	if !ok {
		panic("Type error, not Gossip_Tx")
	}

	msg.Txs = make(map[string]*pb.HotTransactionBlock)

	//encode txs
	//TODO: if cpo is availiable and quota is limited, cut some data
	for id, pu_in := range u.PeerUpdate() {
		pu, ok := pu_in.(txPeerUpdate)
		if !ok {
			panic("Type error, not peerTxs")
		}

		msg.Txs[id] = pu.pruneTxs(gu.epoch, gu.AcquireCaches(id)).HotTransactionBlock
	}

	return msg
}

func (c *hotTxCat) DecodeUpdate(cpo gossip.CatalogPeerPolicies, msg_in proto.Message) (model.Update, error) {

	msg, ok := msg_in.(*pb.Gossip_Tx)
	if !ok {
		panic("Type error, not Gossip_Tx")
	}

	//can return null data
	if len(msg.Txs) == 0 {
		return nil, nil
	}

	//detected a malicious behavior
	if _, ok := msg.Txs[""]; ok {
		return nil, fmt.Errorf("Peer try to update a invalid id (self)")
	}

	u := model.NewscuttlebuttUpdate(txPoolGlobalUpdate{cpo})

	for id, txs := range msg.Txs {
		u.UpdatePeer(id, txPeerUpdate{txs})
	}

	return u, nil
}

func UpdateLocalHotTx(stub *gossip.GossipStub, txs *pb.HotTransactionBlock) error {
	hotcat := stub.GetCatalogHandler(hotTxCatName)
	if hotcat == nil {
		return fmt.Errorf("Can't not found corresponding hottx catalogHandler")
	}

	selfUpdate := model.NewscuttlebuttUpdate(nil)
	selfUpdate.UpdateLocal(txPeerUpdate{txs})

	if err := hotcat.Model().RecvUpdate(selfUpdate); err != nil {
		return err
	} else {
		//notify our peer is updated
		hotcat.SelfUpdate()
		return nil
	}
}
