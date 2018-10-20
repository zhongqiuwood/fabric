package txnetwork

import (
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"testing"
)

func createState(digest []byte, num uint64) *pb.PeerTxState {
	ret := &pb.PeerTxState{Digest: digest, Num: num}

	//todo: make signature
	ret.Endorsement = []byte{1, 2, 3}

	return ret
}

func TestTxGlobal(t *testing.T) {
	global := initGlobalStatus()

	peerG := new(peersGlobal)
	peerG.network = global
	peerG.txNetworkPeers = peerG.network.peers
	selfstatus := model.NewScuttlebuttStatus(peerG)
	stxs, _ := peerG.QuerySelf()
	selfstatus.SetSelfPeer(peerG.selfId, &peerStatus{stxs})
	m := model.NewGossipModel(selfstatus)

	globalcat := new(globalCat)
	globalcat.policy = gossip.NewCatalogPolicyDefault()

	const testpeername = "testpeer"
	//make test peer is known
	pbin := &pb.GossipMsg_Digest{
		Data: map[string]*pb.GossipMsg_Digest_PeerState{testpeername: &pb.GossipMsg_Digest_PeerState{}},
	}

	m.RecvPullDigest(globalcat.TransPbToDigest(pbin))

	if _, ok := peerG.lruIndex[testpeername]; !ok {
		t.Fatalf("%s is not known", testpeername)
	}

	if s := peerG.QueryPeer(testpeername); s != nil {
		t.Fatalf("query get uninited %s", testpeername)
	}

	//update test peer
	uin1 := &pb.Gossip_TxState{
		Txs: map[string]*pb.PeerTxState{testpeername: createState([]byte{1}, 3)},
	}
	uin1.Txs[testpeername].Endorsement = nil

	u1, err := globalcat.DecodeUpdate(nil, uin1)
	if err != nil {
		t.Fatal("decode update 1 fail", err)
	}

	err = m.RecvUpdate(u1)
	if err == nil {
		t.Fatal("do not found no endorsement error")
	}

	if s := peerG.QueryPeer(testpeername); s != nil {
		t.Fatalf("query get uninited %s", testpeername)
	}

	uin2 := &pb.Gossip_TxState{
		Txs: map[string]*pb.PeerTxState{testpeername: createState([]byte{1}, 3)},
	}

	u2, err := globalcat.DecodeUpdate(nil, uin2)
	if err != nil {
		t.Fatal("decode update 1 fail", err)
	}

	err = m.RecvUpdate(u2)
	if err != nil {
		t.Fatal("recv update fail", err)
	}

	if s := peerG.QueryPeer(testpeername); s == nil {
		t.Fatalf("could not get peer %s", testpeername)
	}
	//pick 1

	uout1 := m.RecvPullDigest(globalcat.TransPbToDigest(pbin))

	msgout1 := globalcat.EncodeUpdate(nil, uout1, globalcat.UpdateMessage()).(*pb.Gossip_TxState)

	if s, ok := msgout1.Txs[testpeername]; ok {

		if s.Num != 3 {
			t.Fatal("wrong testpeer state", msgout1)
		}

		if len(s.Endorsement) == 0 {
			t.Fatal("do not include endorsement", msgout1)
		}

	} else {
		t.Fatal("could not get update of testpeer", msgout1)
	}

	//update local
	uself := model.NewscuttlebuttUpdate(nil)
	ustate := createState([]byte{2}, 4)
	ustate.Endorsement = nil
	//update local enable a vanished endorsement (use previous one instead)
	uself.UpdateLocal(peerStatus{ustate})

	err = m.RecvUpdate(uself)
	if err != nil {
		t.Fatal("recv update fail 2", err)
	}

	self, _ := peerG.QuerySelf()

	if self.Num != 4 {
		t.Fatal("self update fail", self)
	}

	if len(self.Endorsement) == 0 {
		t.Fatal("self miss endorsement after updating")
	}

	//pick 2
	pbin.Data[testpeername].Num = 2
	pbin.Data[peerG.selfId] = &pb.GossipMsg_Digest_PeerState{}

	uout2 := m.RecvPullDigest(globalcat.TransPbToDigest(pbin))

	msgout2 := globalcat.EncodeUpdate(nil, uout2, globalcat.UpdateMessage()).(*pb.Gossip_TxState)

	if s, ok := msgout2.Txs[testpeername]; ok {

		if s.Num != 3 {
			t.Fatal("wrong testpeer state", msgout2)
		}

	} else {
		t.Fatal("could not get update of testpeer", msgout2)
	}

	if s, ok := msgout2.Txs[peerG.selfId]; ok {

		if s.Num != 4 {
			t.Fatal("wrong selfpeer state", msgout2)
		}

		if len(s.Endorsement) == 0 {
			t.Fatal("do not include endorsement", msgout2)
		}

	} else {
		t.Fatal("could not get update of selfpeere", msgout2)
	}

	//remove testpeer
	urem := model.NewscuttlebuttUpdate(nil)
	urem.RemovePeers([]string{testpeername})

	err = m.RecvUpdate(urem)
	if err != nil {
		t.Fatal("recv update fail 3", err)
	}

	if _, ok := peerG.lruIndex[testpeername]; ok {
		t.Fatal("testpeer do not removed")
	}

	if peerG.lruQueue.Len() > 0 {
		t.Fatal("wrong lru queue")
	}

	uout3 := m.RecvPullDigest(globalcat.TransPbToDigest(pbin))

	msgout3 := globalcat.EncodeUpdate(nil, uout3, globalcat.UpdateMessage()).(*pb.Gossip_TxState)

	if _, ok := msgout3.Txs[testpeername]; ok {
		t.Fatal("pick ghost test peer")
	}

	if s, ok := msgout3.Txs[peerG.selfId]; ok {

		if s.Num != 4 {
			t.Fatal("wrong selfpeer state", msgout3)
		}

		if len(s.Endorsement) == 0 {
			t.Fatal("do not include endorsement", msgout3)
		}

	} else {
		t.Fatal("could not get update of selfpeere", msgout3)
	}
}

func TestTxGlobalTruncate(t *testing.T) {

}

func TestAsAWhole(t *testing.T) {

	l := initTestLedgerWrapper(t)
	defer func(bits uint) {
		SetPeerTxQueueLen(bits)
	}(peerTxQueueLenBit)

	SetPeerTxQueueLen(3)

	stub := gossip.NewGossipWithPeer(peer.NewPeer(&pb.PeerEndpoint{ID: &pb.PeerID{Name: "testpeer"}}))

	globalM := stub.GetCatalogHandler(globalCatName).Model()
	globalS := model.DumpScuttlebutt(globalM)

	txM := stub.GetCatalogHandler(hotTxCatName).Model()
	txS := model.DumpScuttlebutt(txM)
	txG, ok := txS.ScuttlebuttStatus.(*txPoolGlobal)
	if !ok {
		panic("wrong code, not txPoolGlobal")
	}

	t.Log("obtain statuses:", globalS, txS)
	//now we control the models directly
	if globalS.SelfID != txS.SelfID {
		t.Fatal("Wrong selfid")
	}

	if !globalS.Extended || txS.Extended {
		t.Fatal("Wrong configuration")
	}

	//directly send update and insert new peer
	newpeer := "abc"
	peerDigest := make([]byte, TxDigestVerifyLen)
	peerDigest[0] = 2
	ud := model.NewscuttlebuttUpdate(nil)
	ud.UpdatePeer(newpeer, peerStatus{createState(peerDigest, 6)})

	err := globalM.RecvUpdate(ud)
	if err != nil {
		t.Fatal("recv update fail", err)
	}

	if _, ok := globalS.Peers[newpeer]; !ok {
		t.Fatal("update fail", globalS.Peers)
	}

	if _, ok := txS.Peers[newpeer]; !ok {
		t.Fatal("txs is not covariated", txS.Peers)
	}

	tx1, _ := buildTestTx(t)
	tx2, _ := buildTestTx(t)
	tx1 = buildPrecededTx(peerDigest, tx1)
	tx2 = buildPrecededTx(getTxDigest(tx1), tx2)

	udt := txPeerUpdate{new(pb.HotTransactionBlock)}
	udt.BeginSeries = 7 //peer begin from 7, so the cache just cross the border of dqueue
	udt.Transactions = []*pb.Transaction{tx1, tx2}
	ud = model.NewscuttlebuttUpdate(nil)
	ud.UpdatePeer(newpeer, udt)

	err = txM.RecvUpdate(ud)
	if err != nil {
		t.Fatal("recv tx update fail", err)
	}

	if _, ok := txS.Peers[newpeer]; !ok {
		t.Fatal("update tx fail", txS.Peers)
	}

	udv := udt.To()
	prv := txS.Peers[newpeer].To()
	//clock must equal
	if prv.Less(udv) || udv.Less(prv) {
		t.Fatal("unmatch clock", udv, prv)
	}

	if tx := l.GetPooledTransaction(tx1.GetTxid()); tx == nil {
		t.Fatal("tx1 is not pooled")
	}

	if tx := l.GetPooledTransaction(tx2.GetTxid()); tx == nil {
		t.Fatal("tx2 is not pooled")
	}

	ccache := txG.AcquireCaches(newpeer).(*txCache).commitData
	//cache for tx with series 7 and 8 just use two cache-row (0, 1)
	if ccache[0] == nil || ccache[1] == nil {
		t.Fatal("wrong cache position", ccache)
	}

	ud = model.NewscuttlebuttUpdate(nil)
	ud.UpdatePeer(newpeer, peerStatus{createState(getTxDigest(tx2), 8)})
	err = globalM.RecvUpdate(ud)
	if err != nil {
		t.Fatal("recv rm-update fail", err)
	}

	if ccache[0] != nil {
		t.Fatal("cache is not prune", ccache)
	}
	if ccache[1] == nil {
		t.Fatal("cache is wrong", ccache)
	}

	ud = model.NewscuttlebuttUpdate(nil)
	ud.RemovePeers([]string{newpeer})

	err = globalM.RecvUpdate(ud)
	if err != nil {
		t.Fatal("recv rm-update fail", err)
	}

	if _, ok := globalS.Peers[newpeer]; ok {
		t.Fatal("peer not remove", globalS.Peers)
	}

	if _, ok := txS.Peers[newpeer]; ok {
		t.Fatal("peer not remove", globalS.Peers)
	}

	if len(txG.AcquireCaches(newpeer).(*txCache).commitData[0]) > 0 {
		t.Fatal("removed peer still have ghost cache")
	}

}

func TestSelfUpdateAsAWhole(t *testing.T) {

	initTestLedgerWrapper(t)
	defer func(bits uint) {
		SetPeerTxQueueLen(bits)
	}(peerTxQueueLenBit)

	SetPeerTxQueueLen(3)

	stub := gossip.NewGossipWithPeer(peer.NewPeer(&pb.PeerEndpoint{ID: &pb.PeerID{Name: "testpeer"}}))

	globalM := stub.GetCatalogHandler(globalCatName).Model()
	globalS := model.DumpScuttlebutt(globalM)

	txM := stub.GetCatalogHandler(hotTxCatName).Model()
	txS := model.DumpScuttlebutt(txM)
	// txG, ok := txS.ScuttlebuttStatus.(*txPoolGlobal)
	// if !ok {
	// 	panic("wrong code, not txPoolGlobal")
	// }

	txSelf := txS.Peers[""].(*peerTxMemPool)

	chain := prolongItemChain(t, txSelf.head.clone(), 20)
	var udt = txPeerUpdate{new(pb.HotTransactionBlock)}
	udt.fromTxs(chain.fetch(1, nil))

	ud := model.NewscuttlebuttUpdate(nil)
	ud.UpdateLocal(udt)

	if err := txM.RecvUpdate(ud); err != nil {
		t.Fatalf("update local peer fail: %s", err)
	}

	if txSelf.lastSeries() != 20 {
		t.Fatalf("update local peer fail: unexpected end %d", txSelf.lastSeries())
	}

	jTo := txSelf.jlindex[1]
	ud = model.NewscuttlebuttUpdate(nil)
	ud.UpdateLocal(peerStatus{createState(jTo.digest, jTo.digestSeries)})
	if err := globalM.RecvUpdate(ud); err != nil {
		t.Fatal("recv self update global fail", err)
	}

	peerSelf := globalS.Peers[""].(*peerStatus)
	if peerSelf.Num != jTo.digestSeries {
		t.Fatal("unexpected updated peer status", peerSelf)
	}

	if txSelf.head.digestSeries != jTo.digestSeries {
		t.Fatal("unexpected updated tx status", txSelf.head)
	}
}
