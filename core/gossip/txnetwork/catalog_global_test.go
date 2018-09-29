package txnetwork

import (
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"testing"
)

func initGlobalStatus() *txNetworkGlobal {

	return CreateTxNetworkGlobal()
}

func createState(digest []byte, num uint64, endorse bool) *pb.PeerTxState {
	ret := &pb.PeerTxState{Digest: digest, Num: num}

	//todo: make signature

	if endorse {
		ret.Endorsement = []byte{1, 2, 3}
	}

	return ret
}

func TestTxGlobal(t *testing.T) {
	global := initGlobalStatus()

	selfstatus := model.NewScuttlebuttStatus(global)
	selfstatus.SetSelfPeer(global.selfId, &peerStatus{global.QuerySelf()})
	m := model.NewGossipModel(selfstatus)

	globalcat := new(globalCat)
	globalcat.policy = gossip.NewCatalogPolicyDefault()

	const testpeername = "testpeer"
	//make test peer is known
	pbin := &pb.Gossip_Digest{
		Data: map[string]*pb.Gossip_Digest_PeerState{testpeername: &pb.Gossip_Digest_PeerState{}},
	}

	m.RecvPullDigest(globalcat.TransPbToDigest(pbin))

	if _, ok := global.lruIndex[testpeername]; !ok {
		t.Fatalf("%s is not known", testpeername)
	}

	if s := global.QueryPeer(testpeername); s != nil {
		t.Fatalf("query get uninited %s", testpeername)
	}

	//update test peer
	uin1 := &pb.Gossip_TxState{
		Txs: map[string]*pb.PeerTxState{testpeername: createState([]byte{1}, 3, false)},
	}

	u1, err := globalcat.DecodeUpdate(nil, uin1)
	if err != nil {
		t.Fatal("decode update 1 fail", err)
	}

	err = m.RecvUpdate(u1)
	if err == nil {
		t.Fatal("do not found no endorsement error")
	}

	if s := global.QueryPeer(testpeername); s != nil {
		t.Fatalf("query get uninited %s", testpeername)
	}

	uin2 := &pb.Gossip_TxState{
		Txs: map[string]*pb.PeerTxState{testpeername: createState([]byte{1}, 3, true)},
	}

	u2, err := globalcat.DecodeUpdate(nil, uin2)
	if err != nil {
		t.Fatal("decode update 1 fail", err)
	}

	err = m.RecvUpdate(u2)
	if err != nil {
		t.Fatal("recv update fail", err)
	}

	if s := global.QueryPeer(testpeername); s == nil {
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
	uself.UpdateLocal(peerStatus{createState([]byte{2}, 4, false)})

	err = m.RecvUpdate(uself)
	if err != nil {
		t.Fatal("recv update fail 2", err)
	}

	self := global.QuerySelf()

	if self.Num != 4 {
		t.Fatal("self update fail", self)
	}

	if len(self.Endorsement) == 0 {
		t.Fatal("self miss endorsement after updating")
	}

	//pick 2
	pbin.Data[testpeername].Num = 2
	pbin.Data[global.selfId] = &pb.Gossip_Digest_PeerState{}

	uout2 := m.RecvPullDigest(globalcat.TransPbToDigest(pbin))

	msgout2 := globalcat.EncodeUpdate(nil, uout2, globalcat.UpdateMessage()).(*pb.Gossip_TxState)

	if s, ok := msgout2.Txs[testpeername]; ok {

		if s.Num != 3 {
			t.Fatal("wrong testpeer state", msgout2)
		}

		if len(s.Endorsement) != 0 {
			t.Fatal("include unexpected endorsement", msgout2)
		}

	} else {
		t.Fatal("could not get update of testpeer", msgout2)
	}

	if s, ok := msgout2.Txs[global.selfId]; ok {

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

	if _, ok := global.lruIndex[testpeername]; ok {
		t.Fatal("testpeer do not removed")
	}

	if global.lruQueue.Len() > 0 {
		t.Fatal("wrong lru queue")
	}

	uout3 := m.RecvPullDigest(globalcat.TransPbToDigest(pbin))

	msgout3 := globalcat.EncodeUpdate(nil, uout3, globalcat.UpdateMessage()).(*pb.Gossip_TxState)

	if _, ok := msgout3.Txs[testpeername]; ok {
		t.Fatal("pick ghost test peer")
	}

	if s, ok := msgout3.Txs[global.selfId]; ok {

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
	ud.UpdatePeer(newpeer, peerStatus{createState(peerDigest, 6, true)})

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

	txcache := txG.AcquireCache(newpeer, 0, 0).peerTxCache
	//cache for tx with series 7 and 8 just use two cache-row (0, 1)
	if txcache[0] == nil || txcache[1] == nil {
		t.Fatal("wrong cache position", txcache)
	}

	ud = model.NewscuttlebuttUpdate(nil)
	ud.UpdatePeer(newpeer, peerStatus{createState(getTxDigest(tx2), 8, false)})
	err = globalM.RecvUpdate(ud)
	if err != nil {
		t.Fatal("recv rm-update fail", err)
	}

	if txcache[0] != nil {
		t.Fatal("cache is not prune", txcache)
	}
	if txcache[1] == nil {
		t.Fatal("cache is wrong", txcache)
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
}
