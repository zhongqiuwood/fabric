package txnetwork

import (
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
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
