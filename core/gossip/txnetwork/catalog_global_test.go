package txnetwork

import (
	"container/list"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	pb "github.com/abchain/fabric/protos"
	"testing"
)

func initGlobalStatus() {
	global = &txNetworkGlobal{
		maxPeers:  def_maxPeer,
		lruQueue:  list.New(),
		lruIndex:  make(map[string]*list.Element),
		selfpeers: make(map[*gossip.GossipStub]*selfPeerStatus),
	}
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
	initGlobalStatus()

	self := global.SelfPeer(nil)
	selfstatus := model.NewScuttlebuttStatus(global)
	selfstatus.SetSelfPeer(self.peerId, self)
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

	//pick 2

	//update test peer 2
}
