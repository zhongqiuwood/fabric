package gossip_test

import (
	"github.com/abchain/fabric/core/gossip"
	"github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"testing"
	"time"
)

func TestPeer(t *testing.T) {
	p := peer.NewPeer(&pb.PeerEndpoint{ID: &pb.PeerID{Name: "test"}})

	gossip.NewGossip(p)

	wctx, endworks := context.WithCancel(context.Background())
	defer endworks()

	pAlice := pb.NewSimuPeerStub("alice", stub.GetDefaultFactory())
	pBob := pb.NewSimuPeerStub("bob", stub.GetDefaultFactory())

	if err, trafficf := pAlice.ConnectTo(wctx, pBob); err != nil {
		t.Fatal(err)
	} else {
		go func() {
			if err := trafficf(); err != nil {
				t.Fatal("traffic fail", err)
			}
		}()
	}

	hBob := pAlice.PickHandler(&pb.PeerID{Name: "bob"})

	if hBob == nil {
		t.Fatal("no handler for Bob in Alice")
	}

	msg := &pb.Gossip{Catalog: "Notexist"}
	if err := hBob.SendMessage(msg); err != nil {
		t.Fatal("Send msg fail", err)
	}

	time.Sleep(time.Second)
}
