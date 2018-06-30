package gossip_test

import (
	"github.com/abchain/fabric/core/gossip"
	"github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"testing"
)

func TestPeer(t *testing.T) {
	p := peer.NewPeer(&pb.PeerEndpoint{ID: &pb.PeerID{Name: "test"}})

	gossip.NewGossip(p)

	wctx, endworks := context.WithCancel(context.Background())
	defer endworks()

	pAlice := pb.NewSimuPeerStub("alice", stub.GetDefaultFactory())
	pBob := pb.NewSimuPeerStub("bob", stub.GetDefaultFactory())

	if err := pAlice.ConnectTo(wctx, pBob); err != nil {
		t.Fatal(err)
	}
}
