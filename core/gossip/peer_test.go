package gossip_test

import (
	"github.com/abchain/fabric/core/gossip"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	logging "github.com/op/go-logging"
	"golang.org/x/net/context"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	logging.SetLevel(logging.DEBUG, "")
	os.Exit(m.Run())
}

func TestPeer(t *testing.T) {

	sAlice := gossip.NewGossipWithPeer(peer.NewPeer(&pb.PeerEndpoint{ID: &pb.PeerID{Name: "alice"}}))
	sBob := gossip.NewGossipWithPeer(peer.NewPeer(&pb.PeerEndpoint{ID: &pb.PeerID{Name: "bob"}}))

	wctx, endworks := context.WithCancel(context.Background())
	defer endworks()

	pAlice := pb.NewSimuPeerStub(sAlice.GetSelf().GetName(), sAlice.GetSStub())
	pBob := pb.NewSimuPeerStub(sBob.GetSelf().GetName(), sBob.GetSStub())

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
