package statesync_test

import (
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"golang.org/x/net/context"
	"os"
	"testing"
	"time"
	"github.com/abchain/fabric/core/statesync"
)

func TestMain(m *testing.M) {
	logging.SetLevel(logging.DEBUG, "")
	os.Exit(m.Run())
}

func TestPeerSync(t *testing.T) {

	sAlice := statesync.NewStateSyncWithPeer(peer.NewPeer(&pb.PeerEndpoint{ID: &pb.PeerID{Name: "alice"}}))

	ctx, endworks := context.WithCancel(context.Background())
	defer endworks()

	sAlice.SyncToState(ctx, []byte("targetStatehash"), nil, &pb.PeerID{Name: "bob"})


	time.Sleep(time.Second)
}
