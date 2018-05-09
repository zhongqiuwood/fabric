package gossip

import (
	"testing"

	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
)

func TestPeer(t *testing.T) {
	p, err := peer.NewPeerWithEngine(nil, nil)
	if err != nil {
		return
	}
	NewGossip(p)

	gs := GetGossip()

	txs := []*pb.Transaction{}
	txs = append(txs, &pb.Transaction{})
	gs.BroadcastTx(txs)
}
