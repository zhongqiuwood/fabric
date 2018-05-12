package gossip

import (
	"testing"

	pb "github.com/abchain/fabric/protos"
)

func TestModel(t *testing.T) {
	m := &Model{}
	m.init("peerid", []byte("1"), 1)

	message := &pb.Gossip{}
	referer := &pb.PeerID{}
	m.applyUpdate(referer, message)
}
