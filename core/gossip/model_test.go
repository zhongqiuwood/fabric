package gossip

import (
	"testing"

	pb "github.com/abchain/fabric/protos"
)

func TestModel(t *testing.T) {
	m := &Model{}
	m.init()

	message := &pb.Gossip{}
	m.applyUpdate(message)
}
