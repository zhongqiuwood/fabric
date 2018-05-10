package gossip

import (
	"testing"

	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
)

func TestModel(t *testing.T) {
	// init ledger
	_, err := ledger.GetNewLedger()
	if err != nil {
		return
	}
	m := &Model{}
	m.init()

	message := &pb.Gossip{}
	m.applyUpdate(message)
}
