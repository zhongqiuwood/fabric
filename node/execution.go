package node

import (
	"fmt"
	"github.com/abchain/fabric/core/chaincode"
	cred "github.com/abchain/fabric/core/cred"
	_ "github.com/abchain/fabric/core/gossip/txnetwork"
	"github.com/abchain/fabric/core/ledger"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
)

func (ne *NodeEngine) QueryTransaction(ctx context.Context,
	tx *pb.Transaction, endorser cred.TxEndorser, l *ledger.Ledger, remote ...*PeerEngine) *pb.Response {

	if l == nil {
		l = ne.DefaultLedger()
	}

	if endorser == nil {
		endorser = pe.queryEndorser
	}

	var err error

	if endorser != nil {
		tx, err = endorser.EndorseTransaction(tx)
	}

}
