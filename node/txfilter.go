package node

import (
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/events/litekfk"
	pb "github.com/abchain/fabric/protos"
)

type txFilter struct {
	txTopic map[string]litekfk.Topic
}

func (f *txFilter) ValidatePeerStatus(id string, status *pb.PeerTxState) error {
	return nil
}

func (f *txFilter) TransactionPreValidation(*pb.Transaction) (*pb.Transaction, error) {
	return nil, nil
}

func (f *txFilter) GetPreHandler(id string) (cred.TxPreHandler, error) { return f, nil }
func (f *txFilter) RemovePreHandler(id string)                         {}
func (f *txFilter) Release()                                           {}
