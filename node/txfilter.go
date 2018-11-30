package node

import (
	"github.com/abchain/fabric/events/litekfk"
	pb "github.com/abchain/fabric/protos"
)

type txFilter struct {
	txTopic map[string]litekfk.Topic
}

func (f *txFilter) ValidatePeerStatus(id string, status *pb.PeerTxState) error {

}

func (f *txFilter) TransactionPreValidation(*pb.Transaction) (*pb.Transaction, error) {

}

func (f *txFilter) GetPreHandler(id string) (TxPreHandler, error) { return f, nil }
func (f *txFilter) RemovePreHandler(id string)                    {}
func (f *txFilter) Release()                                      {}
