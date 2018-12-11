package node

import (
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/events/litekfk"
	pb "github.com/abchain/fabric/protos"
)

type CCNameTransformer func(string) string

var nullTransformer = CCNameTransformer(func(in string) string { return in })

/*
	ccSpecValidator organize additional interfaces in a hierarchical fashion
	for chaincode name specification. A interfaces specified for one or some
	chaincode will have priority to handle the corresponding txs, child node
	first and then parent.
	filter ensure each tx will be validated by EXACTLY ONE interface

	an transformer for chaincode name is also appliable
*/

type ccSpecValidator struct {
	parent    *ccSpecValidator
	handlers  map[string]cred.TxPreHandler
	nameTrans CCNameTransformer
}

//for terminal...
type nilValidator struct{}

func NewCCSpecValidator(parent *ccSpecValidator) *ccSpecValidator {
	return &ccSpecValidator{parent, make(map[string]cred.TxPreHandler), nullTransformer}
}

func (f *ccSpecValidator) SetHandler(cc string, h cred.TxPreHandler) {
	curH, ok := f.handlers[cc]
	if !ok {
		f.handlers[cc] = h
	} else {
		f.handlers[cc] = cred.MutipleTxPreHandler(curH, h)
	}
}

func (f *ccSpecValidator) SetNameTransfer(tf CCNameTransformer) (old CCNameTransformer) {
	old = f.nameTrans
	if f == nil {
		panic("Nil transformer is not allowed")
	}
	f.nameTrans = tf
	return
}

func (f *ccSpecValidator) getHandler(ccname string) cred.TxPreHandler {

	if h, ok := f.handlers[ccname]; ok {
		return h
	}

	if f.parent != nil {
		return f.parent.getHandler(ccname)
	}

	return nilValidator{}
}

func (f *ccSpecValidator) TransactionPreValidation(tx *pb.Transaction) (*pb.Transaction, error) {

	ccname := f.nameTrans(string(tx.GetChaincodeID()))

	return f.getHandler(ccname).TransactionPreValidation(tx)
}

func (f *ccSpecValidator) Release() {}

func (nilValidator) TransactionPreValidation(tx *pb.Transaction) (*pb.Transaction, error) {
	return tx, nil
}
func (nilValidator) Release() {}

/*
	Transactions received from txnetwork, and recorded in the TxTopic of NodeEngine
	by specified of chaincode name. This structure allow the executor to trace
	the source of each tx
*/
type TxInNetwork struct {
	*pb.Transaction
	PeerID string
	Peer   *PeerEngine
}

//recordValidator write the incoming tx into a msg-streaming topic
type recordValidator struct {
	//bind the topic map in NodeEngine
	txtopic   map[string]litekfk.Topic
	nameTrans CCNameTransformer
	peer      *PeerEngine
}

type recordValidatorByID struct {
	peerID string
	*recordValidator
}

func (r *recordValidator) GetPreHandler(id string) (cred.TxPreHandler, error) {
	return &recordValidatorByID{id, r}, nil
}
func (r *recordValidator) ValidatePeerStatus(string, *pb.PeerTxState) error { return nil }
func (r *recordValidator) RemovePreHandler(string)                          {}

func (r *recordValidatorByID) TransactionPreValidation(tx *pb.Transaction) (*pb.Transaction, error) {

	ccname := r.nameTrans(string(tx.GetChaincodeID()))

	topic, ok := r.txtopic[ccname]
	if !ok {
		topic, ok = r.txtopic[""]
		if !ok {
			return tx, nil
		}
	}

	if err := topic.Write(&TxInNetwork{tx, r.peerID, r.peer}); err != nil {
		logger.Errorf("Write into topic [%s] fail: %s", ccname, err)
		return tx, cred.ValidateInterrupt
	}

	return tx, nil
}

func (f *recordValidatorByID) Release() {}

//txPoolValidator write the incoming tx into the txpool of ledger
type txPoolValidator struct {
	l *ledger.Ledger
}

func (t txPoolValidator) TransactionPreValidation(tx *pb.Transaction) (*pb.Transaction, error) {
	t.l.PoolTransactions([]*pb.Transaction{tx})
	return tx, nil
}

func (txPoolValidator) Release() {}
