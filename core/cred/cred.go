package credentials

import (
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("credential")

/*
	just like fabric, this is a module for manufacturing, managing and verifing
	credentials of txs and peers, base on the certificates with x.509 standard,
	and it was mostly the refactoring of crypto module in fabric 0.6
	(and somewhat like the mxing of bccsp & msp in fabric 1.0)
*/

type CredentialCore struct {
	TxValidator TxHandlerFactory
}

/*

 -- entries for peer's credentials ---

*/

type PeerVerifier interface {
}

/*

 -- entries for transaction's credentials ---

*/

//TxHandler Factory is for EVERY POSSIBLE PEER while TxEndorserFactory is for A SINGLE PEER
type TxHandlerFactory interface {
	GetPreHandler(id string, status *pb.PeerTxState) (TxPreHandler, error)
	RemovePreHandler(string)
}

type TxPreHandler interface {
	TransactionPreValidation(*pb.Transaction) (*pb.Transaction, error)
	Release()
}

type TxEndorserFactory interface {
	GetEndorser(attr ...string) (TxEndorser, error)
}

type TxEndorser interface {
	EndorseTransaction(*pb.Transaction) (*pb.Transaction, error)
	Release()
}
