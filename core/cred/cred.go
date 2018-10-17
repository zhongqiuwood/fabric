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

type Credentials struct {
	PeerValidator PeerCreds
	TxEndorserDef TxEndorserFactory
	TxValidator   TxHandlerFactory
}

/*

 -- entries for peer's credentials ---

*/

type PeerCreds interface {
	SelfPeerId() string
	PeerIdCred() []byte
	VerifyPeer(string, []byte) error
}

/*

 -- entries for transaction's credentials ---

*/
type TxEndorserFactory interface {
	EndorserId() string
	EndorsePeerState(*pb.PeerTxState) (*pb.PeerTxState, error)
	GetEndorser(attr ...string) (TxEndorser, error)
}

type TxEndorser interface {
	EndorseTransaction(*pb.Transaction) (*pb.Transaction, error)
	Release()
}

type TxHandlerFactory interface {
	ValidatePeerStatus(id string, status *pb.PeerTxState) error
	GetPreHandler(id string) (TxPreHandler, error)
	RemovePreHandler(string)
}

type TxPreHandler interface {
	TransactionPreValidation(*pb.Transaction) (*pb.Transaction, error)
	Release()
}
