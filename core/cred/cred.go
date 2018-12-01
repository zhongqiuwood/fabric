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

/*

 -- entries for per-peer (per-network)'s credentials ---

*/

//peer creds also include the endorse entry because it should be sole per-network
//peer's cred should also derive a default TxHandlerFactory
type PeerCreds interface {
	PeerPki() []byte
	PeerCred() []byte
	EndorsePeerMsg(msg *pb.Message) (*pb.Message, error)
	VerifyPeerMsg(pki []byte, msg *pb.Message) error
	VerifyPeerCred([]byte) error
	DeriveTxCred() TxHandlerFactory
}

type TxHandlerFactory interface {
	ValidatePeerStatus(id string, status *pb.PeerTxState) error
	GetPreHandler(id string) (TxPreHandler, error)
	//notify all of the preparing for a specified id (i.e. caches) can be complete released
	RemovePreHandler(id string)
}

type TxPreHandler interface {
	TransactionPreValidation(*pb.Transaction) (*pb.Transaction, error)
	Release()
}

/*

 -- entries for per-user's credentials, user can be actived in mutiple networks---

*/
type TxEndorserFactory interface {
	EndorserId() []byte //notice the endorserid is bytes
	//EndorsePeerState need to consider the exist endorsment field and decide update it or not
	EndorsePeerState(*pb.PeerTxState) (*pb.PeerTxState, error)
	GetEndorser(attr ...string) (TxEndorser, error)
}

type TxEndorser interface {
	EndorseTransaction(*pb.Transaction) (*pb.Transaction, error)
	Release()
}

/*

 -- entries for per-platform's confidentiality ---

*/

// DataEncryptor is used to encrypt/decrypt chaincode's state data
type DataEncryptor interface {
	Encrypt([]byte) ([]byte, error)
	Decrypt([]byte) ([]byte, error)
}

//YA-fabric 0.9
//it is supposed to be created from something like a certfication but will not
//get an implement in recent
type TxConfidentialityHandler interface {
	//decrypt the transaction
	TransactionPreExecution(*pb.Transaction) (*pb.Transaction, error)
	//returns a DataEncryptor linked to pair defined by
	//the deploy transaction and the execute transaction.
	GenDataEncryptor(deployTx, executeTx *pb.Transaction) (DataEncryptor, error)
}
