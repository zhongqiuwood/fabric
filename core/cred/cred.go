package credentials

import (
	"fmt"
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

	//additional selectable endorser
	TxEndorsers map[string]TxEndorserFactory
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
	EndorserId() []byte //notice the endorserid is bytes
	//EndorsePeerState need to consider the exist endorsment field and decide update it or not
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

/*

 -- entries for transaction's confidentiality ---

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

func (cred *Credentials) SelectEndorser(name string) (TxEndorserFactory, error) {

	if cred.TxEndorsers != nil {
		opt, ok := cred.TxEndorsers[name]
		if ok {
			return opt, nil
		}
	}

	//TODO: we can create external type of endorser if being configured to
	return nil, fmt.Errorf("Specified endorser %s is not exist", name)

}
