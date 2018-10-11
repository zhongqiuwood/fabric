package cred_def

import (
	"crypto/ecdsa"
	"crypto/x509"
	"errors"
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/core/crypto/primitives"
	pb "github.com/abchain/fabric/protos"
)

//this default endorser's factory is just itself, and attribute is not supported
type txEndorser struct {
	eid       string
	issuer    *x509.Certificate
	issuerKey *ecdsa.PrivateKey
}

func (ed *txEndorser) GetEndorser(attr ...string) (cred.TxEndorser, error) {
	if len(attr) > 0 {
		return nil, errors.New("Endorser not support attributes")
	}
	return ed, nil
}

func (ed *txEndorser) Release() {}

func (ed *txEndorser) EndorseTransaction(tx *pb.Transaction) (*pb.Transaction, error) {

	dprik, err := crypto.TxCertPrivKDerivationEC(ed.issuerKey, []byte(ed.eid), tx.GetNonce())
	if err != nil {
		return nil, err
	}

	txmsg, err := crypto.TxToSignatureMsg(tx)
	if err != nil {
		return nil, err
	}

	sign, err := primitives.ECDSASign(dprik, txmsg)
	if err != nil {
		return nil, err
	}

	tx.Signature = sign
	return tx, nil

}
