package credentials

/*
	a default tx validator for txnetwork, this is supposed to be compatible with
	many possible endorsement schemes base on x.509 certificate, including the
	old 0.6 membersrvc's tcert(with the intrinsic kdf being disabled)

	we purpose a key derivation algorithm similar to the tcert derivation in 0.6
	but replace the tcertid with nounce of tx, and the KDF is come from the gossip
	network id.
*/

import (
	"crypto/ecdsa"
	"crypto/x509"
	"errors"
	"github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/core/crypto/primitives"
	"github.com/abchain/fabric/core/crypto/utils"
	pb "github.com/abchain/fabric/protos"
	"sync"
)

type txHandlerDefault struct {
	sync.RWMutex
	rootVerifier *x509ExtVerifier
	disableKDF   bool
	cache        map[string]*txVerifier
}

func NewDefaultTxHandler(disableKDF bool) TxHandlerFactory {
	return &txHandlerDefault{
		disableKDF: disableKDF,
		cache:      make(map[string]*txVerifier),
	}
}

func (f *txHandlerDefault) UpdateRootCA(certs []*x509.Certificate) {
	verifier := NewX509ExtVerifer(certs)
	f.Lock()
	//when verifier is changed, all the cache must be also cleared
	f.cache = make(map[string]*txVerifier)
	f.rootVerifier = verifier
	f.Unlock()
}

func (f *txHandlerDefault) GetPreHandler(id string, s *pb.PeerTxState) (TxPreHandler, error) {

	f.RLock()

	if ret, ok := f.cache[id]; !ok {
		//add state's endorsement (certitiface) into cache
		var err error
		f.RUnlock()
		f.Lock()
		defer f.Unlock()
		ret, err = newVerifier(id, s)
		ret.rootVerifier = f.rootVerifier
		if f.disableKDF {
			ret.disableKDF = true
		}

		f.cache[id] = ret

		return ret, err

	} else {
		f.RUnlock()
		return ret, nil
	}
}

func (f *txHandlerDefault) RemovePreHandler(id string) {
	f.Lock()
	defer f.Unlock()

	delete(f.cache, id)
}

func newVerifier(id string, s *pb.PeerTxState) (*txVerifier, error) {

	cer, err := primitives.DERToX509Certificate(s.Endorsement)
	if err != nil {
		return nil, err
	}

	ret := new(txVerifier)
	ret.cert_der = s.Endorsement
	ret.ecert = cer
	ret.eid = id

	if cer.IsCA {
		ret.ecertPool = x509.NewCertPool()
		ret.ecertPool.AddCert(cer)
	}

	return ret, nil
}

type txVerifier struct {
	eid          string
	rootVerifier *x509ExtVerifier
	disableKDF   bool
	cert_der     []byte
	ecert        *x509.Certificate
	ecertPool    *x509.CertPool
}

func (v *txVerifier) Release() {}

func (v *txVerifier) TransactionPreValidation(tx *pb.Transaction) (*pb.Transaction, error) {

	var refCert *x509.Certificate
	var err error

	//the key derivation from tcert protocol
	if tx.Cert != nil {

		refCert, err = primitives.DERToX509Certificate(tx.Cert)
		if err != nil {
			return nil, err
		}

		if v.rootVerifier == nil {
			return nil, x509.UnknownAuthorityError{Cert: refCert}
		}

		_, _, err = v.rootVerifier.Verify(refCert, v.ecertPool)
		if err != nil {
			return nil, err
		}

	} else {
		refCert = v.ecert
		//also mutate the transaction (fill the cert into transaction)
		tx.Cert = v.cert_der
	}

	//certVerifier.
	certPubkey, ok := refCert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("Tcert not use ecdsa signature")
	}

	if !v.disableKDF {
		certPubkey, err = crypto.TxCertKeyDerivationEC(certPubkey, []byte(v.eid), tx.GetNonce())
		if err != nil {
			return nil, err
		}
	}

	txmsg, err := crypto.TxToSignatureMsg(tx)
	if err != nil {
		return nil, err
	}

	ok, err = primitives.ECDSAVerify(certPubkey, txmsg, tx.GetSignature())
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, utils.ErrInvalidTransactionSignature
	}

	//also prune signature because we do not need it anymore
	tx.Signature = nil
	return tx, nil
}

type TxHandlerFactoryX509 interface {
	TxHandlerFactory
	UpdateRootCA([]*x509.Certificate)
}

//in case if we have a custom txhandler ...
func UpdateRootCAForTxHandler(h_in TxHandlerFactory, certs []*x509.Certificate) {
	h, ok := h_in.(TxHandlerFactoryX509)
	if !ok {
		logger.Errorf("Wrong param, not default txhandler")
	}

	h.UpdateRootCA(certs)
}
