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
	immeCAPool   *x509.CertPool
	immeCAs      []*x509.Certificate
	disableKDF   bool
	cache        map[string]*txVerifier
	certcache    map[string]*x509.Certificate
}

func NewDefaultTxHandler(disableKDF bool) TxHandlerFactory {
	return &txHandlerDefault{
		disableKDF: disableKDF,
		cache:      make(map[string]*txVerifier),
	}
}

func (f *txHandlerDefault) UpdateRootCA(CAs []*x509.Certificate, immeCAs []*x509.Certificate) {
	verifier := NewX509ExtVerifer(CAs)
	var immePool *x509.CertPool
	if len(immeCAs) != 0 {
		immePool = x509.NewCertPool()
		for _, ca := range immeCAs {
			immePool.AddCert(ca)
		}
	}

	f.Lock()
	//when verifier is changed, all the cache must be also cleared
	f.cache = make(map[string]*txVerifier)
	f.rootVerifier = verifier
	f.immeCAs = immeCAs
	f.immeCAPool = immePool
	f.Unlock()
}

func (f *txHandlerDefault) ValidatePeer(id string, status *pb.PeerTxState) error {

	cer, err := primitives.DERToX509Certificate(status.Endorsement)
	if err != nil {
		return err
	} else if len(cer.UnhandledCriticalExtensions) > 0 {
		//the peer's cert must not include extension
		return x509.UnhandledCriticalExtension{}
	}

	_, err = f.rootVerifier.Verify(cer, nil)
	if err != nil {
		return err
	}

	certPubkey, ok := cer.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return errors.New("Tcert not use ecdsa signature")
	}

	smsg, err := status.MsgEncode(id)
	if err != nil {
		return err
	}

	ok, err = primitives.ECDSAVerify(certPubkey, smsg, status.Signature)
	if err != nil {
		return err
	} else if !ok {
		return utils.ErrInvalidTransactionSignature
	}

	logger.Infof("Verify a peer's x509 certificate [%x]", cer.SubjectKeyId)

	//peer's cert can be updated but we require it must have same
	//pubkey as the original one
	f.Lock()
	defer f.Unlock()

	if ret, ok := f.certcache[id]; ok {
		oldpk := ret.PublicKey.(*ecdsa.PublicKey)
		if certPubkey.X.Cmp(oldpk.X) != 0 {
			return errors.New("Tcert is not match with previous")
		}
	}

	//update cache, and rebuild verifier
	f.certcache[id] = cer
	f.cache[id] = newVerifier(id, cer, f)

	return nil
}

func (f *txHandlerDefault) GetPreHandler(id string) (TxPreHandler, error) {

	f.RLock()

	if ret, ok := f.cache[id]; !ok {

		f.RUnlock()
		f.Lock()
		defer f.Unlock()
		cert, ok := f.certcache[id]
		if !ok {
			return nil, errors.New("Peer has not been verified")
		}

		//rebuild cache
		ret = newVerifier(id, cert, f)
		f.cache[id] = ret

		return ret, nil

	} else {
		f.RUnlock()
		return ret, nil
	}
}

func (f *txHandlerDefault) RemovePreHandler(id string) {
	f.Lock()
	defer f.Unlock()

	delete(f.cache, id)
	delete(f.certcache, id)
}

func newVerifier(id string, cert *x509.Certificate, parent *txHandlerDefault) *txVerifier {

	ret := new(txVerifier)
	ret.ecert = cert
	ret.eid = id
	ret.rootVerifier = parent.rootVerifier
	ret.disableKDF = parent.disableKDF
	ret.ecertPool = x509.NewCertPool()

	if len(parent.immeCAs) != 0 {
		for _, ca := range parent.immeCAs {
			ret.ecertPool.AddCert(ca)
		}
	}

	if cert.IsCA {
		ret.ecertPool.AddCert(cert)
	}

	return ret
}

type txVerifier struct {
	eid          string
	rootVerifier *x509ExtVerifier
	disableKDF   bool
	ecert        *x509.Certificate
	ecertPool    *x509.CertPool
}

func (v *txVerifier) Release() {}

func (v *txVerifier) TransactionPreValidation(tx *pb.Transaction) (*pb.Transaction, error) {

	//the key derivation from tcert protocol
	certPubkey, err := crypto.TxCertKeyDerivationEC(v.ecert.PublicKey.(*ecdsa.PublicKey), []byte(v.eid), tx.GetNonce())
	if err != nil {
		return nil, err
	}

	var refCert *x509.Certificate

	if tx.Cert != nil {
		refCert, err = primitives.DERToX509Certificate(tx.Cert)
		if err != nil {
			return nil, err
		}

		if v.rootVerifier == nil {
			return nil, x509.UnknownAuthorityError{Cert: refCert}
		}

		_, _, err = v.rootVerifier.VerifyWithAttr(refCert, v.ecertPool)
		if err != nil {
			return nil, err
		}

		txcertPk, ok := refCert.PublicKey.(*ecdsa.PublicKey)
		if !ok {
			return nil, errors.New("Tcert not use ecdsa signature")
		}

		if !v.disableKDF {
			//must verify the derived key and the key in cert
			//we just compare X for it was almost impossible for attacker to reuse
			//a cert which pubkey has same X but different Y
			if txcertPk.X.Cmp(certPubkey.X) != 0 {
				return nil, errors.New("Pubkey of tx cert is not matched to the derived key")
			}

		} else {
			//case for compatible, just drop the drived key and use the cert one
			certPubkey = txcertPk
		}

	} else {
		refCert = v.ecert
		//also mutate the transaction (fill the cert into transaction)
		tx.Cert = v.ecert.Raw
	}

	txmsg, err := crypto.TxToSignatureMsg(tx)
	if err != nil {
		return nil, err
	}

	ok, err := primitives.ECDSAVerify(certPubkey, txmsg, tx.GetSignature())
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
	UpdateRootCA(ca []*x509.Certificate, immeca []*x509.Certificate)
}

//in case if we have a custom txhandler ...
func UpdateRootCAForTxHandler(h_in TxHandlerFactory, CAs []*x509.Certificate, immeCAs ...*x509.Certificate) {
	h, ok := h_in.(TxHandlerFactoryX509)
	if !ok {
		logger.Errorf("Wrong param, not default txhandler")
	}

	h.UpdateRootCA(CAs, immeCAs)
}
