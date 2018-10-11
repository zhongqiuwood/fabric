package credentials

import (
	"crypto/x509"
	"encoding/asn1"
	"github.com/abchain/fabric/core/crypto/utils"
)

var (
	HyperledgerFabricAttrHId = asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6, 9}
	HyperledgerFabricTCertId = asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6, 7}
)

/*
	x509 verifier verify a x509 chain, require all of the critical ext must also
	be included in root CA (MUST match), the immediate CA is omitted
*/
type x509ExtVerifier struct {
	rootCA    *x509.CertPool
	rootCache map[*x509.Certificate]*utils.ObjIdIndex
}

func NewX509ExtVerifer(certs []*x509.Certificate) *x509ExtVerifier {
	ret := &x509ExtVerifier{
		rootCA:    x509.NewCertPool(),
		rootCache: make(map[*x509.Certificate]*utils.ObjIdIndex),
	}

	for _, cert := range certs {
		ind := new(utils.ObjIdIndex)
		for _, ext := range cert.Extensions {
			//ext.value is nonsense but we just add it ...
			ind.AddItem(ext.Value, ext.Id)
		}
		// add the hyperledger id to be compatible even with the old tcerts from membersrvc
		ind.AddItem(true, HyperledgerFabricAttrHId)
		ind.AddItem(true, HyperledgerFabricTCertId)

		ret.rootCA.AddCert(cert)
		ret.rootCache[cert] = ind
	}

	return ret
}

func (v *x509ExtVerifier) verifyExt(chains [][]*x509.Certificate, exts []asn1.ObjectIdentifier) []*x509.Certificate {

	for _, chain := range chains {
		ca := chain[len(chain)-1]
		r, ok := v.rootCache[ca]
		if !ok {
			//something wrong? but we just skip it...
			logger.Warningf("Could not found root cert in cache, something wrong? [%v]",
				ca.Subject)
			continue
		}

		for _, extid := range chain[0].UnhandledCriticalExtensions {
			if _, ok, _ = r.GetItem(extid); !ok {
				//ext is not matched
				break
			}
		}
		if ok {
			return chain
		}

	}

	return nil
}

/* return the FISRT chain with matched ext, if not found matched chain, return nil, and error indicate it*/
func (v *x509ExtVerifier) Verify(cert *x509.Certificate, immCA *x509.CertPool) ([][]*x509.Certificate, []*x509.Certificate, error) {

	opt := x509.VerifyOptions{
		Intermediates: immCA,
		Roots:         v.rootCA,
	}

	unhandledExt := cert.UnhandledCriticalExtensions
	cert.UnhandledCriticalExtensions = nil
	defer func() {
		cert.UnhandledCriticalExtensions = unhandledExt
	}()

	chains, err := cert.Verify(opt)
	if err != nil {
		return nil, nil, err
	}

	matched := v.verifyExt(chains, unhandledExt)
	if matched == nil {
		return chains, nil, x509.UnhandledCriticalExtension{}
	}

	return chains, matched, nil
}
