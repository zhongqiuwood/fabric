package credentials

/*a standard verifier for handling x.509 certificates*/

import (
	"bytes"
	"crypto/x509"
	"encoding/asn1"
	"fmt"
	"github.com/abchain/fabric/core/crypto/primitives"
	"github.com/abchain/fabric/core/crypto/utils"
	pb "github.com/abchain/fabric/protos"
)

type X509ExtendHandler interface {
	Handle(asn1.ObjectIdentifier, []byte) error
}

type X509Verifier struct {
	roots *x509.CertPool
}

func (v X509Verifier) VerifyFull(cert *x509.Certificate, extH X509ExtendHandler, immca *x509.Certificate...) error{

}

func (v X509Verifier) Prehandling(cert *x509.Certificate, extH X509ExtendHandler) (*x509.Certificate, error){
	
}

func (v X509Verifier) Verify(cert *x509.Certificate, immca *x509.Certificate...) error{
	
}