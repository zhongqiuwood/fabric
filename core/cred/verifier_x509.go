package credentials

/*a standard verifier for handling x.509 certificates*/

import (
	"bytes"
	"crypto/x509"
	"fmt"
	"github.com/abchain/fabric/core/crypto/primitives"
	"github.com/abchain/fabric/core/crypto/utils"
	pb "github.com/abchain/fabric/protos"
)

type X509Verifier struct {
	roots       *x509.CertPool
	attrManager *utils.ObjIdIndex
}
