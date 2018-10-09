package credentials

/*
	a default handler route for txnetwork:

	we purpose a key derivation algorithm similar to the tcert derivation in 0.6
	but replace the tcertid with nounce of tx, and the KDF is come from the gossip
	network id
*/

import (
	"bytes"
	"container/list"
	"crypto/x509"
	"encoding/asn1"
	"fmt"
	"github.com/abchain/fabric/core/crypto/primitives"
	"github.com/abchain/fabric/core/crypto/utils"
	pb "github.com/abchain/fabric/protos"
	"sync"
)

var CertCacheLimit = 4096

type TxHandlerDefault struct {
	sync.RWMutex

	lru   *list.List
	cache map[string]*list.Element
}

type txVerifier struct {
	cert_der []byte
	ecert    *x509.Certificate
}
