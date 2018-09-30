package comm

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/op/go-logging"
	"github.com/spf13/viper"

	"github.com/abchain/fabric/core/util"
)

const defaultTimeout = time.Second * 3

var commLogger = logging.MustGetLogger("comm")

// NewClientConnectionWithAddress Returns a new grpc.ClientConn to the given address.
func NewClientConnectionWithAddress(peerAddress string, block bool, tslEnabled bool, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	if tslEnabled {
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	opts = append(opts, grpc.WithTimeout(defaultTimeout))
	if block {
		opts = append(opts, grpc.WithBlock())
	}
	conn, err := grpc.Dial(peerAddress, opts...)
	if err != nil {
		return nil, err
	}
	return conn, err
}

// InitTLSForPeer returns TLS credentials for peer
func InitTLSForPeer() credentials.TransportCredentials {
	var sn string
	if viper.GetString("peer.tls.serverhostoverride") != "" {
		sn = viper.GetString("peer.tls.serverhostoverride")
	}
	var creds credentials.TransportCredentials
	if rootcert := viper.GetString("peer.tls.rootcert.file"); rootcert != "" {
		var err error
		creds, err = credentials.NewClientTLSFromFile(util.CanonicalizeFilePath(rootcert), sn)
		if err != nil {
			commLogger.Fatalf("Failed to create TLS credentials %v", err)
		}
	} else {
		creds = credentials.NewClientTLSFromCert(nil, sn)
	}
	return creds
}
