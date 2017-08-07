package peerex

import (
	
	"github.com/hyperledger/fabric/core/comm"
	"google.golang.org/grpc"
	"github.com/spf13/viper"
)

type ClientConn struct{	
	C *grpc.ClientConn
}

// NewPeerClientConnection Returns a new grpc.ClientConn to the configured local PEER.
func newPeerClientConnection() (*grpc.ClientConn, error) {
	return newPeerClientConnectionWithAddress(viper.GetString("service.cliaddress"))
}

// NewPeerClientConnectionWithAddress Returns a new grpc.ClientConn to the configured PEER.
func newPeerClientConnectionWithAddress(peerAddress string) (*grpc.ClientConn, error) {
	if comm.TLSEnabledforService() {
		return comm.NewClientConnectionWithAddress(peerAddress, false, true, comm.InitTLSForPeer())
	}
	return comm.NewClientConnectionWithAddress(peerAddress, false, false, nil)
}

func (conn *ClientConn) Dialdefault() error{
	c, err := newPeerClientConnection()
	if err != nil{
		return err
	}
	
	conn.C = c
	return nil
}


