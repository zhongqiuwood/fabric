package peerex

import (
	_ "github.com/hyperledger/fabric/peer/common"
	"github.com/hyperledger/fabric/core/peer"
	"google.golang.org/grpc"
)

type ClientConn struct{	
	C *grpc.ClientConn
}

func (conn *ClientConn) Dialdefault() error{
	c, err := peer.NewPeerClientConnection()
	if err != nil{
		return err
	}
	
	conn.C = c
	return nil
}


