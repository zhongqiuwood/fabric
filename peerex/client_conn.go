package client

import (
	fabricpeer_comm "github.com/hyperledger/fabric/peer/common"
	fabricpeer "github.com/hyperledger/fabric/core/peer"
)

type ClientConn struct{	
	fabricpeer_comm.DevopsConn
}

func (conn *ClientConn) Dialdefault() error{
	c, err := fabricpeer.NewPeerClientConnection()
	if err != nil{
		return err
	}
	
	conn.DevopsConn.C = c
	return nil
}


