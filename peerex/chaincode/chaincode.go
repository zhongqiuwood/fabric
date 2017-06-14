package chaincode

import (
	pb "github.com/hyperledger/fabric/protos"
	"github.com/hyperledger/fabric/peerex"
)

type ChaincodeInvoker struct{
	
	ChaincodeName string
	Conn		  *peerex.ClientConn	
}

func (c *ChaincodeInvoker) InvokeOrQuery(function string, args []string) (msg []byte, err error){
	
	return nil, nil
}


