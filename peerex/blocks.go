package peerex

import (
	"fmt"
	"encoding/base64"
	pb "github.com/hyperledger/fabric/protos"
	proto "github.com/golang/protobuf/proto"
)

func DecodeChaincodeName(payload string) (string, error){
	
	bytes, err := base64.StdEncoding.DecodeString(payload)
	
	if err != nil{
		return "", fmt.Errorf("baes64 decode fail %s", err.Error())
	}
	
	id := &pb.ChaincodeID{}
	
	err = proto.Unmarshal(bytes, id)

	if err != nil{
		return "", fmt.Errorf("protobuf decode fail %s", err.Error())
	}
	
	return id.Name, nil
}


func DecodeTransactionToInvoke(payload string) (*pb.ChaincodeInvocationSpec, error){
	
	bytes, err := base64.StdEncoding.DecodeString(payload)
	
	if err != nil{
		return nil, fmt.Errorf("baes64 decode fail %s", err.Error())
	}
	
	invoke := &pb.ChaincodeInvocationSpec{}
	
	err = proto.Unmarshal(bytes, invoke)

	if err != nil{
		return nil, fmt.Errorf("protobuf decode fail %s", err.Error())
	}
	
	if invoke.ChaincodeSpec == nil || invoke.ChaincodeSpec.CtorMsg == nil ||
	 invoke.ChaincodeSpec.CtorMsg.Args == nil || len(invoke.ChaincodeSpec.CtorMsg.Args) == 0{
		return nil, fmt.Errorf("Uninitialized invoke tx")
	}	
	
	return invoke, nil
}

