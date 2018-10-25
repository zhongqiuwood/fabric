package api

import (
	"fmt"
	"github.com/abchain/fabric/core/chaincode/container/inproccontroller"
	"github.com/abchain/fabric/core/chaincode/shim"
	"github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
)

type EmbeddedChaincode struct {

	//Unique name of the embedded chaincode
	Name string

	// Chaincode is the actual chaincode object
	Chaincode shim.Chaincode
}

//general embedded only need to register before deploy ...
var ccReg map[string]string

const (
	Embedded_Dummy_Path = "embedded/"
)

func init() {
	ccReg = make(map[string]string)
}

func RegisterECC(ecc *EmbeddedChaincode) error {

	name := ecc.Name

	_, ok := ccReg[name]
	if ok {
		return fmt.Errorf("register dupliated embedded chaincode", name)
	}

	regPath := Embedded_Dummy_Path + name

	err := inproccontroller.Register(regPath, ecc.Chaincode)
	if err != nil {
		return fmt.Errorf("could not register embedded chaincode", name, err)
	}

	ccReg[name] = regPath

	return nil
}

func IsEmbedded(name string) bool {
	_, ok := ccReg[name]
	return ok
}

// "build" a given chaincode code (the origin buildSysCC)
func BuildEmbeddedCC(context context.Context, spec *protos.ChaincodeSpec) (*protos.ChaincodeDeploymentSpec, error) {

	regPath, ok := ccReg[spec.ChaincodeID.Name]
	if !ok {
		return nil, fmt.Errorf("Embedded chaincode not found", spec.ChaincodeID.Name)
	}

	dspec := *spec
	dspec.ChaincodeID.Path = regPath

	chaincodeDeploymentSpec := &protos.ChaincodeDeploymentSpec{ExecEnv: protos.ChaincodeDeploymentSpec_SYSTEM, ChaincodeSpec: &dspec}
	return chaincodeDeploymentSpec, nil
}
