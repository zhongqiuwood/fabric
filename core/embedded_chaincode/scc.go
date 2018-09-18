package embedded_chaincode

import (
	"fmt"

	"github.com/abchain/fabric/core/chaincode"
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"golang.org/x/net/context"
)

var sysccLogger = logging.MustGetLogger("sysccapi")

func sysccInit(ctxt context.Context, ledger *ledger.Ledger, chain *chaincode.ChaincodeSupport, syscc *api.SystemChaincode) error {
	// First build and get the deployment spec
	chaincodeID := &protos.ChaincodeID{Path: syscc.Path, Name: syscc.Name}
	spec := protos.ChaincodeSpec{Type: protos.ChaincodeSpec_Type(protos.ChaincodeSpec_Type_value["GOLANG"]), ChaincodeID: chaincodeID, CtorMsg: &protos.ChaincodeInput{Args: syscc.InitArgs}}

	chaincodeDeploymentSpec, err := api.BuildEmbeddedCC(ctxt, &spec)

	if err != nil {
		sysccLogger.Error(fmt.Sprintf("Error deploying chaincode spec: %v\n\n error: %s", spec, err))
		return err
	}

	transaction, err := protos.NewChaincodeDeployTransaction(chaincodeDeploymentSpec, chaincodeDeploymentSpec.ChaincodeSpec.ChaincodeID.Name)
	if err != nil {
		return fmt.Errorf("Error deploying chaincode: %s ", err)
	}

	if err := chain.Deploy(ctxt, chaincodeDeploymentSpec); err != nil {
		return fmt.Errorf("Failed to deploy chaincode spec(%s)", err)
	}

	err, chrte := chain.Launch(ctxt, ledger, chaincodeID, chaincodeDeploymentSpec, transaction)
	if err != nil {
		return fmt.Errorf("Failed to launch chaincode spec(%s)", err)
	}

	//here we never mark ledger into tx status, so init in syscc NEVER write state
	_, err = chain.Execute(ctxt, chrte, spec.CtorMsg, transaction)
	if err != nil {
		return fmt.Errorf("Failed to init chaincode spec(%s)", err)
	}

	return nil
}

// deployLocal deploys the supplied chaincode image to the local peer (pass through devOps interface)
func deploySysCC(syscc *api.SystemChaincode) error {

	err := api.RegisterSysCC(syscc)
	if err != nil {
		sysccLogger.Error(fmt.Sprintf("deploy register fail (%s,%v): %s", syscc.Path, syscc, err))
		return err
	}

	l, err := ledger.GetLedger()
	if err != nil {
		sysccLogger.Errorf("Error acquiring ledger: %s", err)
		return err
	}

	err = sysccInit(context.Background(), l, chaincode.GetChain(chaincode.DefaultChain), syscc)
	if err != nil {
		sysccLogger.Errorf("Init syscc fail: %s", err)
		return err
	}

	return nil
}
