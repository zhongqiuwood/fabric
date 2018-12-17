package embedded_chaincode

import (
	"fmt"

	"github.com/abchain/fabric/core/chaincode"
	"github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

var sysccLogger = logging.MustGetLogger("sysccapi")

func verifySysCC() ([]*api.SystemChaincode, error) {

	wlchaincodes := viper.GetStringMapString("chaincode.system")

	var vcc []*api.SystemChaincode
	for _, syscc := range api.ListSysCC() {
		if val, ok := wlchaincodes[syscc.Name]; ok && (val == "enable" || val == "true" || val == "yes") {

			if err := api.RegisterECC(&api.EmbeddedChaincode{syscc.Name, syscc.Chaincode}); err != nil {
				sysccLogger.Warningf("Can not register syscc %s as embedded cc: %s", syscc.Name, err)
				if syscc.Enforced {
					return nil, fmt.Errorf("system chaincode <%s> require to be launched but fail: %s", syscc.Name, err)
				}
			} else {
				vcc = append(vcc, syscc)
			}
		} else {
			if syscc.Enforced {
				return nil, fmt.Errorf("system chaincode <%s> require to be launched but not on whitelist", syscc.Name)
			}

		}
	}

	return vcc, nil

}

func RegisterSysCCs(ledger *ledger.Ledger, chain *chaincode.ChaincodeSupport) error {

	sysccs, err := verifySysCC()
	if err != nil {
		return err
	}

	for _, sysCC := range sysccs {
		if err = sysccInit(context.Background(), ledger, chain, sysCC); err != nil {
			return err
		}
	}
	return nil
}

func sysccInit(ctxt context.Context, l *ledger.Ledger, chain *chaincode.ChaincodeSupport, syscc *api.SystemChaincode) error {
	// First build and get the deployment spec
	chaincodeID := &protos.ChaincodeID{Path: syscc.Path, Name: syscc.Name}
	spec := protos.ChaincodeSpec{Type: protos.ChaincodeSpec_Type(protos.ChaincodeSpec_Type_value["GOLANG"]), ChaincodeID: chaincodeID, CtorMsg: &protos.ChaincodeInput{Args: syscc.InitArgs}}

	chaincodeDeploymentSpec, err := api.BuildEmbeddedCC(ctxt, &spec)

	if err != nil {
		return fmt.Errorf("Error deploying chaincode spec (%s): %s", syscc.Name, err)
	}

	err, chrte := chain.Launch(ctxt, l, chaincodeID, chaincodeDeploymentSpec)
	if err != nil {
		return fmt.Errorf("Failed to launch chaincode spec (%s): %s", syscc.Name, err)
	}

	dummyout := ledger.TxExecStates{}
	dummyout.InitForInvoking(l)
	//here we never mark ledger into tx status, so init in syscc NEVER write state
	_, err = chain.ExecuteLite(ctxt, chrte, protos.Transaction_CHAINCODE_DEPLOY, spec.CtorMsg, dummyout)
	if err != nil {
		return fmt.Errorf("Failed to init chaincode spec(%s): %s", syscc.Name, err)
	}

	if !dummyout.IsEmpty() {
		sysccLogger.Warning("system chaincode [%s] set states in init, which will be just discarded", syscc.Name)
	}

	sysccLogger.Infof("system chaincode [%s] is launched for ledger <%p>", syscc.Name, l)
	return nil
}
