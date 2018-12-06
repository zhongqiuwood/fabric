/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package service

import (
	"errors"
	"fmt"

	"github.com/op/go-logging"
	"golang.org/x/net/context"

	"github.com/abchain/fabric/core/chaincode"
	"github.com/abchain/fabric/core/chaincode/platforms"
	"github.com/abchain/fabric/core/config"
	ecc "github.com/abchain/fabric/core/embedded_chaincode/api"
	"github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/node"
	pb "github.com/abchain/fabric/protos"
)

var clisrvLogger = logging.MustGetLogger("server")

// NewDevopsServer creates and returns a new Devops server instance.
func NewDevopsServer(p *node.NodeEngine) *Devops {

	d := new(Devops)
	d.node = p

	clisrvLogger.Info("Devops use txnetwork")

	return d
}

// Devops implementation of Devops services
type Devops struct {
	node *node.NodeEngine
}

// TODO: login should become part of the cred
func (*Devops) Login(ctx context.Context, secret *pb.Secret) (*pb.Response, error) {

	return &pb.Response{Status: pb.Response_FAILURE, Msg: []byte("No implement")}, nil
}

// Build builds the supplied chaincode image
func (d *Devops) Build(context context.Context, spec *pb.ChaincodeSpec) (*pb.ChaincodeDeploymentSpec, error) {

	chaincodeDeploymentSpec, err := d.getChaincodeBytes(context, spec)
	if err != nil {
		clisrvLogger.Error(fmt.Sprintf("Error build chaincode spec: %v\n\n error: %s", spec, err))
		return nil, err
	}

	var codePackageBytes []byte
	codePackageBytes = chaincodeDeploymentSpec.CodePackage
	if codePackageBytes == nil {
		return nil, fmt.Errorf("No codepackage under this mode")
	}

	// YA-fabric: not build image on local site any more
	// vm, err := container.NewVM()
	// if err != nil {
	// 	return nil, fmt.Errorf("Error getting vm")
	// }

	// err = vm.BuildChaincodeContainer(spec, codePackageBytes)
	// if err != nil {
	// 	clisrvLogger.Error(fmt.Sprintf("%s", err))
	// 	return nil, err
	// }

	return chaincodeDeploymentSpec, nil

}

// get chaincode bytes
func (*Devops) getChaincodeBytes(context context.Context, spec *pb.ChaincodeSpec) (*pb.ChaincodeDeploymentSpec, error) {

	chainName := chaincode.DefaultChain
	chain := chaincode.GetChain(chainName)
	if chain == nil {
		return nil, fmt.Errorf("No corresponding chain:", chainName)
	}

	//handling embedded chaincode
	if ecc.IsEmbedded(spec.ChaincodeID.Name) {
		return ecc.BuildEmbeddedCC(context, spec)
	}

	var codePackageBytes []byte
	if !chain.UserRunsCC() {
		clisrvLogger.Debugf("Received build request for chaincode spec: %v", spec)
		var err error
		if err = CheckSpec(spec); err != nil {
			return nil, err
		}

		codePackageBytes, err = chaincode.GetChaincodePackageBytes(spec)
		if err != nil {
			err = fmt.Errorf("Error getting chaincode package bytes: %s", err)
			clisrvLogger.Error(fmt.Sprintf("%s", err))
			return nil, err
		}
	}
	chaincodeDeploymentSpec := &pb.ChaincodeDeploymentSpec{ChaincodeSpec: spec, CodePackage: codePackageBytes}
	return chaincodeDeploymentSpec, nil
}

// Deploy deploys the supplied chaincode image to the validators through a transaction
func (d *Devops) Deploy(ctx context.Context, spec *pb.ChaincodeSpec) (*pb.ChaincodeDeploymentSpec, error) {
	// get the deployment spec
	chaincodeDeploymentSpec, err := d.getChaincodeBytes(ctx, spec)

	if err != nil {
		return nil, fmt.Errorf("Error deploying chaincode spec: %v\n\n error: %s", spec, err)
	}

	tx, err := pb.NewChaincodeDeployTransaction(chaincodeDeploymentSpec, "")
	if err != nil {
		return nil, fmt.Errorf("Error deploying chaincode: %s ", err)
	}

	resp, err := d.deliverTx(ctx, tx, spec, true)
	if err != nil {
		return nil, err
	}
	clisrvLogger.Infof("Deploy chaincode [%s] done: [%v]", spec.ChaincodeID.GetName(), resp)

	return chaincodeDeploymentSpec, nil

}

func (d *Devops) deliverTx(ctx context.Context, tx *pb.Transaction, spec *pb.ChaincodeSpec, invoke bool) (*pb.Response, error) {

	//Notice: YA-fabric enforce a deploy name is also specified
	if spec.ChaincodeID.Name == "" {
		return nil, fmt.Errorf("name not given")
	}

	clisrvLogger.Debugf("Sending [%s] transaction with sec [%s %v] to txnetwork", tx.Type, spec.SecureContext, spec.Attributes)
	var resp *pb.Response
	var err error
	if config.SecurityEnabled() && spec.SecureContext != "" {

		ed, err := d.node.SelectEndorser(spec.SecureContext)
		if err != nil {
			return nil, fmt.Errorf("Obtain endorser failure: %s", err)
		}

		txed, err := ed.GetEndorser(spec.Attributes...)
		if err != nil {
			return nil, fmt.Errorf("Create tx endorser failure: %s", err)
		}

		if invoke {
			resp = d.node.ExecuteTransaction(ctx, tx, txed, d.node.DefaultPeer())[0]
		} else {
			resp = d.node.QueryTransaction(ctx, tx, txed, d.node.DefaultLedger(), d.node.DefaultPeer())
		}

	} else {
		//we simply omit securecontext even it was specified
		if invoke {
			resp = d.node.ExecuteTransaction(ctx, tx, nil, d.node.DefaultPeer())[0]
		} else {
			resp = d.node.QueryTransaction(ctx, tx, nil, d.node.DefaultLedger(), d.node.DefaultPeer())
		}
	}

	if resp.Status == pb.Response_FAILURE {
		err = fmt.Errorf(string(resp.Msg))
	}
	return resp, err

}

// Invoke performs the supplied invocation on the specified chaincode through a transaction
func (d *Devops) Invoke(ctx context.Context, chaincodeInvocationSpec *pb.ChaincodeInvocationSpec) (*pb.Response, error) {

	tx, err := pb.NewChaincodeExecute(chaincodeInvocationSpec, "", pb.Transaction_CHAINCODE_INVOKE)
	if nil != err {
		return nil, fmt.Errorf("Error invoking chaincode: %s ", err)
	}

	return d.deliverTx(ctx, tx, chaincodeInvocationSpec.ChaincodeSpec, true)
}

// Query performs the supplied query on the specified chaincode through a transaction
func (d *Devops) Query(ctx context.Context, chaincodeInvocationSpec *pb.ChaincodeInvocationSpec) (*pb.Response, error) {

	tx, err := pb.NewChaincodeExecute(chaincodeInvocationSpec, util.GenerateUUID(), pb.Transaction_CHAINCODE_QUERY)
	if nil != err {
		return nil, fmt.Errorf("Error query chaincode: %s ", err)
	}

	return d.deliverTx(ctx, tx, chaincodeInvocationSpec.ChaincodeSpec, false)
}

// CheckSpec to see if chaincode resides within current package capture for language.
func CheckSpec(spec *pb.ChaincodeSpec) error {
	// Don't allow nil value
	if spec == nil {
		return errors.New("Expected chaincode specification, nil received")
	}

	platform, err := platforms.Find(spec.Type)
	if err != nil {
		return fmt.Errorf("Failed to determine platform type: %s", err)
	}

	return platform.ValidateSpec(spec)
}