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

package platforms

import (
	"archive/tar"
	"fmt"

	"github.com/abchain/fabric/core/chaincode/platforms/car"
	"github.com/abchain/fabric/core/chaincode/platforms/golang"
	"github.com/abchain/fabric/core/chaincode/platforms/java"
	"github.com/abchain/fabric/core/config"
	pb "github.com/abchain/fabric/protos"
)

// Interface for validating the specification and and writing the package for
// the given platform
type Platform interface {
	ValidateSpec(spec *pb.ChaincodeSpec) error
	WritePackage(spec *pb.ChaincodeSpec, tw *tar.Writer) error
	WriteRunTime(spec *pb.ChaincodeSpec, clispec *config.ClientSpec, tw *tar.Writer) error
	GetCodePath(string) (string, error)
}

// Find returns the platform interface for the given platform type
func Find(chaincodeType pb.ChaincodeSpec_Type) (Platform, error) {

	switch chaincodeType {
	case pb.ChaincodeSpec_GOLANG:
		return &golang.Platform{}, nil
	case pb.ChaincodeSpec_CAR:
		return &car.Platform{}, nil
	case pb.ChaincodeSpec_JAVA:
		return &java.Platform{}, nil
	default:
		return nil, fmt.Errorf("Unknown chaincodeType: %s", chaincodeType)
	}

}

func ValidateSpec(spec *pb.ChaincodeSpec) error {

	if spec == nil {
		return fmt.Errorf("Expected chaincode specification, nil received")
	}

	platform, err := Find(spec.Type)
	if err != nil {
		return fmt.Errorf("Failed to determine platform type: %s", err)
	}

	return platform.ValidateSpec(spec)

}

func WritePackage(spec *pb.ChaincodeSpec, tw *tar.Writer) error {

	platform, err := Find(spec.Type)
	if err != nil {
		return nil, err
	}

	//We respect the given chaincode name so it is easy to read
	hashName, err := generateHashcode(spec, tw, platform)
	if err != nil {
		return err
	}

	if spec.ChaincodeID.Name == "" {
		spec.ChaincodeID.Name = hashName
	}

	err = platform.WritePackage(spec, tw)
	if err != nil {
		return nil, err
	}
}

func WriteRunTime(spec *pb.ChaincodeSpec, clispec *config.ClientSpec, tw *tar.Writer) error {

}

func GetArgsAndEnv(spec *pb.ChaincodeSpec, clispec *config.ClientSpec) (args []string, envs []string, err error) {
	cID := spec.ChaincodeID
	cLang := spec.Type

	envs = []string{"CORE_CHAINCODE_ID_NAME=" + cID.Name}
	//if TLS is enabled, pass TLS material to chaincode
	if chaincodeSupport.peerTLS {
		envs = append(envs, "CORE_PEER_TLS_ENABLED=true")
		envs = append(envs, "CORE_PEER_TLS_CERT_FILE="+TLSRootCertFile)
		if chaincodeSupport.peerTLSSvrHostOrd != "" {
			envs = append(envs, "CORE_PEER_TLS_SERVERHOSTOVERRIDE="+chaincodeSupport.peerTLSSvrHostOrd)
		}
	} else {
		envs = append(envs, "CORE_PEER_TLS_ENABLED=false")
	}
	switch cLang {
	case pb.ChaincodeSpec_GOLANG, pb.ChaincodeSpec_CAR:
		//chaincode executable will be same as the name of the chaincode
		args = []string{chaincodeSupport.chaincodeInstallPath + cID.Name, fmt.Sprintf("-peer.address=%s", chaincodeSupport.peerAddress)}
		chaincodeLogger.Debugf("Executable is %s", args[0])
	case pb.ChaincodeSpec_JAVA:
		//TODO add security args
		args = strings.Split(
			fmt.Sprintf("java -jar chaincode.jar -a %s -i %s",
				chaincodeSupport.peerAddress, cID.Name),
			" ")
		if chaincodeSupport.peerTLS {
			args = append(args, " -s")
		}
		chaincodeLogger.Debugf("Executable is %s", args[0])
	default:
		return nil, nil, fmt.Errorf("Unknown chaincodeType: %s", cLang)
	}
	return args, envs, nil

}
