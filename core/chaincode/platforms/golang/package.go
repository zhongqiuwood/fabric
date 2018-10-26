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

package golang

import (
	"archive/tar"
	"fmt"
	"strings"
	"time"

	cutil "github.com/abchain/fabric/core/chaincode/util"
	"github.com/abchain/fabric/core/config"
	pb "github.com/abchain/fabric/protos"
	"github.com/spf13/viper"
)

func parseCodePath(path string) (effectpath string, ishttp bool) {

	if strings.HasPrefix(path, "http://") {
		ishttp = true
		effectpath = path[7:]
	} else if strings.HasPrefix(path, "https://") {
		ishttp = true
		effectpath = path[8:]
	} else {
		effectpath = path
	}

	effectpath = strings.TrimSuffix(effectpath, "/")

	return
}

func (goPlatform *Platform) GetCodePath(path string) (rootpath string, packpath string, shouldclean bool, err error) {

	packetpath, shouldclean = parseCodePath(path)
	if shouldclean {
		rootpath, err = getCodeFromHTTP(path)
	} else {
		rootpath, err = getCodeFromFS(path)
	}

	rootpath = filepath.Join(rootpath, "src")
	return
}

func (goPlatform *Platform) WriteDockerRunTime(spec *pb.ChaincodeSpec, tw *tar.Writer) (dockertemplate string, err error) {

	codeGopath, _ := parseCodePath(spec.ChaincodeID.GetPath())

	toks := strings.Split(codeGopath, "/")
	if toks == nil || len(toks) == 0 {
		err = fmt.Errorf("cannot get path components from %s", codeGopath)
		return
	}

	chaincodeGoName := toks[len(toks)-1]
	if chaincodeGoName == "" {
		err = fmt.Errorf("could not get chaincode name from path %s", codeGopath)
		return
	}

	//write the whole GOPATH if specified
	if viper.GetBool("chaincode.golang.withGOPATH") {
		err = cutil.WriteGopathSrc(tw, codeGopath)
		if err != nil {
			err = fmt.Errorf("Error writing Chaincode package contents: %s", err)
			return
		}
	}

	//let the executable's name be chaincode ID's name
	newRunLine := fmt.Sprintf("RUN go install %s && cp src/github.com/abchain/fabric/peer/core.yaml $GOPATH/bin && mv $GOPATH/bin/%s $GOPATH/bin/%s", urlLocation, chaincodeGoName, spec.ChaincodeID.Name)

	dockertemplate = cutil.GetDockerfileFromConfig("chaincode.golang.Dockerfile")
	if dockertemplate == "" {
		err = fmt.Errorf("Invalid dockertemplate")
		return
	}
	dockertemplate = fmt.Sprintf("%s\n%s", dockertemplate, newRunLine)

	return
}

//tw is expected to have the chaincode in it from GenerateHashcode. This method
//will just package rest of the bytes
func writeChaincodePackage(spec *pb.ChaincodeSpec, clispec *config.ClientSpec, tw *tar.Writer) error {

	var urlLocation string
	if strings.HasPrefix(spec.ChaincodeID.Path, "http://") {
		urlLocation = spec.ChaincodeID.Path[7:]
	} else if strings.HasPrefix(spec.ChaincodeID.Path, "https://") {
		urlLocation = spec.ChaincodeID.Path[8:]
	} else {
		urlLocation = spec.ChaincodeID.Path
	}

	if urlLocation == "" {
		return fmt.Errorf("empty url location")
	}

	if strings.LastIndex(urlLocation, "/") == len(urlLocation)-1 {
		urlLocation = urlLocation[:len(urlLocation)-1]
	}
	toks := strings.Split(urlLocation, "/")
	if toks == nil || len(toks) == 0 {
		return fmt.Errorf("cannot get path components from %s", urlLocation)
	}

	chaincodeGoName := toks[len(toks)-1]
	if chaincodeGoName == "" {
		return fmt.Errorf("could not get chaincode name from path %s", urlLocation)
	}

	//NOTE-this could have been abstracted away so we could use it for all platforms in a common manner
	//However, it would still be docker specific. Hence any such abstraction has to be done in a manner that
	//is not just language dependent but also container depenedent. So lets make this change per platform for now
	//in the interest of avoiding over-engineering without proper abstraction
	if viper.GetBool("peer.tls.enabled") {
		newRunLine = fmt.Sprintf("%s\nCOPY certs/%s %s", newRunLine)
	}

	dockerFileContents := fmt.Sprintf("%s\n%s", cutil.GetDockerfileFromConfig("chaincode.golang.Dockerfile"), newRunLine)
	dockerFileSize := int64(len([]byte(dockerFileContents)))

	//Make headers identical by using zero time
	var zeroTime time.Time
	tw.WriteHeader(&tar.Header{Name: "Dockerfile", Size: dockerFileSize, ModTime: zeroTime, AccessTime: zeroTime, ChangeTime: zeroTime})
	tw.Write([]byte(dockerFileContents))

	//Do we really need to write the whole GOPATH besides target code? at least we should be able to control it by a switch
	if viper.GetBool("chaincode.golang.withGOPATH") {
		err := cutil.WriteGopathSrc(tw, urlLocation)
		if err != nil {
			return fmt.Errorf("Error writing Chaincode package contents: %s", err)
		}
	}
	return nil
}
