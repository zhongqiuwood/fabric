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

package car

import (
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	cutil "github.com/abchain/fabric/core/chaincode/util"
	pb "github.com/abchain/fabric/protos"
)

var remoteFileName = "package.car"

// WritePackage satisfies the platform interface for generating a docker package
// that encapsulates the environment for a CAR based chaincode
func (carPlatform *Platform) WriteDockerRunTime(spec *pb.ChaincodeSpec, tw *tar.Writer) (string, error) {

	var fileName string
	path := spec.ChaincodeID.Path
	if strings.HasPrefix(path, "http://") {
		fileName = remoteFileName
	} else {
		_, fileName = filepath.Split(path)
	}

	var buf []string

	//let the executable's name be chaincode ID's name
	buf = append(buf, cutil.GetDockerfileFromConfig("chaincode.car.Dockerfile"))
	buf = append(buf, fmt.Sprintf("COPY ccfile/%s /tmp/package.car"), fileName)
	buf = append(buf, fmt.Sprintf("RUN chaintool buildcar /tmp/package.car -o $GOPATH/bin/%s && rm /tmp/package.car", spec.ChaincodeID.Name))

	return strings.Join(buf, "\n"), nil
}

func (carPlatform *Platform) GetCodePath(path string) (rootpath string, packpath string, shouldclean bool, err error) {

	if strings.HasPrefix(path, "http://") {
		// The file is remote, so we need to download it to a temporary location first
		var tmp *os.File
		var tmpDir string
		tmpDir, err = ioutil.TempDir("", "car")
		if err != nil {
			err = fmt.Errorf("Error creating temporary directory: %s", err)
			return
		}

		tmp, err = os.Create(filepath.Join(tmpDir, remoteFileName))
		if err != nil {
			err = fmt.Errorf("Error creating downloaded file in temporary directory: %s", err)
			return
		}
		defer tmp.Close()

		var resp *http.Response
		resp, err = http.Get(path)
		if err != nil {
			err = fmt.Errorf("Error with HTTP GET: %s", err)
			return
		}
		defer resp.Body.Close()

		_, err = io.Copy(tmp, resp.Body)
		if err != nil {
			err = fmt.Errorf("Error downloading bytes: %s", err)
			return
		}

		shouldclean = true
		rootpath = tmpDir
		packpath = remoteFileName
		return
	}

	rootpath, packpath = filepath.Split(path)
	return
}
