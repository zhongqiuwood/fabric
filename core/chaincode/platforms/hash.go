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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/op/go-logging"
	"github.com/spf13/viper"

	cutil "github.com/abchain/fabric/core/chaincode/util"
	"github.com/abchain/fabric/core/util"
	pb "github.com/abchain/fabric/protos"
)

var logger = logging.MustGetLogger("platform/hash")

//core hash computation factored out for testing
func computeHash(contents []byte, hash []byte) []byte {
	newSlice := make([]byte, len(hash)+len(contents))

	//copy the contents
	copy(newSlice[0:len(contents)], contents[:])

	//add the previous hash
	copy(newSlice[len(contents):], hash[:])

	//compute new hash
	hash = util.ComputeCryptoHash(newSlice)

	return hash
}

//archive for each file in a directory, in archive all files are put under a magic
//directory "ccfile"
var ArchivePath = "ccfile"

func archiveFilesInDir(rootDir string, dir string, tw *tar.Writer) error {
	currentDir := filepath.Join(rootDir, dir)
	logger.Debugf("hashFiles %s", currentDir)
	//ReadDir returns sorted list of files in dir
	fis, err := ioutil.ReadDir(currentDir)
	if err != nil {
		return fmt.Errorf("ReadDir failed %s\n", err)
	}
	for _, fi := range fis {
		name := filepath.Join(dir, fi.Name())
		if fi.IsDir() {
			if err := archiveFilesInDir(rootDir, name, tw); err != nil {
				return err
			}
			continue
		}

		fqp := filepath.Join(rootDir, name)
		err = cutil.WriteFileToPackage(fqp, filepath.ToSlash(filepath.Join(ArchivePath, name)), tw)

		if err != nil {
			fmt.Printf("Error reading %s\n", err)
			return fmt.Errorf("Error adding file %s to tar: %s", fqp, err)
		}
	}
	return nil
}

func isCodeExist(tmppath string) error {
	file, err := os.Open(tmppath)
	if err != nil {
		return fmt.Errorf("Download failed %s", err)
	}

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("Could not stat file %s", err)
	}

	if !fi.IsDir() {
		return fmt.Errorf("File %s is not dir\n", file.Name())
	}

	return nil
}

//generateHashcode gets hashcode of the code under path. If path is a HTTP(s) url
//it downloads the code first to compute the hash.
//NOTE: for dev mode, user builds and runs chaincode manually. The name provided
//by the user is equivalent to the path. This method will treat the name
//as codebytes and compute the hash from it. ie, user cannot run the chaincode
//with the same (name, ctor, args)
func generateHashcode(spec *pb.ChaincodeSpec, tw *tar.Writer, pf Platform) (string, error) {
	if spec == nil {
		return "", fmt.Errorf("Cannot generate hashcode from nil spec")
	}

	chaincodeID := spec.ChaincodeID
	if chaincodeID == nil || chaincodeID.Path == "" {
		return "", fmt.Errorf("Cannot generate hashcode from empty chaincode path")
	}

	ctor := spec.CtorMsg
	if ctor == nil || len(ctor.Args) == 0 {
		return "", fmt.Errorf("Cannot generate hashcode from empty ctor")
	}

	//code root will point to the directory where the code exists
	//in the case of http it will be a temporary dir that
	//will have to be deleted
	var rootpath, codepath string

	var shouldclean bool
	var err error
	defer func() {
		if shouldclean && codepath != "" {
			os.RemoveAll(filepath.Join(rootpath, codepath))
		}
	}()

	rootpath, codepath, shouldclean, err = pf.GetCodePath(chaincodeID.Path)
	if err != nil {
		return "", fmt.Errorf("Error getting code %s", err)
	}

	tmppath := filepath.Join(rootpath, codepath)
	if err = isCodeExist(tmppath); err != nil {
		return "", fmt.Errorf("code does not exist %s", err)
	}
	ctorbytes, err := proto.Marshal(ctor)
	if err != nil {
		return "", fmt.Errorf("Error marshalling constructor: %s", err)
	}
	hash := util.GenerateHashFromSignature(codepath, ctorbytes)

	archHasher := util.DefaultCryptoHash()
	var writeInf io.Writer
	if tw == nil {
		writeInf = archHasher
	} else {
		writeInf = io.MultiWriter(archHasher, tw)
	}

	err = archiveFilesInDir(rootpath, codepath, writeInf)
	if err != nil {
		return "", fmt.Errorf("Could not archive (or hash) file for %s - %s", path, err)
	}

	err = pf.WritePackage(spec, tw)
	if err != nil {
		return nil, fmt.Errorf("Could not add additional contents from platform: %s", err)
	}

	hash = computeHash(archHasher.Sum(nil), hash)
	return hex.EncodeToString(hash[:]), nil
}
