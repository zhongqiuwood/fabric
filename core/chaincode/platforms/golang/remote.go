package golang

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
)

func getCodeFromFS(path string) (string, error) {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		return "", fmt.Errorf("GOPATH not defined")
	}

	for _, candidate := range filepath.SplitList(gopath) {

		codegopath := filepath.Join(candidate, "src", path)
		fs, err := os.Stat(codegopath)
		if err == nil && fs.IsDir() {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("Codepath is not exist in filesystem")
}

func getCodeFromHTTP(path string) (codegopath string, err error) {
	codegopath = ""
	err = nil

	// The following could be done with os.Getenv("GOPATH") but we need to change it later so this prepares for that next step
	env := os.Environ()
	var origgopath string
	var gopathenvIndex int
	for i, v := range env {
		if strings.Index(v, "GOPATH=") == 0 {
			p := strings.SplitAfter(v, "GOPATH=")
			origgopath = p[1]
			gopathenvIndex = i
			break
		}
	}
	if origgopath == "" {
		err = fmt.Errorf("GOPATH not defined")
		return
	}
	// Only take the first element of GOPATH
	gopath := filepath.SplitList(origgopath)[0]

	// Define a new gopath in which to download the code
	newgopath := filepath.Join(gopath, "_usercode_")

	//ignore errors.. _usercode_ might exist. TempDir will catch any other errors
	os.Mkdir(newgopath, 0755)

	if codegopath, err = ioutil.TempDir(newgopath, ""); err != nil {
		err = fmt.Errorf("could not create tmp dir under %s(%s)", newgopath, err)
		return
	}

	//go paths can have multiple dirs. We create a GOPATH with two source tree's as follows
	//
	//    <temporary empty folder to download chaincode source> : <local go path with OBC source>
	//
	//This approach has several goodness:
	// . Go will pick the first path to download user code (which we will delete after processing)
	// . GO will not download OBC as it is in the second path. GO will use the local OBC for generating chaincode image
	//     . network savings
	//     . more secure
	//     . as we are not downloading OBC, private, password-protected OBC repo's become non-issue

	env[gopathenvIndex] = "GOPATH=" + codegopath + string(os.PathListSeparator) + origgopath

	// Use a 'go get' command to pull the chaincode from the given repo
	cmd := exec.Command("go", "get", path)
	cmd.Env = env
	var out bytes.Buffer
	cmd.Stdout = &out
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf //capture Stderr and print it on error
	err = cmd.Start()

	// Create a go routine that will wait for the command to finish
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-time.After(time.Duration(viper.GetInt("chaincode.deploytimeout")) * time.Millisecond):
		// If pulling repos takes too long, we should give up
		// (This can happen if a repo is private and the git clone asks for credentials)
		if err = cmd.Process.Kill(); err != nil {
			err = fmt.Errorf("failed to kill: %s", err)
		} else {
			err = errors.New("Getting chaincode took too long")
		}
	case err = <-done:
		// If we're here, the 'go get' command must have finished
		if err != nil {
			err = fmt.Errorf("'go get' failed with error: \"%s\"\n%s", err, string(errBuf.Bytes()))
		}
	}
	return
}
