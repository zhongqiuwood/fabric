package chaincode

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	//	"io/ioutil"

	"github.com/abchain/fabric/core/chaincode/platforms"
	"github.com/abchain/fabric/core/config"
	pb "github.com/abchain/fabric/protos"
)

// GetChaincodePackageBytes creates bytes for docker container generation using the supplied chaincode specification
func GetChaincodePackageBytes(spec *pb.ChaincodeSpec) ([]byte, error) {
	if spec == nil || spec.ChaincodeID == nil {
		return nil, fmt.Errorf("invalid chaincode spec")
	}

	inputbuf := bytes.NewBuffer(nil)
	gw := gzip.NewWriter(inputbuf)
	tw := tar.NewWriter(gw)

	_, err := platforms.WritePackage(spec, tw)
	if err != nil {
		return nil, err
	}

	//notice we do not close but just flush the tar stream
	//so we obtain an "opened" tar and it can be appended with
	//the other contents in run-time, and chaincodebytes can
	//include the most essential bytes now
	tw.Flush()
	gw.Close()

	if err != nil {
		return nil, err
	}

	chaincodePkgBytes := inputbuf.Bytes()
	chaincodeLogger.Infof("Generate chaincode package in %d bytes\n", len(chaincodePkgBytes))
	//	ioutil.WriteFile("chaincode_deployment.tar.gz", inputbuf.Bytes(), 0777)
	return chaincodePkgBytes, nil
}

type runtimeReader struct {
	*io.PipeReader
	writePacketResult chan error
}

var runtimeCancel = fmt.Errorf("User Cancel")

func (r *runtimeReader) Finish() error {
	if r == nil {
		return nil
	}
	if err := r.PipeReader.CloseWithError(runtimeCancel); err != nil {
		return err
	}
	return <-r.writePacketResult
}

// Generate a package (in Writer) for (docker) controller
func WriteRuntimePackage(cds *pb.ChaincodeDeploymentSpec, clispec *config.ClientSpec, chaincodebytes []byte) (*runtimeReader, error) {

	if chaincodebytes == nil {
		return nil, fmt.Errorf("chaincode bytes is nil")
	}

	codeTarball, err := gzip.NewReader(bytes.NewBuffer(cds.CodePackage))
	if err != nil {
		return nil, fmt.Errorf("Create unzip for ccbytes fail: %s", err)
	}

	rtR, rtW := io.Pipe()
	gzR, gzW := io.Pipe()
	rtReader := new(runtimeReader)
	rtReader.PipeReader = gzR
	rtReader.writePacketResult = make(chan error)

	totalR := io.MultiReader(codeTarball, rtR)

	//we need to start two thread for the whole writing process, and trace the first one
	go func() {
		wr := tar.NewWriter(rtW)
		err := platforms.WriteRunTime(cds.ChaincodeSpec, clispec, wr)
		if err == nil {
			wr.Close()
		}
		rtReader.writePacketResult <- err
	}()

	go func() {
		wr := gzip.NewWriter(gzW)
		sz, err := io.Copy(wr, totalR)
		chaincodeLogger.Infof("Generate runtime package for cc [%v] in %d bytes, ret [%s]", cds.ChaincodeSpec.ChaincodeID, sz, err)
		wr.Close()
	}()

	return rtReader, nil
}
