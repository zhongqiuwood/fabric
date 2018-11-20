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

	hashstr, err := platforms.WritePackage(spec, gw)
	if err != nil {
		return nil, err
	}

	gw.Close()

	//if spec include no chaincode name, use hash (but shorter than 0.6)
	if spec.ChaincodeID.GetName() == "" {
		if len(hashstr) > 24 {
			hashstr = hashstr[:24]
		}
		spec.ChaincodeID.Name = hashstr
		chaincodeLogger.Infof("chaincode now have its name as %s", spec.ChaincodeID.Name)
	}

	if err != nil {
		return nil, err
	}

	chaincodePkgBytes := inputbuf.Bytes()
	chaincodeLogger.Infof("Generate chaincode package in %d byte", len(chaincodePkgBytes))
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

	select {
	case err := <-r.writePacketResult:
		return err
	default:
		r.CloseWithError(runtimeCancel)
		<-r.writePacketResult
		return nil
	}

}

// Generate a package (in Writer) for (docker) controller
func WriteRuntimePackage(cds *pb.ChaincodeDeploymentSpec, clispec *config.ClientSpec) (*runtimeReader, error) {

	if cds.CodePackage == nil {
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
		chaincodeLogger.Debugf("Generate runtime part for cc [%v], ret [%v]", cds.ChaincodeSpec.ChaincodeID, err)
		if err == nil {
			wr.Close()
			rtW.Close()
		} else {
			rtW.CloseWithError(err)
		}
		rtReader.writePacketResult <- err
	}()

	go func() {
		wr := gzip.NewWriter(gzW)
		sz, err := io.Copy(wr, totalR)
		chaincodeLogger.Debugf("Generate runtime package for cc [%v] in %d bytes, ret [%v]", cds.ChaincodeSpec.ChaincodeID, sz, err)
		if err != nil {
			gzW.CloseWithError(err)
			//also trigger a cancel from reader side ...
			rtR.CloseWithError(err)
		} else {
			wr.Close()
			gzW.Close()
		}

	}()

	return rtReader, nil
}
