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

package chaincode

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"
	"golang.org/x/net/context"

	"strings"

	"github.com/abchain/fabric/core/container"
	"github.com/abchain/fabric/core/container/ccintf"
	"github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/util"
	pb "github.com/abchain/fabric/protos"
)

// ChainName is the name of the chain to which this chaincode support belongs to.
type ChainName string

const (
	// DefaultChain is the name of the default chain.
	DefaultChain ChainName = "default"
	// DevModeUserRunsChaincode property allows user to run chaincode in development environment
	DevModeUserRunsChaincode       string = "dev"
	NetworkModeChaincode           string = "net"
	chaincodeStartupTimeoutDefault int    = 5000
	chaincodeExecTimeoutDefault    int    = 30000
	chaincodeInstallPathDefault    string = "/opt/gopath/bin/"
	peerAddressDefault             string = "0.0.0.0:7051"
)

// chains is a map between different blockchains and their ChaincodeSupport.
//this needs to be a first class, top-level object... for now, lets just have a placeholder
var chains map[ChainName]*ChaincodeSupport

func init() {
	chains = make(map[ChainName]*ChaincodeSupport)
}

//chaincode runtime environment encapsulates handler and container environment
//This is where the VM that's running the chaincode would hook in
type chaincodeRTEnv struct {
	handler      *Handler
	launchNotify chan error
	waitNotify   chan error
}

// runningChaincodes contains maps of chaincodeIDs to their chaincodeRTEs
type runningChaincodes struct {
	sync.RWMutex
	// chaincode environment for each chaincode
	chaincodeMap map[string]*chaincodeRTEnv
}

// GetChain returns the chaincode support for a given chain
func GetChain(name ChainName) *ChaincodeSupport {
	return chains[name]
}

//call this under lock
func (chaincodeSupport *ChaincodeSupport) preLaunchSetup(chaincode string) *chaincodeRTEnv {
	//register placeholder Handler.
	ret := &chaincodeRTEnv{
		launchNotify: make(chan error, 1),
		waitNotify:   make(chan error, 1),
	}
	chaincodeSupport.runningChaincodes.chaincodeMap[chaincode] = ret
	return ret
}

const (
	deployTxKey = "__YAFABRIC_deployTx"
)

func (chaincodeSupport *ChaincodeSupport) FinalDeploy(ctx context.Context, txsuccess bool, cds *pb.ChaincodeDeploymentSpec, t *pb.Transaction, ledger *ledger.Ledger) error {

	var err error
	if !txsuccess {
		err = fmt.Errorf("Deploy tx did not get success responds")
	} else {
		err = ledger.SetState(cds.ChaincodeSpec.ChaincodeID.Name, deployTxKey, []byte(t.GetTxid()))
	}

	if err != nil {
		chaincodeLogger.Infof("stopping due to error while final deploy tx: %s", err)
		errIgnore := chaincodeSupport.Stop(ctx, cds)
		if errIgnore != nil {
			chaincodeLogger.Debugf("error on stop %s(%s)", errIgnore, err)
		}
	}
	return err
}

func (chaincodeSupport *ChaincodeSupport) extractDeployTx(chaincode string, ledger *ledger.Ledger) (*pb.Transaction, error) {
	if chaincodeSupport.userRunsCC {
		chaincodeLogger.Error("You are attempting to perform an action other than Deploy on Chaincode that is not ready and you are in developer mode. Did you forget to Deploy your chaincode?")
	}

	var depTxId []byte
	var depTx *pb.Transaction
	var ledgerErr error
	depTxId, ledgerErr = ledger.GetState(chaincode, deployTxKey, true)
	if ledgerErr != nil {
		return nil, fmt.Errorf("Failed to get deploy key in ledger (%s)", ledgerErr)
	} else if depTxId == nil {
		//to compatible old code
		chaincodeLogger.Warningf("Deploy tx for chaincoide %s not found, try chaincode name as tx id", chaincode)
		depTx, ledgerErr = ledger.GetTransactionByID(chaincode)
	} else {
		depTx, ledgerErr = ledger.GetTransactionByID(string(depTxId))
	}

	//hopefully we are restarting from existing image and the deployed transaction exists
	if ledgerErr != nil {
		return nil, fmt.Errorf("Could not get deployment transaction for %s - %s", chaincode, ledgerErr)
	}
	if depTx == nil {
		return nil, fmt.Errorf("deployment transaction does not exist for %s", chaincode)
	}
	if nil != chaincodeSupport.secHelper {
		var err error
		depTx, err = chaincodeSupport.secHelper.TransactionPreExecution(depTx)
		// Note that t is now decrypted and is a deep clone of the original input t
		if nil != err {
			return nil, fmt.Errorf("failed tx preexecution%s - %s", chaincode, err)
		}
	}

	return depTx, nil
}

//call this under lock
func (chaincodeSupport *ChaincodeSupport) chaincodeHasBeenLaunched(chaincode string) (*chaincodeRTEnv, bool) {
	chrte, hasbeenlaunched := chaincodeSupport.runningChaincodes.chaincodeMap[chaincode]
	return chrte, hasbeenlaunched
}

// NewChaincodeSupport creates a new ChaincodeSupport instance
func NewChaincodeSupport(chainname ChainName, getPeerEndpoint func() (*pb.PeerEndpoint, error), userrunsCC bool, secHelper crypto.Peer) *ChaincodeSupport {
	pnid := viper.GetString("peer.networkId")
	pid := viper.GetString("peer.id")

	s := &ChaincodeSupport{name: chainname,
		runningChaincodes: &runningChaincodes{
			chaincodeMap: make(map[string]*chaincodeRTEnv),
		},
		secHelper:     secHelper,
		peerNetworkID: pnid,
		peerID:        pid}

	//initialize global chain
	chains[chainname] = s

	s.peerAddress = viper.GetString("chaincode.peer.address")
	if s.peerAddress == "" {
		peerEndpoint, err := getPeerEndpoint()
		if err != nil {
			chaincodeLogger.Errorf("Error getting PeerEndpoint, using peer.address: %s", err)
			s.peerAddress = viper.GetString("peer.address")
		} else {
			s.peerAddress = peerEndpoint.Address
		}
		chaincodeLogger.Infof("Chaincode support using peerAddress: %s\n", s.peerAddress)
	}

	//peerAddress = viper.GetString("peer.address")
	if s.peerAddress == "" {
		s.peerAddress = peerAddressDefault
	}

	s.userRunsCC = userrunsCC

	//get chaincode startup timeout
	tOut, err := strconv.Atoi(viper.GetString("chaincode.startuptimeout"))
	if err != nil { //what went wrong ?
		tOut = chaincodeStartupTimeoutDefault
		chaincodeLogger.Infof("could not retrive startup timeout var...setting to %d secs\n", tOut/1000)
	}

	s.ccStartupTimeout = time.Duration(tOut) * time.Millisecond

	//get chaincode exec timeout
	tOut, err = strconv.Atoi(viper.GetString("chaincode.exectimeout"))
	if err != nil { //what went wrong ?
		tOut = chaincodeExecTimeoutDefault
		chaincodeLogger.Infof("could not retrive exec timeout var...setting to %d secs\n", tOut/1000)
	}

	s.ccExecTimeout = time.Duration(tOut) * time.Millisecond

	//TODO I'm not sure if this needs to be on a per chain basis... too lowel and just needs to be a global default ?
	s.chaincodeInstallPath = viper.GetString("chaincode.installpath")
	if s.chaincodeInstallPath == "" {
		s.chaincodeInstallPath = chaincodeInstallPathDefault
	}

	s.peerTLS = viper.GetBool("peer.tls.enabled")
	if s.peerTLS {
		s.peerTLSCertFile = util.CanonicalizeFilePath(viper.GetString("peer.tls.rootcert.file"))
		//		s.peerTLSKeyFile = viper.GetString("peer.tls.key.file")
		s.peerTLSSvrHostOrd = viper.GetString("peer.tls.serverhostoverride")
	}

	kadef := 0
	if ka := viper.GetString("chaincode.keepalive"); ka == "" {
		s.keepalive = time.Duration(kadef) * time.Second
	} else {
		t, terr := strconv.Atoi(ka)
		if terr != nil {
			chaincodeLogger.Errorf("Invalid keepalive value %s (%s) defaulting to %d", ka, terr, kadef)
			t = kadef
		} else if t <= 0 {
			chaincodeLogger.Debugf("Turn off keepalive(value %s)", ka)
			t = kadef
		}
		s.keepalive = time.Duration(t) * time.Second
	}

	return s
}

// // ChaincodeStream standard stream for ChaincodeMessage type.
// type ChaincodeStream interface {
// 	Send(*pb.ChaincodeMessage) error
// 	Recv() (*pb.ChaincodeMessage, error)
// }

// ChaincodeSupport responsible for providing interfacing with chaincodes from the Peer.
type ChaincodeSupport struct {
	name                 ChainName
	runningChaincodes    *runningChaincodes
	peerAddress          string
	ccStartupTimeout     time.Duration
	ccExecTimeout        time.Duration
	chaincodeInstallPath string
	userRunsCC           bool
	secHelper            crypto.Peer
	peerNetworkID        string
	peerID               string
	peerTLS              bool
	peerTLSCertFile      string
	//	peerTLSKeyFile       string
	peerTLSSvrHostOrd string
	keepalive         time.Duration
}

// DuplicateChaincodeHandlerError returned if attempt to register same chaincodeID while a stream already exists.
type DuplicateChaincodeHandlerError struct {
	ChaincodeID *pb.ChaincodeID
}

func (d *DuplicateChaincodeHandlerError) Error() string {
	return fmt.Sprintf("Duplicate chaincodeID error: %s", d.ChaincodeID)
}

func newDuplicateChaincodeHandlerError(chaincodeHandler *Handler) error {
	return &DuplicateChaincodeHandlerError{ChaincodeID: chaincodeHandler.ChaincodeID}
}

func (chaincodeSupport *ChaincodeSupport) UserRunsCC() bool {
	return chaincodeSupport.userRunsCC
}

func (chaincodeSupport *ChaincodeSupport) registerHandler(cID *pb.ChaincodeID, stream ccintf.ChaincodeStream) (*Handler, *workingStream, error) {

	key := cID.Name
	chaincodeSupport.runningChaincodes.Lock()
	defer chaincodeSupport.runningChaincodes.Unlock()

	chrte, ok := chaincodeSupport.chaincodeHasBeenLaunched(key)
	if ok && chrte.handler != nil {
		//add more stream into exist handler
		ws, err := chrte.handler.addNewStream(stream)
		if err != nil {
			return nil, nil, err
		}
		return chrte.handler, ws, nil
	}

	//handler is just lauched
	handler := newChaincodeSupportHandler(chaincodeSupport)
	handler.ChaincodeID = cID
	var err error

	//a placeholder, unregistered handler will be setup by query or transaction processing that comes
	//through via consensus. In this case we swap the handler and give it the notify channel
	if chrte != nil {
		chrte.handler = handler
		defer func(chrte *chaincodeRTEnv) { chrte.launchNotify <- err }(chrte)
	} else {
		//should not allow register in "NET" mode
		if !chaincodeSupport.userRunsCC {
			return nil, nil, fmt.Errorf("Can't register chaincode without invoking deploy tx")
		}
		chaincodeSupport.runningChaincodes.chaincodeMap[key] = &chaincodeRTEnv{handler: handler}
	}

	var ws *workingStream
	ws, err = handler.addNewStream(stream)
	if err != nil {
		return nil, nil, err
	}
	// ----------- YA-fabric 0.9 note -------------
	//the protocol (cc shim require an ACT from server) should be malformed
	//for the handshaking of connection can be responsed by grpc itself
	//we will eliminate this response in the later version and the code
	//following is just for compatible

	chaincodeLogger.Debugf("cc [%s] is lauching, sending back %s", key, pb.ChaincodeMessage_REGISTERED)
	err = ws.Send(&pb.ChaincodeMessage{Type: pb.ChaincodeMessage_REGISTERED})
	if err != nil {
		return nil, nil, err
	}
	// --------------------------------------------

	chaincodeLogger.Debugf("registered handler complete for chaincode %s", key)

	return handler, ws, nil
}

func (chaincodeSupport *ChaincodeSupport) deregisterHandler(chaincodehandler *Handler) {

	key := chaincodehandler.ChaincodeID.Name
	chaincodeSupport.runningChaincodes.Lock()
	defer chaincodeSupport.runningChaincodes.Unlock()

	delete(chaincodeSupport.runningChaincodes.chaincodeMap, key)
	chaincodeLogger.Debugf("Deregistered handler with key: %s", key)

}

//get args and env given chaincodeID
func (chaincodeSupport *ChaincodeSupport) getArgsAndEnv(cID *pb.ChaincodeID, cLang pb.ChaincodeSpec_Type) (args []string, envs []string, err error) {
	envs = []string{"CORE_CHAINCODE_ID_NAME=" + cID.Name}
	//if TLS is enabled, pass TLS material to chaincode
	if chaincodeSupport.peerTLS {
		envs = append(envs, "CORE_PEER_TLS_ENABLED=true")
		envs = append(envs, "CORE_PEER_TLS_CERT_FILE="+chaincodeSupport.peerTLSCertFile)
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

// launchAndWaitForRegister will launch container if not already running. Use the targz to create the image if not found
func (chaincodeSupport *ChaincodeSupport) launchAndWaitForRegister(ctxt context.Context, cds *pb.ChaincodeDeploymentSpec, cID *pb.ChaincodeID, cLang pb.ChaincodeSpec_Type, targz io.Reader) error {
	chaincode := cID.Name
	if chaincode == "" {
		return fmt.Errorf("chaincode name not set")
	}

	//launch the chaincode
	args, env, err := chaincodeSupport.getArgsAndEnv(cID, cLang)
	if err != nil {
		return err
	}

	chaincodeLogger.Debugf("start container: %s(networkid:%s,peerid:%s)", chaincode, chaincodeSupport.peerNetworkID, chaincodeSupport.peerID)

	vmtype, _ := chaincodeSupport.getVMType(cds)

	sir := container.StartImageReq{CCID: ccintf.CCID{ChaincodeSpec: cds.ChaincodeSpec, NetworkID: chaincodeSupport.peerNetworkID, PeerID: chaincodeSupport.peerID}, Reader: targz, Args: args, Env: env}

	ipcCtxt := context.WithValue(ctxt, ccintf.GetCCHandlerKey(), chaincodeSupport)

	resp, err := container.VMCProcess(ipcCtxt, vmtype, sir)
	if err != nil || (resp != nil && resp.(container.VMCResp).Err != nil) {
		if err == nil {
			err = resp.(container.VMCResp).Err
		}
		err = fmt.Errorf("Error starting container: %s", err)
		return err
	}

	return nil
}

func (chaincodeSupport *ChaincodeSupport) finishLaunching(chaincode string, notify error) {

	//we need a "lasttime checking", so if the launching chaincode is not registered,
	//we just erase it and notify a termination
	chaincodeSupport.runningChaincodes.Lock()
	defer chaincodeSupport.runningChaincodes.Unlock()
	if rte, ok := chaincodeSupport.chaincodeHasBeenLaunched(chaincode); !ok {
		//nothing to do
		chaincodeLogger.Warningf("trying to terminate the launching for unexist chaincode %s", chaincode)
		return
		// } else if rte.handler != nil {
		// 	//chaincode is registered ...
		// 	return false
		// } else {
	} else {
		if rte.waitNotify == nil {
			panic("another routine has make this calling, we have wrong code?")
		}
		rte.waitNotify <- notify
		rte.waitNotify = nil //mark launching is over
	}

	//if we get err notify, we must clear the rte even it has created a handler
	if notify != nil {
		delete(chaincodeSupport.runningChaincodes.chaincodeMap, chaincode)
	}
}

//Stop stops a chaincode if running
func (chaincodeSupport *ChaincodeSupport) Stop(context context.Context, cds *pb.ChaincodeDeploymentSpec) error {
	chaincode := cds.ChaincodeSpec.ChaincodeID.Name
	if chaincode == "" {
		return fmt.Errorf("chaincode name not set")
	}

	//stop the chaincode
	sir := container.StopImageReq{CCID: ccintf.CCID{ChaincodeSpec: cds.ChaincodeSpec, NetworkID: chaincodeSupport.peerNetworkID, PeerID: chaincodeSupport.peerID}, Timeout: 0}

	vmtype, _ := chaincodeSupport.getVMType(cds)

	_, err := container.VMCProcess(context, vmtype, sir)
	if err != nil {
		err = fmt.Errorf("Error stopping container: %s", err)
		//but proceed to cleanup
	}

	return err
}

// Launch will launch the chaincode if not running (if running return nil) and will wait for handler of the chaincode to get into FSM ready state.
func (chaincodeSupport *ChaincodeSupport) Launch(ctx context.Context, ledger *ledger.Ledger, cID *pb.ChaincodeID, cds *pb.ChaincodeDeploymentSpec, t *pb.Transaction) (error, *chaincodeRTEnv) {

	chaincode := cID.Name

	chaincodeSupport.runningChaincodes.Lock()

	//the first tx touch the corresponding run-time object is response for the actually
	//launching and other tx just wait
	if chrte, ok := chaincodeSupport.chaincodeHasBeenLaunched(chaincode); ok {
		if chrte.waitNotify == nil {
			chaincodeLogger.Debugf("chaincode is running(no need to launch) : %s", chaincode)
			chaincodeSupport.runningChaincodes.Unlock()
			return nil, chrte
		}
		//all of us must wait here till the cc is really launched (or failed...)
		chaincodeLogger.Debug("chainicode not in READY state...waiting")
		//now we "chain" the notify so mutiple waitings
		notifyChain := make(chan error, 1)
		notfy := chrte.waitNotify
		chrte.waitNotify = notifyChain
		chaincodeSupport.runningChaincodes.Unlock()

		//the waiting route get notify from the routine
		//which actually responds for the lauching
		ret := <-notfy
		//and just chain it
		notifyChain <- ret
		chaincodeLogger.Debugf("wait chaincode %s for lauching: %s", chaincode, ret)
		return ret, chrte
	}

	//the first one create rte and start its adventure ...
	chrte := chaincodeSupport.preLaunchSetup(chaincode)
	chaincodeSupport.runningChaincodes.Unlock()

	var err error
	var depTx *pb.Transaction
	defer func() { chaincodeSupport.finishLaunching(chaincode, err) }()

	if t.Type != pb.Transaction_CHAINCODE_DEPLOY {
		//so the cds must be nil
		if cds != nil {
			panic("something wrong in our code?")
		}

		depTx, err = chaincodeSupport.extractDeployTx(chaincode, ledger)
		if err != nil {
			return err, chrte
		}

		cds = new(pb.ChaincodeDeploymentSpec)
		//Get lang from original deployment
		err := proto.Unmarshal(depTx.Payload, cds)
		if err != nil {
			return fmt.Errorf("failed to unmarshal deployment transactions for %s - %s", chaincode, err), chrte
		}
	}

	//from here on : if we launch the container and get an error, we need to stop the container
	wctx, wctxend := context.WithTimeout(ctx, chaincodeSupport.ccStartupTimeout)
	defer wctxend()
	cLang := cds.ChaincodeSpec.Type
	//launch container if it is a System container or not in dev mode
	if !chaincodeSupport.userRunsCC || cds.ExecEnv == pb.ChaincodeDeploymentSpec_SYSTEM {
		var targz io.Reader = bytes.NewBuffer(cds.CodePackage)
		err = chaincodeSupport.launchAndWaitForRegister(wctx, cds, cID, cLang, targz)
		if err != nil {
			chaincodeLogger.Errorf("launchAndWaitForRegister failed %s", err)
			return err, chrte
		}

		defer func() {
			if err != nil {
				chaincodeLogger.Infof("stopping due to error while launching %s", err)
				errIgnore := chaincodeSupport.Stop(ctx, cds)
				if errIgnore != nil {
					chaincodeLogger.Debugf("error on stop %s(%s)", errIgnore, err)
				}
			}
		}()
		//wait for REGISTER state
		select {
		case err = <-chrte.launchNotify:
		case <-wctx.Done():
			err = fmt.Errorf("Timeout expired while starting chaincode %s(networkid:%s,peerid:%s)", chaincode, chaincodeSupport.peerNetworkID, chaincodeSupport.peerID)
		}
		if err != nil {
			return err, chrte
		}
	}

	//send ready (if not deploy) for ready state
	if chrte.handler == nil {
		err = fmt.Errorf("handler is not available though lauching [%s(networkid:%s,peerid:%s)] notify ok", chaincode, chaincodeSupport.peerNetworkID, chaincodeSupport.peerID)
		return err, chrte
	}
	err = chrte.handler.readyChaincode(t, depTx)
	if err != nil {
		return err, chrte
	}
	chaincodeLogger.Debug("LaunchChaincode complete")
	return nil, chrte
}

// getSecHelper returns the security help set from NewChaincodeSupport
func (chaincodeSupport *ChaincodeSupport) getSecHelper() crypto.Peer {
	return chaincodeSupport.secHelper
}

//getVMType - just returns a string for now. Another possibility is to use a factory method to
//return a VM executor
func (chaincodeSupport *ChaincodeSupport) getVMType(cds *pb.ChaincodeDeploymentSpec) (string, error) {
	if cds.ExecEnv == pb.ChaincodeDeploymentSpec_SYSTEM {
		return container.SYSTEM, nil
	}
	return container.DOCKER, nil
}

// Prepare parse the transaction and obtain require informations
func (chaincodeSupport *ChaincodeSupport) Preapre(t *pb.Transaction) (*pb.ChaincodeID, *pb.ChaincodeInput, *pb.ChaincodeDeploymentSpec, error) {

	if t.Type == pb.Transaction_CHAINCODE_DEPLOY {
		cds := &pb.ChaincodeDeploymentSpec{}
		err := proto.Unmarshal(t.Payload, cds)
		if err != nil {
			return nil, nil, nil, err
		}
		return cds.ChaincodeSpec.ChaincodeID, cds.ChaincodeSpec.CtorMsg, cds, nil
	} else if t.Type == pb.Transaction_CHAINCODE_INVOKE || t.Type == pb.Transaction_CHAINCODE_QUERY {
		ci := &pb.ChaincodeInvocationSpec{}
		err := proto.Unmarshal(t.Payload, ci)
		if err != nil {
			return nil, nil, nil, err
		}
		return ci.ChaincodeSpec.ChaincodeID, ci.ChaincodeSpec.CtorMsg, nil, nil
	} else {
		return nil, nil, nil, fmt.Errorf("invalid transaction type: %d", t.Type)
	}

}

// Deploy deploys the chaincode if not in development mode where user is running the chaincode.
func (chaincodeSupport *ChaincodeSupport) Deploy(context context.Context, cds *pb.ChaincodeDeploymentSpec) error {

	cID := cds.ChaincodeSpec.ChaincodeID
	cLang := cds.ChaincodeSpec.Type
	chaincode := cID.Name

	if chaincodeSupport.userRunsCC {
		chaincodeLogger.Debug("user runs chaincode, not deploying chaincode")
		return nil
	}

	chaincodeSupport.runningChaincodes.Lock()
	//if its in the map, there must be a connected stream...and we are trying to build the code ?!
	if _, ok := chaincodeSupport.chaincodeHasBeenLaunched(chaincode); ok {
		chaincodeLogger.Debugf("deploy ?!! there's a chaincode with that name running: %s", chaincode)
		chaincodeSupport.runningChaincodes.Unlock()
		return fmt.Errorf("deploy attempted but a chaincode with same name running %s", chaincode)
	}
	chaincodeSupport.runningChaincodes.Unlock()

	args, envs, err := chaincodeSupport.getArgsAndEnv(cID, cLang)
	if err != nil {
		return fmt.Errorf("error getting args for chaincode %s", err)
	}

	var targz io.Reader = bytes.NewBuffer(cds.CodePackage)
	cir := &container.CreateImageReq{CCID: ccintf.CCID{ChaincodeSpec: cds.ChaincodeSpec, NetworkID: chaincodeSupport.peerNetworkID, PeerID: chaincodeSupport.peerID}, Args: args, Reader: targz, Env: envs}

	vmtype, _ := chaincodeSupport.getVMType(cds)

	chaincodeLogger.Debugf("deploying chaincode %s(networkid:%s,peerid:%s)", chaincode, chaincodeSupport.peerNetworkID, chaincodeSupport.peerID)

	//create image and create container
	_, err = container.VMCProcess(context, vmtype, cir)
	if err != nil {
		err = fmt.Errorf("Error starting container: %s", err)
	}

	return err
}

// Register the bidi stream entry point called by chaincode to register with the Peer.
// registerHandler implements ccintf.HandleChaincodeStream for all vms to call with appropriate stream
// It call the main loop in handler for handling the associated Chaincode stream
func (chaincodeSupport *ChaincodeSupport) Register(stream pb.ChaincodeSupport_RegisterServer) error {
	return chaincodeSupport.HandleChaincodeStream(stream.Context(), stream)
}

func (chaincodeSupport *ChaincodeSupport) HandleChaincodeStream(ctx context.Context, stream ccintf.ChaincodeStream) error {
	msg, err := stream.Recv()
	if msg.Type != pb.ChaincodeMessage_REGISTER {
		return fmt.Errorf("Recv unexpected message type [%s] at the beginning of ccstream", msg.ChaincodeEvent)
	}
	chaincodeID := &pb.ChaincodeID{}
	err = proto.Unmarshal(msg.Payload, chaincodeID)
	if err != nil {
		return fmt.Errorf("Error in received [%s], could NOT unmarshal registration info: %s", pb.ChaincodeMessage_REGISTER, err)
	}

	handler, ws, err := chaincodeSupport.registerHandler(chaincodeID, stream)
	if err != nil {
		return fmt.Errorf("Register handler fail: %s", err)
	}

	deadline, ok := ctx.Deadline()
	chaincodeLogger.Debugf("Current context deadline = %s, ok = %v", deadline, ok)
	return ws.processStream(handler)
}

// Execute executes a transaction and waits for it to complete until a timeout value.
func (chaincodeSupport *ChaincodeSupport) Execute(ctxt context.Context, chrte *chaincodeRTEnv, cMsg *pb.ChaincodeInput, tx *pb.Transaction) (*pb.ChaincodeMessage, error) {

	wctx, cf := context.WithTimeout(ctxt, chaincodeSupport.ccExecTimeout)
	defer cf()
	return chrte.handler.executeMessage(wctx, cMsg, tx)
}
