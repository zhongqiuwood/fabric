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
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"
	"golang.org/x/net/context"

	"github.com/abchain/fabric/core/chaincode/container"
	"github.com/abchain/fabric/core/chaincode/container/ccintf"
	"github.com/abchain/fabric/core/chaincode/platforms"
	"github.com/abchain/fabric/core/config"
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/ledger"
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
	chaincodeDeployTimeoutDefault  int    = 30000
	chaincodeExecTimeoutDefault    int    = 30000
	peerAddressDefault             string = "0.0.0.0:7051"

	TLSRootCertFile = "chaincodeCA.pem"
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
	launchResult error
	waitCtx      context.Context
}

// runningChaincodes contains maps of chaincodeIDs to their chaincodeRTEs
type runningChaincodes struct {
	sync.RWMutex
	// chaincode environment for each chaincode
	chaincodeMap map[string]map[*ledger.Ledger]*chaincodeRTEnv

	// only for usercc, cache pre-connected chaincode and use them for launching
	freeChainCodes map[string][]*chaincodeRTEnv
}

// GetChain returns the chaincode support for a given chain
func GetChain(name ChainName) *ChaincodeSupport {
	return chains[name]
}

func GetDefaultChain() *ChaincodeSupport {
	return chains[DefaultChain]
}

//call this under lock
func (chaincodeSupport *ChaincodeSupport) preLaunchSetup(l *ledger.Ledger, chaincode string) *chaincodeRTEnv {
	//register placeholder Handler.
	ret := &chaincodeRTEnv{
		launchNotify: make(chan error, 1),
	}

	if _, ok := chaincodeSupport.runningChaincodes.chaincodeMap[chaincode]; !ok {
		chaincodeSupport.runningChaincodes.chaincodeMap[chaincode] = make(map[*ledger.Ledger]*chaincodeRTEnv)
	}
	chaincodeSupport.runningChaincodes.chaincodeMap[chaincode][l] = ret
	return ret
}

const (
	codepackCCName = ".repoCC"
	deployTxKey    = "__YAFABRIC_deployTx"
)

//used to filter some chaincode name from external accessing
var ReservedCCName = map[string]bool{codepackCCName: true}

func (chaincodeSupport *ChaincodeSupport) FinalDeploy(ctx context.Context, txsuccess bool, cds *pb.ChaincodeDeploymentSpec, t *pb.Transaction) {

	if txsuccess {
		return
	}

	chaincodeLogger.Infof("stopping due to error while final deploy tx")
	errIgnore := chaincodeSupport.Stop(ctx, cds)
	if errIgnore != nil {
		chaincodeLogger.Debugf("error on stop %s", errIgnore)
	}
}

func checkDeployTx(chaincode string, ledger *ledger.Ledger) ([]byte, error) {
	return ledger.GetState(chaincode, deployTxKey, true)
}

func (chaincodeSupport *ChaincodeSupport) extractDeployData(chaincode string, ledger *ledger.Ledger) (*pb.ChaincodeDeploymentSpec, *pb.ChaincodeSecurityContext, error) {
	if chaincodeSupport.userRunsCC {
		chaincodeLogger.Error("You are attempting to perform an action other than Deploy on Chaincode that is not ready and you are in developer mode. Did you forget to Deploy your chaincode?")
	}

	var depTx *pb.Transaction
	var ledgerErr error
	depTx, ledgerErr = checkDeployTx(chaincode, ledger)
	if ledgerErr != nil {
		return nil, fmt.Errorf("Failed to get deploy key in ledger (%s)", ledgerErr)
	} else if depTx == nil {
		//to compatible old code
		chaincodeLogger.Warningf("Deploy tx for chaincoide %s not found, try chaincode name as tx id", chaincode)
		depTx, ledgerErr = ledger.GetTransactionByID(chaincode)
	}

	//hopefully we are restarting from existing image and the deployed transaction exists
	if ledgerErr != nil {
		return nil, fmt.Errorf("Could not get deployment transaction for %s - %s", chaincode, ledgerErr)
	}
	if depTx == nil {
		return nil, fmt.Errorf("deployment transaction does not exist for %s", chaincode)
	}
	if nil != chaincodeSupport.txHandler {
		var err error
		depTx, err = chaincodeSupport.txHandler.TransactionPreExecution(depTx)
		// Note that t is now decrypted and is a deep clone of the original input t
		if nil != err {
			return nil, fmt.Errorf("failed tx preexecution%s - %s", chaincode, err)
		}
	}

	return depTx, nil
}

//call this under lock
func (chaincodeSupport *ChaincodeSupport) chaincodeHasBeenLaunched(l *ledger.Ledger, chaincode string) (chrte *chaincodeRTEnv, hasbeenlaunched bool) {

	if chaincodeSupport.userRunsCC {
		//check cached, prelaunched sessions
		defer func() {
			if !hasbeenlaunched {
				for _, chrte = range chaincodeSupport.runningChaincodes.freeChainCodes[chaincode] {
					//take one cached session out
					chrte.handler.Ledger = l
					if _, ok := chaincodeSupport.runningChaincodes.chaincodeMap[chaincode]; !ok {
						chaincodeSupport.runningChaincodes.chaincodeMap[chaincode] = make(map[*ledger.Ledger]*chaincodeRTEnv)
					}
					chaincodeSupport.runningChaincodes.chaincodeMap[chaincode][l] = chrte
					fcc := chaincodeSupport.runningChaincodes.freeChainCodes[chaincode]
					chaincodeSupport.runningChaincodes.freeChainCodes[chaincode] = fcc[1:]
					hasbeenlaunched = true
				}
			}
		}()
	}

	if ml, ledgerExist := chaincodeSupport.runningChaincodes.chaincodeMap[chaincode]; !ledgerExist {
		return
	} else {
		chrte, hasbeenlaunched = ml[l]
		return
	}
}

// NewChaincodeSupport creates a new ChaincodeSupport instance
func NewChaincodeSupport(chainname ChainName, nodeName string, srvSpec *config.ServerSpec, userrunsCC bool, txH cred.TxConfidentialityHandler) *ChaincodeSupport {

	s := &ChaincodeSupport{name: chainname,
		runningChaincodes: &runningChaincodes{
			chaincodeMap: make(map[string]map[*ledger.Ledger]*chaincodeRTEnv),
		},
		txHandler:   txH,
		userRunsCC:  userrunsCC,
		clientGuide: srvSpec.GetClient(),
		nodeID:      nodeName}

	//currently chaincode support only accept a file scheme for tls
	if s.clientGuide.EnableTLS && s.clientGuide.TLSRootCertFile == "" {
		chaincodeLogger.Fatalf("could not use tls scheme except for file: %v", s.clientGuide)
		return nil
	}

	//initialize global chain
	chains[chainname] = s
	chaincodeLogger.Infof("Chaincode support %s using peerAddress: %s\n", chainname, s.clientGuide.Address)

	//get chaincode startup timeout
	tOut, err := strconv.Atoi(viper.GetString("chaincode.startuptimeout"))
	if err != nil {
		tOut = chaincodeStartupTimeoutDefault
		chaincodeLogger.Infof("could not retrive startup timeout var...setting to %d secs\n", tOut/1000)
	}

	s.ccStartupTimeout = time.Duration(tOut) * time.Millisecond

	//get chaincode deploy timeout
	tOut, err = strconv.Atoi(viper.GetString("chaincode.deploytimeout"))
	if err != nil {
		tOut = chaincodeDeployTimeoutDefault
		chaincodeLogger.Infof("could not retrive deploy timeout var...setting to %d secs\n", tOut/1000)
	}

	s.ccDeployTimeout = time.Duration(tOut) * time.Millisecond

	//get chaincode exec timeout
	tOut, err = strconv.Atoi(viper.GetString("chaincode.exectimeout"))
	if err != nil {
		tOut = chaincodeExecTimeoutDefault
		chaincodeLogger.Infof("could not retrive exec timeout var...setting to %d secs\n", tOut/1000)
	}

	s.ccExecTimeout = time.Duration(tOut) * time.Millisecond

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
	name              ChainName
	runningChaincodes *runningChaincodes
	peerAddress       string
	ccStartupTimeout  time.Duration
	ccDeployTimeout   time.Duration
	ccExecTimeout     time.Duration
	userRunsCC        bool
	txHandler         cred.TxConfidentialityHandler
	nodeID            string
	clientGuide       *config.ClientSpec
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

	var handler *Handler
	var err error
	//TODO: handling mutiple stream in one handler
	for l, chrte := range chaincodeSupport.runningChaincodes.chaincodeMap[key] {

		//so it was a pending runtime (just launched) and can be use
		//(all cc just created is equivalence for pending launching instance
		//and we can assigned to any one of them)
		if chrte.handler == nil {
			handler = newChaincodeSupportHandler(chaincodeSupport)
			handler.ChaincodeID = cID
			handler.Ledger = l

			chrte.handler = handler
			defer func(chrte *chaincodeRTEnv) { chrte.launchNotify <- err }(chrte)
			break
		}

	}

	//if we can not find any available pending runtime, maybe we can cache it (only for userruncc mode)
	if handler == nil {
		if !chaincodeSupport.userRunsCC {
			return nil, nil, fmt.Errorf("Can't register chaincode without invoking deploy tx")
		} else {
			handler = newChaincodeSupportHandler(chaincodeSupport)
			handler.ChaincodeID = cID
			chaincodeSupport.runningChaincodes.freeChainCodes[key] = append(chaincodeSupport.runningChaincodes.freeChainCodes[key],
				&chaincodeRTEnv{handler: handler})
		}
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
	l := chaincodehandler.Ledger
	chaincodeSupport.runningChaincodes.Lock()
	defer chaincodeSupport.runningChaincodes.Unlock()

	chaincodeLogger.Debugf("Deregistered handler with key %s and ledger %p", key, l)
	if l == nil {
		//this should be a free rte ...
		for i, rte := range chaincodeSupport.runningChaincodes.freeChainCodes[key] {
			if rte.handler == chaincodehandler {
				freeRtes := chaincodeSupport.runningChaincodes.freeChainCodes[key]
				freeRtes = append(freeRtes[:i], freeRtes[i+1:]...)
				chaincodeSupport.runningChaincodes.freeChainCodes[key] = freeRtes
				break
			}
		}
	} else {

		if _, ok := chaincodeSupport.runningChaincodes.chaincodeMap[key]; !ok {
			chaincodeLogger.Warning("handler for chaincode %s has been pruned", key)
			return
		}

		delete(chaincodeSupport.runningChaincodes.chaincodeMap[key], l)
		if len(chaincodeSupport.runningChaincodes.chaincodeMap[key]) == 0 {
			chaincodeLogger.Debugf("clean handlers group with key: %s", key)
			delete(chaincodeSupport.runningChaincodes.chaincodeMap, key)
		}
	}

}

// //get args and env given chaincodeID
// func (chaincodeSupport *ChaincodeSupport) getArgsAndEnv(cID *pb.ChaincodeID, cLang pb.ChaincodeSpec_Type) (args []string, envs []string, err error) {
// 	envs = []string{"CORE_CHAINCODE_ID_NAME=" + cID.Name}
// 	//if TLS is enabled, pass TLS material to chaincode
// 	if chaincodeSupport.peerTLS {
// 		envs = append(envs, "CORE_PEER_TLS_ENABLED=true")
// 		envs = append(envs, "CORE_PEER_TLS_CERT_FILE="+TLSRootCertFile)
// 		if chaincodeSupport.peerTLSSvrHostOrd != "" {
// 			envs = append(envs, "CORE_PEER_TLS_SERVERHOSTOVERRIDE="+chaincodeSupport.peerTLSSvrHostOrd)
// 		}
// 	} else {
// 		envs = append(envs, "CORE_PEER_TLS_ENABLED=false")
// 	}
// 	switch cLang {
// 	case pb.ChaincodeSpec_GOLANG, pb.ChaincodeSpec_CAR:
// 		//chaincode executable will be same as the name of the chaincode
// 		args = []string{chaincodeSupport.chaincodeInstallPath + cID.Name, fmt.Sprintf("-peer.address=%s", chaincodeSupport.peerAddress)}
// 		chaincodeLogger.Debugf("Executable is %s", args[0])
// 	case pb.ChaincodeSpec_JAVA:
// 		//TODO add security args
// 		args = strings.Split(
// 			fmt.Sprintf("java -jar chaincode.jar -a %s -i %s",
// 				chaincodeSupport.peerAddress, cID.Name),
// 			" ")
// 		if chaincodeSupport.peerTLS {
// 			args = append(args, " -s")
// 		}
// 		chaincodeLogger.Debugf("Executable is %s", args[0])
// 	default:
// 		return nil, nil, fmt.Errorf("Unknown chaincodeType: %s", cLang)
// 	}
// 	return args, envs, nil
// }

// launchAndWaitForRegister will launch container if not already running. Use the targz to create the image if not found
func (chaincodeSupport *ChaincodeSupport) launchAndWaitForRegister(ctxt context.Context, cds *pb.ChaincodeDeploymentSpec, cID *pb.ChaincodeID, cLang pb.ChaincodeSpec_Type, targz io.Reader) error {

	if chaincodeSupport.userRunsCC && cds.GetExecEnv() != pb.ChaincodeDeploymentSpec_SYSTEM {
		return fmt.Errorf("chaincode is user-running and no need to launch")
	}

	chaincode := cID.Name
	if chaincode == "" {
		return fmt.Errorf("chaincode name not set")
	}

	//launch the chaincode
	args, env, err := platforms.GetArgsAndEnv(cds.ChaincodeSpec, chaincodeSupport.clientGuide)
	if err != nil {
		return err
	}

	chaincodeLogger.Debugf("start container: %s(chain:%s,nodeid:%s)", chaincode, chaincodeSupport.name, chaincodeSupport.nodeID)
	chaincodeLogger.Debugf("envs are %v, %v", args, env)

	vmtype, _ := chaincodeSupport.getVMType(cds)

	sir := container.StartImageReq{CCID: ccintf.CCID{ChaincodeSpec: cds.ChaincodeSpec, NetworkID: string(chaincodeSupport.name), PeerID: chaincodeSupport.nodeID}, Reader: targz, Args: args, Env: env}

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

func (chaincodeSupport *ChaincodeSupport) finishLaunching(l *ledger.Ledger, chaincode string, notify error) {

	//we need a "lasttime checking", so if the launching chaincode is not registered,
	//we just erase it and notify a termination
	chaincodeSupport.runningChaincodes.Lock()
	defer chaincodeSupport.runningChaincodes.Unlock()
	if rte, ok := chaincodeSupport.chaincodeHasBeenLaunched(l, chaincode); !ok {
		//nothing to do
		chaincodeLogger.Warningf("trying to terminate the launching for unexist chaincode %s", chaincode)
		return
		// } else if rte.handler != nil {
		// 	//chaincode is registered ...
		// 	return false
		// } else {
	} else {

		//sanity check
		if rte.waitCtx == nil {
			panic("another routine has make this calling, we have wrong code?")
		}
		rte.launchResult = notify
		rte.waitCtx = nil
	}

	//if we get err notify, we must clear the rte even it has created a handler
	if notify != nil {
		ml := chaincodeSupport.runningChaincodes.chaincodeMap[chaincode]
		delete(ml, l)
		if len(ml) == 0 {
			delete(chaincodeSupport.runningChaincodes.chaincodeMap, chaincode)
		}
	}
}

//Stop stops a chaincode if running
func (chaincodeSupport *ChaincodeSupport) Stop(context context.Context, cds *pb.ChaincodeDeploymentSpec) error {

	if chaincodeSupport.userRunsCC && cds.GetExecEnv() != pb.ChaincodeDeploymentSpec_SYSTEM {
		return fmt.Errorf("chaincode is user-running and no need to stop")
	}

	chaincode := cds.ChaincodeSpec.ChaincodeID.Name
	if chaincode == "" {
		return fmt.Errorf("chaincode name not set")
	}

	//stop the chaincode
	sir := container.StopImageReq{CCID: ccintf.CCID{ChaincodeSpec: cds.ChaincodeSpec, NetworkID: string(chaincodeSupport.name), PeerID: chaincodeSupport.nodeID}, Timeout: 0}

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
	if chrte, ok := chaincodeSupport.chaincodeHasBeenLaunched(ledger, chaincode); ok {
		if chrte.waitCtx == nil {
			chaincodeLogger.Debugf("chaincode is running(no need to launch) : %s", chaincode)
			chaincodeSupport.runningChaincodes.Unlock()
			return nil, chrte
		}
		//all of us must wait here till the cc is really launched (or failed...)
		chaincodeLogger.Debug("chainicode not in READY state...waiting")
		chaincodeSupport.runningChaincodes.Unlock()

		select {
		case <-chrte.waitCtx.Done():
		case <-ctx.Done():
			return fmt.Errorf("Cancel: %s", ctx.Err()), nil
		}

		chaincodeLogger.Debugf("wait chaincode %s for lauching: [%s]", chaincode, chrte.launchResult)
		if chrte.launchResult == nil {
			return nil, chrte
		} else {
			return chrte.launchResult, nil
		}
	}

	//the first one create rte and start its adventure ...
	chrte := chaincodeSupport.preLaunchSetup(ledger, chaincode)
	var waitCf context.CancelFunc
	chrte.waitCtx, waitCf = context.WithCancel(ctx)
	chaincodeSupport.runningChaincodes.Unlock()

	var err error
	var depTx *pb.Transaction

	//so the launchResult in runtime will be set first
	defer waitCf()
	defer func() { chaincodeSupport.finishLaunching(ledger, chaincode, err) }()

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
	} else {
		//TODO: we never check the old-style deployment (no deploy name, just the txid) any more?
		var depTxid []byte
		depTxid, err = checkDeployTxID(chaincode, ledger)
		if err != nil {
			return err, chrte
		}
		if depTxid != nil {
			return fmt.Errorf("Try to redeploy existed chaincode [%s]", chaincode), chrte
		}
	}

	cLang := cds.ChaincodeSpec.Type
	//launch container if it is a System container or not in dev mode
	if !chaincodeSupport.userRunsCC || cds.ExecEnv == pb.ChaincodeDeploymentSpec_SYSTEM {
		var packrd *runtimeReader
		if cds.ExecEnv != pb.ChaincodeDeploymentSpec_SYSTEM {
			packrd, err = WriteRuntimePackage(cds, chaincodeSupport.clientGuide)
			if err != nil {
				chaincodeLogger.Errorf("WriteRuntimePackage failed %s", err)
				return err, chrte
			}
		}

		wctx, wctxend := context.WithTimeout(ctx, chaincodeSupport.ccDeployTimeout)
		defer wctxend()

		err = chaincodeSupport.launchAndWaitForRegister(wctx, cds, cID, cLang, packrd)
		//first finish and trace the real reason in runtime reading
		if omiterr := packrd.Finish(); omiterr != nil {
			chaincodeLogger.Errorf("WriteRuntimePackage failed, reason was %s", omiterr)
		}

		if err != nil {
			chaincodeLogger.Errorf("launchAndWaitForRegister failed %s", err)
			return err, chrte
		}

		//from here on : if we launch the container and get an error, we need to stop the container
		defer func() {
			if err != nil {
				chaincodeLogger.Infof("stopping due to error while launching %s", err)
				errIgnore := chaincodeSupport.Stop(ctx, cds)
				if errIgnore != nil {
					chaincodeLogger.Debugf("error on stop %s(%s)", errIgnore, err)
				}
			}
		}()
	}

	wctx, wctxend := context.WithTimeout(ctx, chaincodeSupport.ccStartupTimeout)
	defer wctxend()

	//wait for REGISTER state
	select {
	case err = <-chrte.launchNotify:
	case <-wctx.Done():
		err = fmt.Errorf("Timeout expired while starting chaincode %s(chain:%s,nodeid:%s)", chaincode, chaincodeSupport.name, chaincodeSupport.nodeID)
	}
	if err != nil {
		return err, chrte
	}

	//send ready (if not deploy) for ready state
	if chrte.handler == nil {
		err = fmt.Errorf("handler is not available though lauching [%s(chain:%s,nodeid:%s)] notify ok", chaincode, chaincodeSupport.name, chaincodeSupport.nodeID)
		return err, chrte
	}
	err = chrte.handler.readyChaincode(t, depTx)
	if err != nil {
		return err, chrte
	}
	chaincodeLogger.Debug("LaunchChaincode complete")
	return nil, chrte
}

// returns the security help set from NewChaincodeSupport
func (chaincodeSupport *ChaincodeSupport) getTxHandler() cred.TxConfidentialityHandler {
	return chaincodeSupport.txHandler
}

//getVMType - just returns a string for now. Another possibility is to use a factory method to
//return a VM executor
func (chaincodeSupport *ChaincodeSupport) getVMType(cds *pb.ChaincodeDeploymentSpec) (string, error) {
	if cds.ExecEnv == pb.ChaincodeDeploymentSpec_SYSTEM {
		return container.SYSTEM, nil
	}
	return container.DOCKER, nil
}

// Register the bidi stream entry point called by chaincode to register with the Peer.
// registerHandler implements ccintf.HandleChaincodeStream for all vms to call with appropriate stream
// It call the main loop in handler for handling the associated Chaincode stream
func (chaincodeSupport *ChaincodeSupport) Register(stream pb.ChaincodeSupport_RegisterServer) error {
	return chaincodeSupport.HandleChaincodeStream(stream.Context(), stream)
}

func (chaincodeSupport *ChaincodeSupport) HandleChaincodeStream(ctx context.Context, stream ccintf.ChaincodeStream) error {
	msg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("Error in recv [%s]", err)
	} else if msg.Type != pb.ChaincodeMessage_REGISTER {
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
func (chaincodeSupport *ChaincodeSupport) Execute(ctxt context.Context, chrte *chaincodeRTEnv, cMsg *pb.ChaincodeInput, tx *pb.Transaction, outstate ledger.TxExecStates) (*pb.ChaincodeMessage, ledger.TxExecStates, error) {

	msg, err := createTransactionMessage(tx, cMsg)
	if err != nil {
		return nil, emptyExState, fmt.Errorf("Failed to transaction message(%s)", err)
	}

	// if err = handler.setChaincodeSecurityContext(tx, msg); err != nil {
	// 	return nil, emptyExState, err
	// }

	wctx, cf := context.WithTimeout(ctxt, chaincodeSupport.ccExecTimeout)
	defer cf()
	msg, outstate, err = chrte.handler.executeMessage(wctx, msg, outstate)

	return msg, outstate, err
}

func (chaincodeSupport *ChaincodeSupport) ReleaseLedger(l *ledger.Ledger) error {

	chaincodeLogger.Debugf("Release all handler bind to ledger %p", l)

	chaincodeSupport.runningChaincodes.Lock()
	defer chaincodeSupport.runningChaincodes.Unlock()

	for _, rtes := range chaincodeSupport.runningChaincodes.chaincodeMap {
		if rte, ok := rtes[l]; ok {
			//sanity check: a run-time of pending status indicate some execute is running with
			//corresponding ledger, and the caller do execute and releaseledger simultaneously,
			//which should not be allowed
			if rte.handler == nil {
				panic("try to delete a runtime when launching is pending, indicate a malformed, racing code")
			}

			//disconnect all streams
			//DO NOT delete the corresponding handler explicitly, deregisterHandler will do that
			for _, strm := range rte.handler.workingStream {
				//TODO: we should define some message to gracely shutdown the stream
				strm.resp <- &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR}
			}
		}
	}

	return nil
}
