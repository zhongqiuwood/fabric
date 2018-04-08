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

package node

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/abchain/fabric/consensus/helper"
	"github.com/abchain/fabric/core"
	"github.com/abchain/fabric/core/chaincode"
	"github.com/abchain/fabric/core/comm"
	"github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/embedded_chaincode"
	"github.com/abchain/fabric/core/ledger/genesis"
	"github.com/abchain/fabric/core/peer"
	"github.com/abchain/fabric/core/rest"
	"github.com/abchain/fabric/core/service"
	"github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/events/producer"
	pb "github.com/abchain/fabric/protos"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
)

var chaincodeDevMode bool

func startCmd() *cobra.Command {
	// Set the flags on the node start command.
	flags := nodeStartCmd.Flags()
	flags.BoolVarP(&chaincodeDevMode, "peer-chaincodedev", "", false,
		"Whether peer in chaincode development mode")

	return nodeStartCmd
}

var nodeStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the node.",
	Long:  `Starts a node that interacts with the network.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return StartNode(nil)
	},
}

func StartNode(postrun func() error) error {
	// Parameter overrides must be processed before any paramaters are
	// cached. Failures to cache cause the server to terminate immediately.
	if chaincodeDevMode {
		logger.Info("Running in chaincode development mode")
		logger.Info("Set consensus to NOOPS and user starts chaincode")
		logger.Info("Disable loading validity system chaincode")

		viper.Set("peer.validator.enabled", "true")
		viper.Set("peer.validator.consensus", "noops")
		viper.Set("chaincode.mode", chaincode.DevModeUserRunsChaincode)

	}

	if err := peer.CacheConfiguration(); err != nil {
		return err
	}

	peerEndpoint, err := peer.GetPeerEndpoint()
	if err != nil {
		err = fmt.Errorf("Failed to get Peer Endpoint: %s", err)
		return err
	}

	ehubLis, ehubGrpcServer, err := createEventHubServer()
	if err != nil {
		grpclog.Fatalf("Failed to create ehub server: %v", err)
	}

	logger.Infof("Security enabled status: %t", core.SecurityEnabled())
	if viper.GetBool("security.privacy") {
		if core.SecurityEnabled() {
			logger.Infof("Privacy enabled status: true")
		} else {
			panic(errors.New("Privacy cannot be enabled as requested because security is disabled"))
		}
	} else {
		logger.Infof("Privacy enabled status: false")
	}

	if err := writePid(util.CanonicalizePath(viper.GetString("peer.fileSystemPath"))+"peer.pid",
		os.Getpid()); err != nil {
		return err
	}

	pb.CurrentDbVersion = uint32(viper.GetInt("peer.db.version"))

	db.Start(pb.CurrentDbVersion)
	defer db.Stop(pb.CurrentDbVersion)

	secHelper, err := getSecHelper()
	if err != nil {
		return err
	}

	secHelperFunc := func() crypto.Peer {
		return secHelper
	}

	srv_chaincode := func(server *grpc.Server) {
		registerChaincodeSupport(chaincode.DefaultChain, server, secHelper)
	}

	var peerServer *peer.Impl

	// Create the peerServer
	if peer.ValidatorEnabled() {
		logger.Debug("Running as validating peer - making genesis block if needed")
		makeGenesisError := genesis.MakeGenesis()
		if makeGenesisError != nil {
			return makeGenesisError
		}
		logger.Debugf("Running as validating peer - installing consensus %s",
			viper.GetString("peer.validator.consensus"))

		peerServer, err = peer.NewPeerWithEngine(secHelperFunc, helper.GetEngine)
	} else {
		logger.Debug("Running as non-validating peer")
		peerServer, err = peer.NewPeerWithHandler(secHelperFunc, peer.NewPeerHandler)
	}

	if err != nil {
		logger.Fatalf("Failed creating new peer with handler %v", err)

		return err
	}

	// Register the Peer server
	srv_peer := func(server *grpc.Server) {
		pb.RegisterPeerServer(server, peerServer)
	}

	// Register Devops server
	serverDevops := core.NewDevopsServer(peerServer)
	//pb.RegisterDevopsServer(grpcServer, serverDevops)

	// Register the ServerOpenchain server
	serverOpenchain, err := rest.NewOpenchainServerWithPeerInfo(peerServer)
	if err != nil {
		err = fmt.Errorf("Error creating OpenchainServer: %s", err)
		return err
	}

	srv_devops := func(server *grpc.Server) {
		pb.RegisterDevopsServer(server, serverDevops)
	}

	srv_openchain := func(server *grpc.Server) {
		pb.RegisterOpenchainServer(server, serverOpenchain)
	}

	srv_admin := func(server *grpc.Server) {
		pb.RegisterAdminServer(server, core.NewAdminServer())
	}

	// Create and register the REST service if configured
	if viper.GetBool("rest.enabled") {
		go rest.StartOpenchainRESTServer(serverOpenchain, serverDevops)
	}

	// Start the grpc server. Done in a goroutine so we can deploy the
	// genesis block if needed.
	serve := make(chan error)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		serve <- nil
	}()

	fsrv := func(addr string, tls bool, serv ...func(*grpc.Server)) {
		srverr := service.StartService(addr, tls, serv...)
		if srverr != nil {
			srverr = fmt.Errorf("fabric service exited with error: %s", srverr)
		} else {
			logger.Info("fabric service exited")
		}
		serve <- srverr
	}

	logger.Infof("Discovery hidden mode: %t", comm.DiscoveryHidden())
	logger.Infof("Starting peer with ID=%s, network ID=%s, address=%s, rootnodes=%v, validator=%v",
		peerEndpoint.ID, viper.GetString("peer.networkId"), peerEndpoint.Address,
		viper.GetString("peer.discovery.rootnode"), peer.ValidatorEnabled())

	//main rpc (peer, chaincode)
	go fsrv(viper.GetString("peer.listenAddress"), comm.TLSEnabled(), srv_chaincode, srv_peer)

	//local rpc (only work in the same network)
	logger.Infof("Starting local service")
	go fsrv(viper.GetString("peer.localaddr"), comm.TLSEnabledForLocalSrv(), srv_admin, srv_devops, srv_openchain)

	//service rpc
	if viper.GetBool("service.enabled") {
		logger.Infof("Starting devops service")
		go fsrv(viper.GetString("service.address"), comm.TLSEnabledforService(), srv_devops)
	}

	// Start the event hub server, still not integrate into service package
	if ehubGrpcServer != nil && ehubLis != nil {
		go ehubGrpcServer.Serve(ehubLis)
	}

	if viper.GetBool("peer.profile.enabled") {
		go func() {
			profileListenAddress := viper.GetString("peer.profile.listenAddress")
			logger.Infof("Starting profiling server with listenAddress = %s", profileListenAddress)
			if profileErr := http.ListenAndServe(profileListenAddress, nil); profileErr != nil {
				logger.Errorf("Error starting profiler: %s", profileErr)
			}
		}()
	}

	if postrun != nil {
		err = postrun()
		if err != nil {
			logger.Info("Post run fail, exit immediately ...", err)
		}
	}

	// Block until grpc server exits
	if err == nil {
		err = <-serve
	}

	// TODO: we still not clear other serverice like rest and ehub ...
	service.StopServices()

	return err
}

func registerChaincodeSupport(chainname chaincode.ChainName, grpcServer *grpc.Server,
	secHelper crypto.Peer) {

	//get user mode
	userRunsCC := false
	if viper.GetString("chaincode.mode") == chaincode.DevModeUserRunsChaincode {
		userRunsCC = true
	}

	//check duplicated creation for same chain
	//it is safe to bind a implement to differnt server (it is thread-safe)
	ccSrv := chaincode.GetChain(chainname)
	if ccSrv == nil {
		ccSrv = chaincode.NewChaincodeSupport(chainname, peer.GetPeerEndpoint, userRunsCC, secHelper)
		//Now that chaincode is initialized, register all system chaincodes.
		embedded_chaincode.RegisterSysCCs()
	}

	pb.RegisterChaincodeSupportServer(grpcServer, ccSrv)
}

func createEventHubServer() (net.Listener, *grpc.Server, error) {
	var lis net.Listener
	var grpcServer *grpc.Server
	var err error
	if peer.ValidatorEnabled() {
		lis, err = net.Listen("tcp", viper.GetString("peer.validator.events.address"))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to listen: %v", err)
		}

		//TODO - do we need different SSL material for events ?
		var opts []grpc.ServerOption

		if comm.TLSEnabled() {
			creds, err := service.GetServiceTLSCred()
			if err != nil {
				return nil, nil, fmt.Errorf("Failed to generate credentials %v", err)
			}
			opts = []grpc.ServerOption{grpc.Creds(creds)}
		}

		grpcServer = grpc.NewServer(opts...)
		ehServer := producer.NewEventsServer(
			uint(viper.GetInt("peer.validator.events.buffersize")),
			viper.GetInt("peer.validator.events.timeout"))

		pb.RegisterEventsServer(grpcServer, ehServer)
	}
	return lis, grpcServer, err
}

var once sync.Once

//this should be called exactly once and the result cached
//NOTE- this crypto func might rightly belong in a crypto package
//and universally accessed
func getSecHelper() (crypto.Peer, error) {
	var secHelper crypto.Peer
	var err error
	once.Do(func() {
		if core.SecurityEnabled() {
			enrollID := viper.GetString("security.enrollID")
			enrollSecret := viper.GetString("security.enrollSecret")
			if peer.ValidatorEnabled() {
				logger.Debugf("Registering validator with enroll ID: %s", enrollID)
				if err = crypto.RegisterValidator(enrollID, nil, enrollID, enrollSecret); nil != err {
					return
				}
				logger.Debugf("Initializing validator with enroll ID: %s", enrollID)
				secHelper, err = crypto.InitValidator(enrollID, nil)
				if nil != err {
					return
				}
			} else {
				logger.Debugf("Registering non-validator with enroll ID: %s", enrollID)
				if err = crypto.RegisterPeer(enrollID, nil, enrollID, enrollSecret); nil != err {
					return
				}
				logger.Debugf("Initializing non-validator with enroll ID: %s", enrollID)
				secHelper, err = crypto.InitPeer(enrollID, nil)
				if nil != err {
					return
				}
			}
		}
	})
	return secHelper, err
}
