package node

import (
	"fmt"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"

	"github.com/abchain/fabric/core/comm"
	"github.com/abchain/fabric/core/util"
)

var serviceLogger = logging.MustGetLogger("service")
var servers []*grpc.Server

type servicePoint struct {
	*grpc.Server
	lPort     net.Listener
	srvStatus chan error
}

//standard init read all configurations from viper, inwhich we use a subtree of *peer*
//in the 0.6's configurations
func (ep *servicePoint) Init(conf *viper.Viper) error {

	addr := conf.GetString("listenAddress")
	if addr == "" {
		return fmt.Errorf("Peer's listening address for service not specified")
	}

	if err := ep.SetPort(addr); err != nil {
		return err
	}

	var opts []grpc.ServerOption

	//tls
	if conf.GetBool("tls.enable") {

		creds, err := credentials.NewServerTLSFromFile(
			util.CanonicalizeFilePath(conf.GetString("tls.cert.file")),
			util.CanonicalizeFilePath(conf.GetString("tls.key.file")))

		if err != nil {
			serviceLogger.Errorf("Failed to generate peer's credentials: %v", err)
			return err
		}
		opts = append(opts, grpc.Creds(creds))

	}

	//other options (msg size, etc ...)
	msgMaxsize := conf.GetInt("messagesizelimit")
	if msgMaxsize > 1024*1024*4 {
		//in p2p network we usually require a sync channel of recv and send
		opts = append(opts, grpc.MaxRecvMsgSize(msgMaxsize), grpc.MaxSendMsgSize(msgMaxsize))
	}

	ep.Server = grpc.NewServer(opts...)
	return nil
}

//some routines for create a simple grpc server (currently only port, no tls and other options enable)
func (ep *servicePoint) SetPort(listenAddr string) error {
	var err error
	ep.lPort, err = net.Listen("tcp", listenAddr)
	if err != nil {
		serviceLogger.Errorf("Failed to listen on %s: %v", listenAddr, err)
		return err
	}
	ep.Server = grpc.NewServer()
	return nil
}

func (ep *servicePoint) Start() error {

	if ep.Server == nil {
		return fmt.Errorf("Server is not inited")
	}

	serviceLogger.Infof("Starting service with address=%s", ep.lPort.Addr())
	ep.srvStatus = make(chan error, 1)
	go func() {
		ep.srvStatus <- ep.Serve(ep.lPort)
		serviceLogger.Info("grpc server exited")
	}()

	return nil
}

var ServiceRunning = fmt.Errorf("Service is still running")

func (ep *servicePoint) Status() error {

	if ep.srvStatus == nil {
		return fmt.Errorf("Service not running")
	}

	select {
	case e := <-ep.srvStatus:
		//so the function is re-entriable
		ep.srvStatus <- e
		return e
	default:
		return ServiceRunning
	}
}

func (ep *servicePoint) Stop() error {
	if ep.Server == nil {
		return fmt.Errorf("Server is not inited")
	} else if ep.srvStatus == nil {
		return fmt.Errorf("Service not running")
	}

	defer func() {
		ep.srvStatus = nil
	}()

	return ep.Stop()
}

//we still reserved the global APIs for node-wide services
func GetServiceTLSCred() (credentials.TransportCredentials, error) {

	return credentials.NewServerTLSFromFile(
		util.CanonicalizeFilePath(viper.GetString("peer.tls.cert.file")),
		util.CanonicalizeFilePath(viper.GetString("peer.tls.key.file")))

}

func StopServices() {

	serviceLogger.Infof("Stop all services")

	for _, s := range servers {
		s.Stop()
	}

	servers = nil
}

func StartService(listenAddr string, enableTLS bool, regserv ...func(*grpc.Server)) error {
	if "" == listenAddr {
		return fmt.Errorf("Listen address for service not specified")
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		serviceLogger.Fatalf("Failed to listen: %v", err)
		return err
	}

	var opts []grpc.ServerOption

	if enableTLS {
		creds, err := GetServiceTLSCred()

		if err != nil {
			serviceLogger.Fatalf("Failed to generate credentials %v", err)
			return err
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}

	msgsize := comm.MaxMessageSize()
	opts = append(opts, grpc.MaxRecvMsgSize(msgsize), grpc.MaxSendMsgSize(msgsize))

	grpcServer := grpc.NewServer(opts...)

	servers = append(servers, grpcServer)

	for _, f := range regserv {
		f(grpcServer)
	}

	serviceLogger.Infof("Starting service with address=%s", listenAddr)

	if grpcErr := grpcServer.Serve(lis); grpcErr != nil {
		return fmt.Errorf("grpc server exited with error: %s", grpcErr)
	} else {
		serviceLogger.Info("grpc server exited")
	}

	return nil
}
