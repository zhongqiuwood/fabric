package node

import (
	"errors"
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
		return errors.New("Listen address for service not specified")
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
