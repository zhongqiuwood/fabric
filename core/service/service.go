package service

import (
	
	"errors"
	"fmt"
	"net"
	_ "golang.org/x/net/context"
	"github.com/op/go-logging"	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"		
	"github.com/spf13/viper"
	
	"github.com/hyperledger/fabric/core"
	"github.com/hyperledger/fabric/core/rest"
	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/core/util"
	_ "github.com/hyperledger/fabric/protos"
)

var serviceLogger = logging.MustGetLogger("service")

func GetServiceTLSCred() (credentials.TransportCredentials, error){

	if comm.TLSEnabled() {
		return credentials.NewServerTLSFromFile(
			util.CanonicalizeFilePath(viper.GetString("peer.tls.cert.file")),
			util.CanonicalizeFilePath(viper.GetString("peer.tls.key.file")))
	}

	return nil, nil	
}

func StartFabricService(server *rest.ServerOpenchain, devops *core.Devops) error{
	
	//just c&p code from peer/node/start.go here
	listenAddr := viper.GetString("service.address")

	if "" == listenAddr {
		return errors.New("Listen address for service not specified")
	}	
	
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		grpclog.Fatalf("Failed to listen: %v", err)
		return err
	}	
	
	var opts []grpc.ServerOption
	creds, err := GetServiceTLSCred()

	if err != nil {
		grpclog.Fatalf("Failed to generate credentials %v", err)
	}else if creds != nil{
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	
	grpcServer := grpc.NewServer(opts...)
	
	serviceLogger.Infof("Starting peer service with address=%s",
		listenAddr)	
	
	if grpcErr := grpcServer.Serve(lis); grpcErr != nil {
		return fmt.Errorf("grpc server exited with error: %s", grpcErr)
	} else {
		serviceLogger.Info("grpc server exited")
	}	
	
	return nil
}