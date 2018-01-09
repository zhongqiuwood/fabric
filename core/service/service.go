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
	
	"github.com/abchain/fabric/core"
	"github.com/abchain/fabric/core/rest"
	"github.com/abchain/fabric/core/comm"
	"github.com/abchain/fabric/core/util"
	pb "github.com/abchain/fabric/protos"
)

var serviceLogger = logging.MustGetLogger("service")

func GetServiceTLSCred() (credentials.TransportCredentials, error){

	return credentials.NewServerTLSFromFile(
		util.CanonicalizeFilePath(viper.GetString("peer.tls.cert.file")),
		util.CanonicalizeFilePath(viper.GetString("peer.tls.key.file")))

}

func startSrv(server *rest.ServerOpenchain, devops *core.Devops, listenAddr string, 
	enableTLS bool, regAdmin bool) error{

	if "" == listenAddr {
		return errors.New("Listen address for service not specified")
	}	
	
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		grpclog.Fatalf("Failed to listen: %v", err)
		return err
	}	
	
	var opts []grpc.ServerOption
	
	if enableTLS{
		creds, err := GetServiceTLSCred()

		if err != nil {
			grpclog.Fatalf("Failed to generate credentials %v", err)
			return err
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}			
	}
	
	grpcServer := grpc.NewServer(opts...)
	
	pb.RegisterDevopsServer(grpcServer, devops)
	pb.RegisterOpenchainServer(grpcServer, server)
	if regAdmin{
		pb.RegisterAdminServer(grpcServer, core.NewAdminServer())
	}
	
	serviceLogger.Infof("Starting service with address=%s", listenAddr)	
	
	if grpcErr := grpcServer.Serve(lis); grpcErr != nil {
		return fmt.Errorf("grpc server exited with error: %s", grpcErr)
	} else {
		serviceLogger.Info("grpc server exited")
	}	
	
	return nil	
}

func StartLocalService(server *rest.ServerOpenchain, devops *core.Devops) error{

	return startSrv(server, devops, viper.GetString("peer.localaddr"), 
		comm.TLSEnabledForLocalSrv(), true)
}

func StartFabricService(server *rest.ServerOpenchain, devops *core.Devops) error{
	
	return startSrv(server, devops, viper.GetString("service.address"), 
		comm.TLSEnabledforService(), false)		

}