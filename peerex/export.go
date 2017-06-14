package common

//wrap and export vendor related packages

import (
	"google.golang.org/grpc"
	"github.com/spf13/viper"
)

type DevopsConn struct{
	C *grpc.ClientConn
}

func InitPeerViper(configPath string){
	viper.AddConfigPath(configPath)	
}



