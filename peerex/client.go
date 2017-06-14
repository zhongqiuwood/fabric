package peerex

import (
	"os"
	"path/filepath"
	"strings"
	"runtime"
	
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	
	"github.com/hyperledger/fabric/core"
	"github.com/hyperledger/fabric/flogging"
)

var logger = logging.MustGetLogger("clientcore")

func InitPeerViper(envprefix string, filename string, configPath ...string) error{
	
	viper.SetEnvPrefix(envprefix)
	viper.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)	
	
	for _, c := range configPath {
		viper.AddConfigPath(c)	
	}
	
	viper.SetConfigName(filename) // Name of config file (without extension)

	return viper.ReadInConfig() // Find and read the config file
	
}

type GlobalConfig struct{
	
	EnvPrefix  	   string
	ConfigFileName string	
	ConfigPath []  string
	
	LogRole	   string //peer, node, network, chaincode, version, can apply different log level from config file	
}

func (g *GlobalConfig) InitGlobal() error{
	
	if g.EnvPrefix == ""{
		g.EnvPrefix = viperEnvPrefix
	}
	
	if g.ConfigFileName == ""{
		g.ConfigFileName = viperFileName
	}
	
	if g.ConfigPath == nil{
		viper.AddConfigPath("./") // Path to look for the config file in
		// Path to look for the config file in based on GOPATH
		gopath := os.Getenv("GOPATH")
		for _, p := range filepath.SplitList(gopath) {
			peerpath := filepath.Join(p, "src/github.com/hyperledger/fabric/peer")
			viper.AddConfigPath(peerpath)
		}		
	}
	
	err := InitPeerViper(g.EnvPrefix, g.ConfigFileName, g.ConfigPath...)
	if err != nil{
		return err
	}
	
	if g.LogRole == "" {
		g.LogRole = "peer"
	}
	flogging.LoggingInit(g.LogRole)
	
	logger.Info("Global init done ...")
	
	return core.CacheConfiguration()
}

type PerformanceTuning struct{
	MaxProcs int
}

func (p *PerformanceTuning) Apply() error{
	
	if p.MaxProcs == 0{
		runtime.GOMAXPROCS(viper.GetInt("peer.gomaxprocs"))
	}else{
		runtime.GOMAXPROCS(p.MaxProcs)
	}
	
	return nil
}
