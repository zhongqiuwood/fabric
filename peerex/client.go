package peerex

import (
	"os"
	"path/filepath"
	"strings"
	"runtime"
	
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	
	"github.com/hyperledger/fabric/core"
	"github.com/hyperledger/fabric/core/util"
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

func InitLogger(module string) *logging.Logger{
	return logging.MustGetLogger(module)
}

var globalConfigDone = false

type GlobalConfig struct{
	
	EnvPrefix  	   string
	ConfigFileName string	
	ConfigPath 	   []string
	
	LogRole	   string //peer, node, network, chaincode, version, can apply different log level from config file	
}

func (_ *GlobalConfig) InitFinished() bool{
	return globalConfigDone
}

func (g GlobalConfig) InitGlobal() error{
	
	if globalConfigDone {
		logger.Info("Global initiazation has done ...")
		return nil
	}
	
	if g.EnvPrefix == ""{
		g.EnvPrefix = viperEnvPrefix
	}
	
	if g.ConfigFileName == ""{
		g.ConfigFileName = viperFileName
	}
	
	if g.ConfigPath == nil{
		g.ConfigPath = make([]string, 1, 10)
		g.ConfigPath[0] = "./"// Path to look for the config file in
		// Path to look for the config file in based on GOPATH
		gopath := os.Getenv("GOPATH")
		for _, p := range filepath.SplitList(gopath) {
			peerpath := filepath.Join(p, "src/github.com/hyperledger/fabric/peer")
			g.ConfigPath = append(g.ConfigPath, peerpath)
		}		
	}
	
	err := InitPeerViper(g.EnvPrefix, g.ConfigFileName, g.ConfigPath...)
	if err != nil{
		return err
	}
	
	if g.LogRole == "" {
		g.LogRole = "client"
	}
	flogging.LoggingInit(g.LogRole)
	
	err = core.CacheConfiguration()
	if err != nil{
		return err
	}
	
	globalConfigDone = true
	logger.Info("Global init done ...")
	
	return nil
}

func (_ *GlobalConfig) GetPeerFS() string{
	if !globalConfigDone{
		return ""
	}
	
	return util.CanonicalizePath(viper.GetString("peer.fileSystemPath"))
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
