package peerex

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/op/go-logging"
	"github.com/spf13/viper"

	"github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/flogging"
	"github.com/abchain/fabric/core/peer"
)

var logger = logging.MustGetLogger("clientcore")

//deprecated: use the local package in peerex/viper
func InitPeerViper(envprefix string, filename string, configPath ...string) error {

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

//deprecated: use the local package in peerex/logging
func InitLogger(module string) *logging.Logger {
	return logging.MustGetLogger(module)
}

var globalConfigDone = false

type GlobalConfig struct {
	EnvPrefix      string
	ConfigFileName string
	ConfigPath     []string
	SkipConfigFile bool

	LogRole string //peer, node, network, chaincode, version, can apply different log level from config file
}

type fileLog struct {
	backend logging.Backend
	bufwr   bufio.Writer
}

func (fl *fileLog) Log(lv logging.Level, id int, rec *logging.Record) error {
	fl.backend.Log(lv, id, rec)
	return nil
}

func (_ *GlobalConfig) InitFinished() bool {
	return globalConfigDone
}

func (g GlobalConfig) InitGlobalWrapper(resetlog bool,
	defaultViperSetting map[string]interface{}) error {

	for k, v := range defaultViperSetting {
		viper.SetDefault(k, v)
	}

	return g.InitGlobal(resetlog)
}

func (g GlobalConfig) InitGlobal(resetlog bool) error {

	if globalConfigDone {
		logger.Info("Global initiazation has done ...")
		return nil
	}

	if g.EnvPrefix == "" {
		g.EnvPrefix = viperEnvPrefix
	}

	if g.ConfigFileName == "" {
		g.ConfigFileName = viperFileName
	}

	if g.ConfigPath == nil {
		g.ConfigPath = make([]string, 1, 10)
		g.ConfigPath[0] = "./" // Path to look for the config file in
		// Path to look for the config file in based on GOPATH
		gopath := os.Getenv("GOPATH")
		for _, p := range filepath.SplitList(gopath) {
			peerpath := filepath.Join(p, "src/github.com/abchain/fabric/peer")
			g.ConfigPath = append(g.ConfigPath, peerpath)
		}
	}

	err := InitPeerViper(g.EnvPrefix, g.ConfigFileName, g.ConfigPath...)
	if !g.SkipConfigFile && err != nil {
		return err
	}

	fpath := peer.GetPeerLogPath()

	if fpath != "" {
		util.MkdirIfNotExist(fpath)
	}

	if g.LogRole == "" {
		g.LogRole = "client"
	}

	flogging.LoggingInit(g.LogRole)

	if resetlog {
		err = flogging.LoggingFileInit(fpath)
		if err != nil {
			logger.Warning("Could not output log to file:", err)
		}
	}

	globalConfigDone = true
	logger.Info("Global init done ...")

	return nil
}

func (_ *GlobalConfig) GetPeerFS() string {
	if !globalConfigDone {
		return ""
	}

	return util.CanonicalizePath(viper.GetString("peer.fileSystemPath"))
}

type PerformanceTuning struct {
	MaxProcs int
}

func (p *PerformanceTuning) Apply() error {

	if p.MaxProcs == 0 {
		runtime.GOMAXPROCS(viper.GetInt("peer.gomaxprocs"))
	} else {
		runtime.GOMAXPROCS(p.MaxProcs)
	}

	return nil
}
