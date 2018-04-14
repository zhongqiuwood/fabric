package peerex

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/op/go-logging"
	"github.com/spf13/viper"

	"github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/flogging"
	logutil "github.com/abchain/fabric/peerex/logging"
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

func (g GlobalConfig) InitGlobalWrapper(restlog bool,
	defaultViperSetting map[string]interface{}) error {

	for k, v := range defaultViperSetting {
		viper.SetDefault(k, v)
	}

	return g.InitGlobal(restlog)
}


func (g GlobalConfig) InitGlobal(restlog bool) error {

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

	if restlog {
		outputfile := viper.GetString("logging.output.file")
		if outputfile != "" {

			fpath := util.CanonicalizePath(viper.GetString("peer.fileSystemPath"))
			if fpath == "" {
				return errors.New("No filesystem path is specified but require log-to-file")
			}

			// failed to produce the log file in case of fpath not existing
			util.MkdirIfNotExist(fpath)

			if viper.GetBool("logging.output.postfix") {
				outputfile = outputfile + string(time.Now().Format("Mon Jan 2,2006 15-04-05"))
			}

			flog := filepath.Join(fpath, outputfile)

			flogout, err := logutil.NewFileLogBackend(flog, "", 0,
				logging.WARNING, logging.ERROR)

			format := logging.MustStringFormatter("%{time:15:04:05.000} %{level:.4s} [%{pid}]%{message}")
			formatterFileLog := logging.NewBackendFormatter(flogout, format)

			if err == nil {
				stdlogout := logutil.WrapBackend(os.Stderr, "", 0)
				logutil.SetCustomBackends([]logging.Backend{stdlogout, formatterFileLog})
			} else {
				logutil.SetBackend(os.Stderr, "", 0)
				logger.Error("Create file log output fail:", err)
			}
		}

	}

	if g.LogRole == "" {
		g.LogRole = "client"
	}

	flogging.LoggingInit(g.LogRole)

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
