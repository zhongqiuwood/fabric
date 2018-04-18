package logging

import (
	flog "github.com/abchain/fabric/flogging"
	"github.com/op/go-logging"
	"io"
	"os"
)

var (
	lowLayerBackend logging.Backend
	defFormat       logging.Formatter

	defBackend *flog.ModuleLeveled
)

func init() {

	defFormat := logging.MustStringFormatter(
		"%{color}%{time:15:04:05.000} [%{module}] %{shortfunc} -> %{level:.4s} %{id:03x}%{color:reset} %{message}",
	)

	lowLayerBackend = logging.NewLogBackend(os.Stderr, "", 0)
	backend := logging.NewBackendFormatter(lowLayerBackend, defFormat)

	defBackend = flog.DuplicateLevelBackend(backend)

}

func InitLogger(module string) *logging.Logger {

	logger := logging.MustGetLogger(module)
	logger.SetBackend(defBackend)

	return logger
}

//logger obtained from InitLogger do not identify to the other used in fabric module
//we can apply this settings to fabric
func ApplySetting() {
	logging.SetBackend(defBackend)
}

func SetLogFormat(format string) error {

	f, err := logging.NewStringFormatter(format)
	if err != nil {
		return err
	}

	defFormat = f

	defBackend.Backend = logging.NewBackendFormatter(lowLayerBackend, defFormat)

	return nil
}

func SetBackend(w io.Writer, prefix string, flag int) {

	lowLayerBackend = logging.NewLogBackend(os.Stderr, prefix, flag)

	defBackend.Backend = logging.NewBackendFormatter(lowLayerBackend, defFormat)

}
