package flogging

import (
	"bufio"
	"errors"
	"github.com/abchain/fabric/core/util"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogBackend utilizes the standard log module.
type FileLogBackend struct {
	Logger     *log.Logger
	file       *os.File
	writer     *bufio.Writer
	flushLevel logging.Level
	syncLevel  logging.Level
	sync.Mutex
}

func NewFileLogBackend(filePath string, prefix string, flag int,
	flushLevel logging.Level, syncLevel logging.Level) (*FileLogBackend, error) {
	var backend *FileLogBackend
	backend = nil

	if flushLevel < syncLevel {
		syncLevel = syncLevel
	}

	fileHandler, err := os.Create(filePath)
	if err != nil {
		return nil, err
	} else {
		w := bufio.NewWriter(fileHandler)

		backend = &FileLogBackend{
			Logger:     log.New(fileHandler, prefix, flag),
			file:       fileHandler,
			writer:     w,
			flushLevel: flushLevel,
			syncLevel:  syncLevel}
	}
	return backend, nil
}

// Log implements the Backend interface.
func (b *FileLogBackend) Log(level logging.Level, calldepth int, rec *logging.Record) error {

	//output inner is thread-safe so we do not need to lock it
	err := b.Logger.Output(calldepth+2, rec.Formatted(calldepth+1))

	if err != nil && level <= b.flushLevel {

		b.Lock()
		defer b.Unlock()

		//notice: leve for flush should be sync first
		err = b.writer.Flush()

		if err == nil && level <= b.syncLevel {
			err = b.file.Sync()
		}

	}

	return err
}

func LoggingFileInit(fpath string) error {

	outputfile := viper.GetString("logging.output.file")
	if outputfile != "" {

		directory := viper.GetString("logging.output.directory")
		if directory != "" {
			directory = util.CanonicalizeFilePath(directory)
			_, err := os.Stat(directory)
			if err == nil {
				fpath = directory
			}
		}

		if fpath == "" {
			return errors.New("No filesystem path is specified but require log-to-file")
		}

		if viper.GetBool("logging.output.postfix") {
			outputfile = outputfile + string(time.Now().Format("Mon Jan 2,2006 15-04-05"))
		}

		flog := filepath.Join(fpath, outputfile)
		flogout, err := NewFileLogBackend(flog, "", 0, logging.WARNING, logging.ERROR)

		if err != nil {
			return err
		}

		formatStr := viper.GetString("logging.output.format")
		if formatStr == "" {
			//we use another format which will overwrite the top-most one
			formatStr = "%{time:15:04:05.000} [%{module}] %{shortfunc} -> %{level:.4s} [%{pid}] %{message}"
		}

		formatterFileLog := logging.NewBackendFormatter(flogout, logging.MustStringFormatter(formatStr))

		//buiding multi-logger from logging package is a little overhead
		//(it build a leveledbackend) but still OK
		//we mixed file output with the default backend ...
		DefaultBackend.Backend = logging.MultiLogger(DefaultBackend.Backend, formatterFileLog)
		loggingLogger.Info("Log will be also output to", flog)
	} else {
		loggingLogger.Info("Do not output log to file")
	}

	return nil

}
