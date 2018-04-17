package logging

import (
	"bufio"
	flog "github.com/abchain/fabric/flogging"
	"github.com/op/go-logging"
	"io"
	"log"
	"os"
	"sync"
)

var (
	defBackend logging.LeveledBackend
)

func applyBackends(f logging.Formatter, b logging.Backend) logging.LeveledBackend {
	fb := logging.NewBackendFormatter(backend, format)
	return logging.AddModuleLevel(fb)
}

func InitLogger(module string) *logging.Logger {

	logger := logging.MustGetLogger(module)
	logger.SetBackend(flog.DefaultBackend)

	return logger
}

func SetLogFormat(format string) error {

	f, err := logging.NewStringFormatter(format)
	if err != nil {
		return err
	}

	if defBackend == nil {
		defBackend = flog.DefaultBackend
	}

	defBackend = applyBackends()
	DefaultFormatter = f

	return nil
}

//deprecated, do nothing
func SetBackend(w io.Writer, prefix string, flag int) {

}

func WrapBackend(w io.Writer, prefix string, flag int) logging.Backend {
	return logging.NewLogBackend(w, prefix, flag)
}

// NewLogBackend creates a new LogBackend.
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

// LogBackend utilizes the standard log module.
type FileLogBackend struct {
	Logger     *log.Logger
	file       *os.File
	writer     *bufio.Writer
	flushLevel logging.Level
	syncLevel  logging.Level
	sync.Mutex
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
