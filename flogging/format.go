package flogging

//we use this implement for formatting, replace the original one in go-logging
//so it can be easily append on a backend which has been "leveled"
//that is, the formatting become the top-most layer in logging blocks

import (
	"github.com/op/go-logging"
)

type backendFormatter struct {
	logging.LeveledBackend
	detoured logging.Backend
}

// NewBackendFormatter creates a new backend which makes all records that
// passes through it beeing formatted by the specific formatter.
func ApplyFormatter(b logging.LeveledBackend, f logging.Formatter) logging.LeveledBackend {
	fb := logging.NewBackendFormatter(b, f)
	return &backendFormatter{b, fb}
}

// Log implements the Log function required by the Backend interface.
func (bf *backendFormatter) Log(level logging.Level, calldepth int, r *logging.Record) error {
	return bf.detoured.Log(level, calldepth+1, r)
}
