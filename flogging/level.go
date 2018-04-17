package flogging

import (
	"github.com/op/go-logging"
)

// We mimic the level.go in op-logging but use a light-weight implements
// (no default formatters)

type moduleLeveled struct {
	levels map[string]logging.Level
	logging.Backend
}

// duplicate from the default one and replace with another backend
func DuplicateLevelBackend(backend logging.Backend) logging.LeveledBackend {

	return &moduleLeveled{
		levels:  DefaultBackend.levels,
		Backend: backend,
	}
}

// GetLevel returns the log level for the given module.
func (l *moduleLeveled) GetLevel(module string) logging.Level {
	level, exists := l.levels[module]
	if !exists {
		level, exists = l.levels[""]
		// no configuration exists, default to debug
		if !exists {
			level = logging.DEBUG
		}
	}
	return level
}

// SetLevel sets the log level for the given module.
func (l *moduleLeveled) SetLevel(level logging.Level, module string) {
	l.levels[module] = level
}

// IsEnabledFor will return true if logging is enabled for the given module.
func (l *moduleLeveled) IsEnabledFor(level logging.Level, module string) bool {
	return level <= l.GetLevel(module)
}
