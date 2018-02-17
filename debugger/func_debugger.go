package debugger

//This module enhances debug ability and dump goroutine id

import (
	"bytes"
	"fmt"
	"runtime"
	"github.com/op/go-logging"

	"errors"
	"strconv"
	"sync"
	"runtime/debug"
)

var logger *logging.Logger
var goroutineSpace = []byte("goroutine ")

func Init() {
	logger = logging.MustGetLogger("debugger")
}


func Foo() {}

const (
	EXIT  int = -1
	ENTER int = 0
	INFO  int = 1
	DEBUG int = 2
	WARN  int = 3
	ERROR int = 4
	NOTICE int = 5
)

func Log(level int, format string, args ...interface{}) {
	if logger == nil {
		return
	}

	action := ""
	if level == ENTER {
		action = "[Enter]"
		format = format + " in"
	} else if level == EXIT {
		action = "[Exit]"
		format = format + " out"
	}

	pc, _, _, _ := runtime.Caller(1)
	f := runtime.FuncForPC(pc)

	format = "[gid_" + goidstr() + "] " + action + "" + f.Name() + " :" + format

	switch level {
	case ENTER:
	case EXIT:
	case NOTICE:
		logger.Debugf(format, args...)
	case INFO:
		logger.Infof(format, args...)
	case DEBUG:
		logger.Debugf(format, args...)
	case WARN:
		logger.Warningf(format, args...)
	case ERROR:
		logger.Errorf(format, args...)
	}
}

// dump call stack
func DumpStack() {
	logger.Infof("%s", debug.Stack())
}

// return go routine id
func goidstr() string {
	return strconv.FormatUint(getGoroutineID(), 10)
}

var littleBuf = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 64)
		return &buf
	},
}

func getGoroutineID() uint64 {
	bp := littleBuf.Get().(*[]byte)
	defer littleBuf.Put(bp)
	b := *bp
	b = b[:runtime.Stack(b, false)]
	// Parse the 4707 out of "goroutine 4707 ["
	b = bytes.TrimPrefix(b, goroutineSpace)
	i := bytes.IndexByte(b, ' ')
	if i < 0 {
		panic(fmt.Sprintf("No space found in %q", b))
	}
	b = b[:i]
	n, err := parseUintBytes(b, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse goroutine ID out of %q: %v", b, err))
	}
	return n
}


// parseUintBytes is like strconv.ParseUint, but using a []byte.
func parseUintBytes(s []byte, base int, bitSize int) (n uint64, err error) {
	var cutoff, maxVal uint64

	if bitSize == 0 {
		bitSize = int(strconv.IntSize)
	}

	s0 := s
	switch {
	case len(s) < 1:
		err = strconv.ErrSyntax
		goto Error

	case 2 <= base && base <= 36:
		// valid base; nothing to do

	case base == 0:
		// Look for octal, hex prefix.
		switch {
		case s[0] == '0' && len(s) > 1 && (s[1] == 'x' || s[1] == 'X'):
			base = 16
			s = s[2:]
			if len(s) < 1 {
				err = strconv.ErrSyntax
				goto Error
			}
		case s[0] == '0':
			base = 8
		default:
			base = 10
		}

	default:
		err = errors.New("invalid base " + strconv.Itoa(base))
		goto Error
	}

	n = 0
	cutoff = cutoff64(base)
	maxVal = 1<<uint(bitSize) - 1

	for i := 0; i < len(s); i++ {
		var v byte
		d := s[i]
		switch {
		case '0' <= d && d <= '9':
			v = d - '0'
		case 'a' <= d && d <= 'z':
			v = d - 'a' + 10
		case 'A' <= d && d <= 'Z':
			v = d - 'A' + 10
		default:
			n = 0
			err = strconv.ErrSyntax
			goto Error
		}
		if int(v) >= base {
			n = 0
			err = strconv.ErrSyntax
			goto Error
		}

		if n >= cutoff {
			// n*base overflows
			n = 1<<64 - 1
			err = strconv.ErrRange
			goto Error
		}
		n *= uint64(base)

		n1 := n + uint64(v)
		if n1 < n || n1 > maxVal {
			// n+v overflows
			n = 1<<64 - 1
			err = strconv.ErrRange
			goto Error
		}
		n = n1
	}

	return n, nil

Error:
	return n, &strconv.NumError{Func: "ParseUint", Num: string(s0), Err: err}
}

// Return the first number n such that n*base >= 1<<64.
func cutoff64(base int) uint64 {
	if base < 2 {
		return 0
	}
	return (1<<64-1)/uint64(base) + 1
}