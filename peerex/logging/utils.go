package logging

import (
	"github.com/abchain/fabric/flogging"
	"github.com/op/go-logging"
)

func InitLogger(module string) *logging.Logger {
	return logging.MustGetLogger(module)
}

func SetLogFormat(format string) error {

	f, err := logging.NewStringFormatter(format)
	if err != nil {
		return err
	}

	logging.DefaultFormatter = f

	return nil
}

func SetLogLevel(defaultrole string) {

	if defaultrole == "" {
		defaultrole = "client"
	}

	flogging.LoggingInit(defaultrole)

}
