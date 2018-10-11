package cred_driver

import (
	_ "github.com/abchain/fabric/core/cred"
	"github.com/op/go-logging"
)

//driver read config and build a custom CredentialCore

var logger = logging.MustGetLogger("cred_driver")
