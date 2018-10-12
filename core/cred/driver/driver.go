package cred_driver

import (
	cred "github.com/abchain/fabric/core/cred"
	"github.com/op/go-logging"
)

//driver read config and build a custom CredentialCore

var logger = logging.MustGetLogger("cred_driver")

func GetCredentials() *cred.Credentials {
	return nil
}

/*
	set the credential singleton from config files:
	None: the credential core including nothing
	Custom: indicate the credential core is customed outside of the default config routine
	(this function), configing will check if the required feature is available
	Default: build a credential core from specified certificate files
	Agent: use the agent mode in 0.6

	Client side (endorser) and Server side (validator) can be configured respectively
*/
func ConfigCommonCredentials() error {
	return nil
}
