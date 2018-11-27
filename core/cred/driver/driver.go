package cred_driver

import (
	cred "github.com/abchain/fabric/core/cred"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

//driver read config and build a custom CredentialCore

var logger = logging.MustGetLogger("cred_driver")

//try to obtain mutiple endorser's configuration from config files
func DriveEndorsers(vp *viper.Viper) (map[string]cred.TxEndorserFactory, error) {
	return nil, nil
}

//try to obtain confidential's configuration from config files
func DriveConfidentials(vp *viper.Viper) (cred.TxConfidentialityHandler, error) {
	return nil, nil
}
