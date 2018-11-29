package cred_driver

import (
	cred "github.com/abchain/fabric/core/cred"
	"github.com/spf13/viper"
)

//driver read config and build a custom CredentialCore

type Credentials_PeerCredBase struct {
	PeerValidator cred.PeerCreds
	TxValidator   cred.TxHandlerFactory
}

type Credentials_PeerDriver struct {
	*Credentials_PeerCredBase
	TxEndorserDef cred.TxEndorserFactory

	//if config file specified a "custom" endorser and it can be obtained
	//from this field, TxEndorserDef will be set to the corresponding one
	SuppliedEndorser map[string]cred.TxEndorserFactory
}

func (drv *Credentials_PeerCredBase) Clone() *Credentials_PeerCredBase {
	return nil
}

/*
	configure the peer's credential from config files, if suitable content
	has been found, the corresponding item in driver struct is set and the
	fields can not be configured will be untouched

	when Credentials_PeerCredBase is empty, new Credentials_PeerCredBase is
	created, or if it has been set, driver will try to merge the new content
	into it

	it configue the per-peer creds while a endorser may be also derived from
	the peer credential
*/
func (drv *Credentials_PeerDriver) Drive(vp *viper.Viper) error {
	return nil
}
