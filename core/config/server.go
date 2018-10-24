package config

import (
	"fmt"
	"github.com/spf13/viper"
)

//a server's specification: including the listen address,
//declaimed address (which a external accessor should use),
//TLS scheme etc...
type ServerSpec struct {
	Address      string
	ExternalAddr string
	tlsSpec
}

//Init the spec from viper
func (s *ServerSpec) Init(vp *viper.Viper) error {

	s.ExternalAddr = vp.GetString("address")
	if s.ExternalAddr == "" {
		return fmt.Errorf("can not find address configuration")
	}

	s.Address = vp.GetString("listenAddress")
	if s.Address == "" {
		s.Address = s.ExternalAddr
	}
	logger.Debugf("Set serverspec's address as [%s %s]", s.Address, s.ExternalAddr)

	if vp.IsSet("tls") {
		logger.Debugf("Read tls configuration for serverspec")
		s.tlsSpec.Init(vp.Sub("tls"))
	}

	return nil
}

func (s *ServerSpec) GetClient() *ClientSpec {

	ret := new(ClientSpec)
	ret.Address = s.ExternalAddr
	ret.tlsClientSpec = s.tlsClientSpec
	return ret
}
