package config

import (
	"github.com/abchain/fabric/core/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc/credentials"
)

type tlsClientSpec struct {
	EnableTLS       bool
	TLSRootCertFile string
	TLSHostOverride string
}

type tlsSpec struct {
	tlsClientSpec
	TLSKeyFile  string
	TLSCertFile string
}

func (ts *tlsClientSpec) Init(vp *viper.Viper) {

	ts.EnableTLS = vp.GetBool("enabled")
	if !ts.EnableTLS {
		return
	}

	//we also recognize the file scheme
	if vp.IsSet("file") {
		ts.TLSRootCertFile = vp.GetString("file.rootcert")
	}

	ts.TLSHostOverride = vp.GetString("serverhostoverride")
}

func (ts *tlsSpec) Init(vp *viper.Viper) {

	ts.tlsClientSpec.Init(vp)
	if !ts.EnableTLS {
		return
	}

	//we recognize the file scheme
	if vp.IsSet("file") {
		ts.TLSCertFile = vp.GetString("file.cert")
		ts.TLSKeyFile = vp.GetString("file.key")
		//we suppose the certfile is self-signatured cert for supporting a client implement
		if ts.TLSRootCertFile == "" {
			ts.TLSRootCertFile = ts.TLSCertFile
		}
	}
}

//we also make an implement to generating the grpc credential
func (ts *tlsSpec) GetServerTLSOptions() (credentials.TransportCredentials, error) {

	return credentials.NewServerTLSFromFile(
		util.CanonicalizeFilePath(ts.TLSCertFile),
		util.CanonicalizeFilePath(ts.TLSKeyFile))
}

func (ts *tlsClientSpec) GetClientTLSOptions() (credentials.TransportCredentials, error) {

	return credentials.NewClientTLSFromFile(
		util.CanonicalizeFilePath(ts.TLSRootCertFile),
		ts.TLSHostOverride)
}
