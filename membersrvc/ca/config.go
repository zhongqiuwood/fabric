package ca

// import (
// 	"github.com/spf13/viper"
// )

type ServerConfig struct {
	rootPath       string
	caDir          string
}

type PKIConfig struct {
	caOrganization string
	caCountry      string
}
