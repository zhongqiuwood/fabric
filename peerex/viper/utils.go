package viper

import (
	"github.com/spf13/viper"
	"strings"
)

//we can set viper which fabric peer is used
func InitPeerViper(envprefix string, filename string, configPath ...string) error {

	viper.SetEnvPrefix(envprefix)
	viper.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	for _, c := range configPath {
		viper.AddConfigPath(c)
	}

	viper.SetConfigName(filename) // Name of config file (without extension)

	return viper.ReadInConfig() // Find and read the config file

}

func SetViperDefault(key string, value interface{}) {
	viper.SetDefault(key, value)
}

//or create another one for ourself
func New() *viper.Viper {

	return viper.New()

}
