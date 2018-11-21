package config_test

import (
	"github.com/abchain/fabric/core/config"
	"github.com/spf13/viper"
	"testing"
)

func TestViperSetting(t *testing.T) {

	conf := config.SetupTestConf{"CORE", "chaincodetest", "../chaincode"}
	conf.Setup()

	before := len(viper.GetStringMapString("peer"))
	viper.Set("peer.fileSystemPath", "abcdef/ghijk")
	after := len(viper.GetStringMapString("peer"))

	if viper.GetString("peer.version") == "" {
		t.Fatal("Unexpected viper behavior 1")
	}

	if before != after {
		t.Fatal("Unexpected viper behavior 2:", before, after)
	}
}
