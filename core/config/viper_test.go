package config_test

import (
	"github.com/abchain/fabric/core/config"
	"github.com/spf13/viper"
	"testing"
)

func TestViperSetting(t *testing.T) {

	conf := config.SetupTestConf{"CORE", "chaincodetest", "../chaincode"}
	conf.Setup()

	originalSetting := viper.GetStringMap("peer")
	before := len(originalSetting)
	t.Log(originalSetting)
	viper.Set("peer.fileSystemPath", "abcdef/ghijk")
	after := len(viper.Sub("peer").AllSettings())

	if viper.GetString("peer.version") == "" {
		t.Fatal("Unexpected viper behavior 1")
	}

	if before == after {
		t.Errorf("Viper seems have repaired its sub, we could use their implement")
	}
	t.Logf("Viper will use the overwrite layer in sub method so the item is different: [%d vs %d]", before, after)

	after = len(config.SubViper("peer").AllSettings())
	t.Log(config.SubViper("peer").AllSettings())
	if before != after {
		t.Errorf("Wrong items count: %d vs %d", before, after)
	}

	config.CacheViper()
	after = len(config.SubViper("peer").AllSettings())
	if before != after {
		t.Errorf("Wrong items count after making cache: %d vs %d", before, after)
	}
}
