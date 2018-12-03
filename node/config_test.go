package node

import (
	"github.com/abchain/fabric/core/config"
	"github.com/spf13/viper"
	"io/ioutil"
	"testing"
)

func TestLegacyInit(t *testing.T) {

	cf := config.SetupTestConf{"FABRIC", "conf_legacy_test", ""}
	cf.Setup()

	tempDir, err := ioutil.TempDir("", "fabric-db-test")
	if err != nil {
		t.Fatal("tempfile fail", err)
	}

	viper.Set("peer.fileSystemPath", tempDir)

	ne := new(NodeEngine)
	ne.Name = "test"
	if err := ne.Init(); err != nil {
		t.Fatal(err)
	}
}

func TestInit(t *testing.T) {

	cf := config.SetupTestConf{"FABRIC", "conf_test", ""}
	cf.Setup()

	tempDir, err := ioutil.TempDir("", "fabric-db-test")
	if err != nil {
		t.Fatal("tempfile fail", err)
	}

	viper.Set("node.fileSystemPath", tempDir)

	ne := new(NodeEngine)
	ne.Name = "test"
	if err := ne.Init(); err != nil {
		t.Fatal(err)
	}
}
