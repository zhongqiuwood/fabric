package node

import (
	"github.com/abchain/fabric/core/config"
	"github.com/spf13/viper"
	"io/ioutil"
	"testing"
)

func buildLegacyNode(t *testing.T) *NodeEngine {

	cf := config.SetupTestConf{"FABRIC", "conf_legacy_test", ""}
	cf.Setup()

	tempDir, err := ioutil.TempDir("", "fabric-db-test")
	if err != nil {
		t.Fatal("tempfile fail", err)
	}

	viper.Set("peer.fileSystemPath", tempDir)
	config.CacheViper()

	ne := new(NodeEngine)
	ne.Name = "test"
	if err := ne.Init(); err != nil {
		t.Fatal(err)
	}

	return ne

}

func TestTxNetwork(t *testing.T) {
}
