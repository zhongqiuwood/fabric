package gossip

import (
	"github.com/spf13/viper"
	"sync"
)

var cache sync.Once
var disablePeerPolicy = false

func cacheConfiguration() {

	disablePeerPolicy = !viper.GetBool("peer.txnetwork.policy.enable")
	if disablePeerPolicy {
		logger.Info("Gossip network has disabled peer policy")
	}
}
