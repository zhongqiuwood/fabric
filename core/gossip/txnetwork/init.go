package txnetwork

import (
	_ "github.com/abchain/fabric/core/gossip"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("gossip_cat")

const (

	//set some weighting on scoring the peer ...
	cat_hottx_one_merge_weight = uint(10)
)

func InitGossipCatalogs() {

}
