package node

import (
	_ "github.com/abchain/fabric/core/config"
	"github.com/spf13/viper"
)

// entry, ok := txnetwork.GetNetworkEntry(stub)
// if !ok {
// 	return nil, fmt.Errorf("Corresponding entry of given gossip network is not found: [%v]", stub)
// }

// var err error
// if endorser == nil {
// 	err = entry.ResetPeerSimple(util.GenerateBytesUUID())
// } else {
// 	err = entry.ResetPeer(endorser)
// }

// if err != nil{
// 	return nil, fmt.Errorf("Corresponding entry of given gossip network is not found: [%v]", stub)
// }

func (pe *PeerEngine) Init(vp *viper.Viper) {

}
