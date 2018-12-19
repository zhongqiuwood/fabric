package legacynode

import (
	"github.com/abchain/fabric/consensus/helper"
	"github.com/abchain/fabric/node"
)

func startLegacyMode(thenode *node.NodeEngine) error {

	defPeer := thenode.DefaultPeer()

	helper.GetEngine(defPeer.Peer, defPeer.StateSyncStub)

	return nil
}
