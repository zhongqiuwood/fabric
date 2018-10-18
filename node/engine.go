package node

/*

	YA-fabric: Peer Engine:

	This is an aggregation of required features for a system-chaincode working
	for its purpose (in most case for consensus), a peer Engine include:

	* A module implement for acting as a peer in the P2P network
	* A complete txnetwork and expose its entry
	* A ledger for tracing the state and history of txnetwork
	* A state syncing module
	* Credential module which is effective in tx handling and broadcasting

*/

import (
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/gossip/txnetwork"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/peer"
)

type PeerEngine struct {
	*cred.Credentials
	*txnetwork.TxNetworkEntry
	peer.Peer

	//don't access ledger from PeerEngine, visit it in NodeEngine instead
	ledger   *ledger.Ledger
	srvPoint *servicePoint
}

/*
	node engine intregated mutiple PeerEngine and ledgers, system should access
	ledger here
*/
type NodeEngine struct {
	Ledgers map[string]*ledger.Ledger
	Peers   map[string]*PeerEngine
}

func (ne *NodeEngine) DefaultLedger() *ledger.Ledger {
	return ne.Ledgers[""]
}
