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
	"fmt"
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/gossip/txnetwork"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/peer"
	"github.com/abchain/fabric/events/litekfk"
	"github.com/op/go-logging"
	"golang.org/x/net/context"
)

var (
	logger = logging.MustGetLogger("engine")
)

type PeerEngine struct {
	*txnetwork.TxNetworkEntry
	peer.Peer

	defaultEndorser cred.TxEndorserFactory
	defaultAttr     []string
	srvPoint        *servicePoint

	//run-time vars
	lastID string

	lastCache  txPoint
	exitNotify chan interface{}
	runStatus  error
	stopFunc   context.CancelFunc
}

/*
	node engine intregated mutiple PeerEngine, ledgers and credentials, system should access
	ledger here
*/
type NodeEngine struct {
	Name      string
	Ledgers   map[string]*ledger.Ledger
	Peers     map[string]*PeerEngine
	Endorsers map[string]cred.TxEndorserFactory
	Cred      struct {
		Peer cred.PeerCreds
		Tx   cred.TxHandlerFactory
	}
	//all the received transactions can be read out from different topic by its chaincode name,
	//according to the configuration in transation filter
	TxTopic map[string]litekfk.Topic

	srvPoints []*servicePoint

	//additional tx hooks
	globalTxHandlers []cred.TxHandlerFactory
	peerTxHandlers   map[string]cred.TxHandlerFactory
}

func (ne *NodeEngine) DefaultLedger() *ledger.Ledger {
	return ne.Ledgers[""]
}

func (ne *NodeEngine) DefaultPeer() *PeerEngine {
	return ne.Peers[""]
}

func (ne *NodeEngine) SelectEndorser(name string) (cred.TxEndorserFactory, error) {

	if ne.Endorsers != nil {
		opt, ok := ne.Endorsers[name]
		if ok {
			return opt, nil
		}
	}

	//TODO: we can create external type of endorser if being configured to
	return nil, fmt.Errorf("Specified endorser %s is not exist", name)

}

func (ne *NodeEngine) SelectLedger(name string) (*ledger.Ledger, error) {

	if ne.Ledgers != nil {
		l, ok := ne.Ledgers[name]
		if ok {
			return l, nil
		}
	}

	//TODO: we can create external type of endorser if being configured to
	return nil, fmt.Errorf("Specified ledger %s is not exist", name)

}

func (ne *NodeEngine) AddServicePoint(s ServicePoint) {
	ne.srvPoints = append(ne.srvPoints, s.servicePoint)
}
