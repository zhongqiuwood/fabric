package node

import (
	"fmt"
	"github.com/abchain/fabric/core/config"
	"github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/cred/driver"
	"github.com/abchain/fabric/core/db"
	gossip_stub "github.com/abchain/fabric/core/gossip/stub"
	sync_stub "github.com/abchain/fabric/core/statesync/stub"

	"github.com/abchain/fabric/core/gossip/txnetwork"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/ledger/genesis"
	"github.com/abchain/fabric/core/peer"
	"github.com/spf13/viper"
)

func (ne *NodeEngine) GenCredDriver() *cred_driver.Credentials_PeerDriver {
	drv := cred_driver.Credentials_PeerCredBase{ne.Cred.Peer, ne.Cred.Tx}
	return &cred_driver.Credentials_PeerDriver{drv.Clone(), nil, ne.Endorsers}
}

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

func (pe *PeerEngine) Init(vp *viper.Viper, node *NodeEngine, tag string) error {

	var err error
	cfg := new(config.ServerSpec)
	if err = cfg.Init(vp); err != nil {
		return fmt.Errorf("Init config fail", err)
	}

	credrv := node.GenCredDriver()
	if err = credrv.Drive(vp); err != nil {
		return fmt.Errorf("Init credential fail", err)
	}
	pe.defaultEndorser = credrv.TxEndorserDef

	isValidator := viper.GetBool("validator.enable")
	if isValidator {
		logger.Info("Peer [%s] is set to be validator", tag)
	}

	var peercfg *peer.PeerConfig
	if peercfg, err = peer.NewPeerConfig(isValidator, vp); err != nil {
		return fmt.Errorf("Init peer config fail", err)
	}

	if pe.Peer, err = peer.CreateNewPeer(credrv.PeerValidator, peercfg); err != nil {
		return fmt.Errorf("Init peer fail", err)
	}

	srvPoint := new(servicePoint)
	if err = srvPoint.InitWithConfig(cfg); err != nil {
		return fmt.Errorf("Init serverpoint fail", err)
	}
	node.srvPoints = append(node.srvPoints, srvPoint)

	//init gossip network
	if gstub := gossip_stub.InitGossipStub(pe.Peer, srvPoint.Server); gstub == nil {
		return fmt.Errorf("Can not create gossip server", err)
	} else {
		pe.TxNetworkEntry = txnetwork.GetNetworkEntry(gstub)
	}

	//build tx validator
	networkTxCred := []credentials.TxHandlerFactory{credrv.TxValidator}
	if hv, ok := node.peerTxHandlers[tag]; ok {
		networkTxCred = append(networkTxCred, hv)
	}
	networkTxCred = append(networkTxCred, node.globalTxHandlers...)
	pe.TxNetworkEntry.InitCred(credentials.MutipleTxHandler(networkTxCred...))

	//TODO: create and init sync entry
	_ = sync_stub.InitStateSyncStub(pe.Peer, "ledgerName", srvPoint.Server)

	//test ledger configuration
	if useledger := vp.GetString("ledger"); useledger == "" || useledger == "default" {
		//use default ledger, do nothing
	} else if useledger == "sole" {
		//create a new ledger tagged by the peer, add it to node
		l, ok := node.Ledgers[useledger]
		if !ok {
			tagdb, err := db.StartDB(useledger, vp.Sub("db"))
			if err != nil {
				return fmt.Errorf("Try to create db %s fail: %s", useledger, err)
			}
			l, err = ledger.GetNewLedger(tagdb)
			if err != nil {
				return fmt.Errorf("Try to create ledger %s fail: %s", useledger, err)
			}
			err = genesis.MakeGenesisForLedger(l, "", nil)
			if err != nil {
				return fmt.Errorf("Try to create genesis block for ledger %s fail: %s", useledger, err)
			}

			node.Ledgers[useledger] = l
			logger.Info("Create default ledger [%s] for peer [%s]", useledger, tag)
		}
		pe.TxNetworkEntry.InitLedger(l)

	} else {
		//select a ledger created before (by node or other peer), throw error if not found
		l, ok := node.Ledgers[useledger]
		if !ok {
			return fmt.Errorf("Could not find specified ledger [%s]", useledger)
		}
		pe.TxNetworkEntry.InitLedger(l)
	}

	return nil
}
