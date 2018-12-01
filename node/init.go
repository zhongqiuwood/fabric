package node

import (
	"fmt"
	"github.com/abchain/fabric/core/config"
	"github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/cred/driver"
	"github.com/abchain/fabric/core/db"
	gossip_stub "github.com/abchain/fabric/core/gossip/stub"
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

//ne will fully respect an old-fashion (fabric 0.6) config file
func (ne *NodeEngine) Init() error {

	fpath := viper.GetString("node.fileSystemPath")
	//if not set, use the old fashion one (peer.fileSystemPath)
	if fpath != "" {
		db.InitDBPath(fpath)
	}

	//create ledgers
	ledgerTags := viper.GetStringSlice("node.ledgers")
	var defaultTag string
	for _, tag := range ledgerTags {

		vp := viper.Sub("ledgers." + tag)
		if vp.GetBool("default") {
			if defaultTag != "" {
				logger.Warningf("Duplicated default tag found [%s vs %s], later will be used", defaultTag, tag)
			}
			defaultTag = tag
		}

		if _, err := ne.addLedger(vp, tag); err != nil {
			return fmt.Errorf("Init ledger %s in node fail: %s", tag, err)
		}
		logger.Info("Init ledger %s", tag)
	}

	//select default ledger, if not, use first one, or just create one from peer setting

	//create peers
	peerTags := viper.GetStringSlice("node.peers")

	//other infrastructures ...

	//if we have only default peer, use its server point

}

func (ne *NodeEngine) addLedger(vp *viper.Viper, tag string) (*ledger.Ledger, error) {

	if l, ok := ne.Ledgers[tag]; ok {
		return l, nil
	}

	tagdb, err := db.StartDB(tag, vp.Sub("db"))
	if err != nil {
		return nil, fmt.Errorf("Try to create db fail: %s", err)
	}
	l, err := ledger.GetNewLedger(tagdb)
	if err != nil {
		return nil, fmt.Errorf("Try to create ledger fail: %s", err)
	}
	err = genesis.MakeGenesisForLedger(l, "", nil)
	if err != nil {
		return nil, fmt.Errorf("Try to create genesis block for ledger fail: %s", err)
	}

	ne.Ledgers[tag] = l
	return l, nil
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

	//test ledger configuration
	if useledger := vp.GetString("ledger"); useledger == "" || useledger == "default" {
		//use default ledger, do nothing
	} else if useledger == "sole" {
		//create a new ledger tagged by the peer, add it to node
		l, err := node.addLedger(vp, useledger)
		if err != nil {
			return fmt.Errorf("Create peer's default ledger [%s] fail: %s", useledger, err)
		}
		logger.Info("Create default ledger [%s] for peer [%s]", useledger, tag)
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
