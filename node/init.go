package node

import (
	"fmt"
	"github.com/abchain/fabric/core/config"
	"github.com/abchain/fabric/core/cred/driver"
	gossip_stub "github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/core/gossip/txnetwork"
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

	pe.srvPoint = new(servicePoint)
	if err = pe.srvPoint.InitWithConfig(cfg); err != nil {
		return fmt.Errorf("Init serverpoint fail", err)
	}

	if gstub := gossip_stub.InitGossipStub(pe.Peer, pe.srvPoint.Server); gstub == nil {
		return fmt.Errorf("Can not create gossip server", err)
	} else {
		pe.TxNetworkEntry = txnework.GetNetworkEntry(gstub)
	}

	//TODO: create sync entry

	return nil
}
