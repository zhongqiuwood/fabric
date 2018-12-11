package node

import (
	"fmt"
	"github.com/abchain/fabric/core/config"
	"github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/cred/driver"
	"github.com/abchain/fabric/core/db"
	gossip_stub "github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/events/litekfk"
	//"github.com/abchain/fabric/core/statesync/stub"

	"github.com/abchain/fabric/core/gossip/txnetwork"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/ledger/genesis"
	"github.com/abchain/fabric/core/peer"
	"github.com/spf13/viper"
)

func addLedger(vp *viper.Viper, tag string) (*ledger.Ledger, error) {

	tagdb, err := db.StartDB(tag, config.SubViper("db", vp))
	if err != nil {
		return nil, fmt.Errorf("Try to create db fail: %s", err)
	}

	checkonly := vp.GetBool("notUpgrade")
	err = ledger.UpgradeLedger(tagdb, checkonly)
	if err != nil {
		return nil, fmt.Errorf("Upgrade ledger fail: %s", err)
	}

	l, err := ledger.GetNewLedger(tagdb, ledger.NewLedgerConfig(vp))
	if err != nil {
		return nil, fmt.Errorf("Try to create ledger fail: %s", err)
	}
	err = genesis.MakeGenesisForLedger(l, "", nil)
	if err != nil {
		return nil, fmt.Errorf("Try to create genesis block for ledger fail: %s", err)
	}

	return l, nil
}

func (ne *NodeEngine) GenCredDriver() *cred_driver.Credentials_PeerDriver {
	drv := cred_driver.Credentials_PeerCredBase{ne.Cred.Peer, ne.Cred.Tx}
	return &cred_driver.Credentials_PeerDriver{drv.Clone(), nil, ne.Endorsers}
}

//preinit phase simply read all peer's name in the config and create them,
//user can make settings on node and peer which will be respected in the
//Init process
func (ne *NodeEngine) PreInit() {

	ne.Ledgers = make(map[string]*ledger.Ledger)
	ne.Peers = make(map[string]*PeerEngine)
	ne.Endorsers = make(map[string]credentials.TxEndorserFactory)
	ne.Cred.ccSpecValidator = NewCCSpecValidator(nil)
	ne.TxTopic = make(map[string]litekfk.Topic)
	ne.TxTopicNameHandler = nullTransformer

	//occupy ledger's objects position
	ledgerTags := viper.GetStringSlice("node.ledgers")
	for _, tag := range ledgerTags {
		ne.Ledgers[tag] = nil
	}

	//occupy peer's objects position
	peerTags := viper.GetStringSlice("node.peers")
	for _, tag := range peerTags {
		p := new(PeerEngine)
		p.PreInit(ne)
		ne.Peers[tag] = p
	}
}

//ne will fully respect an old-fashion (fabric 0.6) config file
func (ne *NodeEngine) ExecInit() error {

	fpath := viper.GetString("node.fileSystemPath")
	//if not set, use the old fashion one (peer.fileSystemPath)
	if fpath != "" {
		db.InitDBPath(fpath)
	}

	//init ledgers
	var defaultTag string
	for tag, l := range ne.Ledgers {

		vp := config.SubViper("ledgers." + tag)
		if vp.GetBool("default") {
			if defaultTag != "" {
				logger.Warningf("Duplicated default tag found [%s vs %s], later will be used", defaultTag, tag)
			}
			defaultTag = tag
		}

		//respect the pre-set ledger
		if l != nil {
			logger.Infof("Ledger %s has been set before init", tag)
			continue
		}

		var err error
		if l, err = addLedger(vp, tag); err != nil {
			return fmt.Errorf("Init ledger %s in node fail: %s", tag, err)
		}

		ne.Ledgers[tag] = l
		logger.Info("Init ledger:", tag)
	}

	//select default ledger, if not, use first one, or just create one from peer setting
	if len(ne.Ledgers) > 0 {

		if defaultTag == "" {
			for k, _ := range ne.Ledgers {
				defaultTag = k
				break
			}
		}

		logger.Info("Default ledger is:", defaultTag)
		ledger.SetDefaultLedger(ne.Ledgers[defaultTag])
		ne.Ledgers[""] = ne.Ledgers[defaultTag]
	} else {
		//start default db
		db.Start()
		if l, err := ledger.GetLedger(); err != nil {
			return fmt.Errorf("Init default ledger fail: %s", err)
		} else {
			ne.Ledgers[""] = l
			logger.Warningf("No ledger created, use old-fashion default one")
		}
	}

	//create base credentials
	creddrv := new(cred_driver.Credentials_PeerDriver)
	if err := creddrv.Drive(config.SubViper("node")); err == nil {
		ne.Cred.Peer = creddrv.PeerValidator
		ne.Cred.Tx = creddrv.TxValidator
	} else {
		logger.Info("No credentials availiable in node:", err)
	}
	//TODO: create endorsers

	//add a default txtopic
	topicCfg := litekfk.NewDefaultConfig()
	ne.TxTopic[""] = litekfk.CreateTopic(topicCfg)

	//init peers
	for tag, p := range ne.Peers {

		vp := viper.Sub(tag)
		if vp.GetBool("default") {
			if defaultTag != "" {
				logger.Warningf("Duplicated default tag found [%s vs %s], later will be used", defaultTag, tag)
			}
			defaultTag = tag
		}

		if err := p.Init(vp, ne, tag); err != nil {
			return fmt.Errorf("Create peer %s fail: %s", tag, err)
		}
		logger.Info("Create peer:", tag)
	}

	if len(ne.Peers) > 0 {
		if defaultTag == "" {
			for k, _ := range ne.Peers {
				defaultTag = k
				break
			}
		}

		logger.Info("Default peer is:", defaultTag)
		ne.Peers[""] = ne.Peers[defaultTag]
	} else {
		//try to create peer in "peer" block
		vp := config.SubViper("peer")
		p := new(PeerEngine)
		if err := p.Init(vp, ne, ""); err != nil {
			return fmt.Errorf("Create default peer fail: %s", err)
		}
		ne.Peers[""] = p
		logger.Info("Create old-fashion, default peer")
	}

	return nil

}

//wrap two step in one
func (ne *NodeEngine) Init() error {
	ne.PreInit()
	return ne.ExecInit()
}

func (pe *PeerEngine) PreInit(node *NodeEngine) {
	pe.CredOpts.ccSpecValidator = NewCCSpecValidator(node.Cred.ccSpecValidator)
}

func (pe *PeerEngine) Init(vp *viper.Viper, node *NodeEngine, tag string) error {

	var err error
	credrv := node.GenCredDriver()
	if err = credrv.Drive(vp); err != nil {
		if node.Options.EnforceSec {
			return fmt.Errorf("Init credential fail: %s", err)
		} else {
			logger.Warningf("Drive cred fail: %s, no security is available", err)
		}

	}
	pe.defaultEndorser = credrv.TxEndorserDef

	pe.srvPoint = new(servicePoint)
	if err = pe.srvPoint.Init(vp); err != nil {
		return fmt.Errorf("Init serverpoint fail: %s", err)
	}
	node.srvPoints = append(node.srvPoints, pe.srvPoint)

	isValidator := vp.GetBool("validator.enabled")
	if isValidator {
		logger.Infof("Peer [%s] is set to be validator", tag)
	}

	var peercfg *peer.PeerConfig
	if peercfg, err = peer.NewPeerConfig(isValidator, vp, pe.srvPoint.spec); err != nil {
		return fmt.Errorf("Init peer config fail: %s", err)
	}

	if pe.Peer, err = peer.CreateNewPeer(credrv.PeerValidator, peercfg); err != nil {
		return fmt.Errorf("Init peer fail: %s", err)
	}

	//init gossip network
	if gstub := gossip_stub.InitGossipStub(pe.Peer, pe.srvPoint.Server); gstub == nil {
		return fmt.Errorf("Can not create gossip server: %s", err)
	} else {
		pe.TxNetworkEntry = txnetwork.GetNetworkEntry(gstub)
	}

	//build tx validator
	//see tech note for the building block for all validators
	networkTxCred := []credentials.TxHandlerFactory{}
	if credrv.TxValidator != nil {
		networkTxCred = append(networkTxCred, credrv.TxValidator)
		networkTxCred = append(networkTxCred, credentials.EscalateToTxHandler(pe.CredOpts.ccSpecValidator))
	}
	networkTxCred = append(networkTxCred, node.Cred.Customs...)
	networkTxCred = append(networkTxCred, pe.CredOpts.Customs...)
	networkTxCred = append(networkTxCred, &recordValidator{node.TxTopic, node.TxTopicNameHandler, pe})

	//TODO: create and init sync entry
	//_ = sync_stub.InitStateSyncStub(pe.Peer, "ledgerName", srvPoint.Server)
	var peerLedger *ledger.Ledger

	//test ledger configuration
	if useledger := vp.GetString("ledger"); useledger == "" || useledger == "default" {
		//use default ledger, do nothing
		var err error
		peerLedger, err = ledger.GetLedger()
		if err != nil {
			return fmt.Errorf("Could not get default ledger: %s", err)
		}

	} else if useledger == "sole" {
		//create a new ledger tagged by the peer, add it to node
		var err error
		l, exist := node.Ledgers[useledger]
		if !exist {
			l, err = addLedger(vp, useledger)
			if err != nil {
				return fmt.Errorf("Create peer's default ledger [%s] fail: %s", useledger, err)
			}
			node.Ledgers[useledger] = l
			logger.Info("Create default ledger [%s] for peer [%s]", useledger, tag)
		}
		peerLedger = l

	} else {
		//select a ledger created before (by node or other peer), throw error if not found
		l, ok := node.Ledgers[useledger]
		if !ok {
			return fmt.Errorf("Could not find specified ledger [%s]", useledger)
		}
		peerLedger = l

	}

	if !pe.CredOpts.NoPooling {
		networkTxCred = append(networkTxCred, credentials.EscalateToTxHandler(txPoolValidator{peerLedger}))
	}

	pe.TxNetworkEntry.InitLedger(peerLedger)
	pe.TxNetworkEntry.InitCred(credentials.MutipleTxHandler(networkTxCred...))

	return nil
}
