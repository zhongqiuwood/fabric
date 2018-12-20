package startnode

import (
	"fmt"
	"github.com/abchain/fabric/core/chaincode"
	"github.com/abchain/fabric/core/config"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/embedded_chaincode"
	"github.com/abchain/fabric/core/peer"
	"github.com/abchain/fabric/events/producer"
	"github.com/abchain/fabric/node"
	api "github.com/abchain/fabric/node/service"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

var (
	logger         = logging.MustGetLogger("engine")
	theNode        *node.NodeEngine
	theDoom        context.CancelFunc
	theEndServices func()

	//an helper for simply waiting while running
	TheGuard func(context.Context)
)

func GetNode() *node.NodeEngine { return theNode }

func PreInitFabricNode(name string) {
	if theNode != nil {
		panic("Doudble call of init")
	}
	theNode = new(node.NodeEngine)
	theNode.Name = name
	theNode.PreInit()

	peer.PeerGlobalParentCtx, theDoom = context.WithCancel(context.Background())
}

func Final() {
	if theEndServices != nil {
		theEndServices()
	}

	db.Stop()
	if theDoom != nil {
		theDoom()
	}

	theNode.FinalRelease()
}

func RunFabricNode() error {
	status, err := theNode.RunAll()
	if err != nil {
		return err
	}

	theEndServices = func() {
		theNode.StopServices(status)
	}

	TheGuard = func(ctx context.Context) {

		for {
			select {
			case <-ctx.Done():
				return
			case srvp := <-status:
				logger.Errorf("server point [%s] fail: %s", srvp.Spec().Address, srvp.Status())
			}
		}
	}

	return nil
}

func SetEndServicesFunc(f func()) {
	if theEndServices == nil {
		theEndServices = f
	} else {
		theEndServices = func() {
			f()
			theEndServices()
		}
	}

}

func InitFabricNode() error {

	if err := theNode.ExecInit(); err != nil {
		return fmt.Errorf("NODE INIT FAILURE: ***** %s *****", err)
	}

	//create node and other infrastructures ... (if no setting, use default peer's server point)
	//chaincode: TODO: support mutiple chaincode platforms
	ccsrv, err := node.CreateServerPoint(config.SubViper("chaincode"))
	if err != nil {
		logger.Infof("Can not create server spec for chaincode: [%s], merge it into peer", err)
		ccsrv = theNode.DefaultPeer().GetServerPoint()
	} else {
		theNode.AddServicePoint(ccsrv)
	}

	userRunsCC := false
	if viper.GetString("chaincode.mode") == chaincode.DevModeUserRunsChaincode {
		userRunsCC = true
	}

	ccplatform := chaincode.NewChaincodeSupport(chaincode.DefaultChain, theNode.Name, ccsrv.Spec(), userRunsCC)
	pb.RegisterChaincodeSupportServer(ccsrv.Server, ccplatform)

	//TODO: now we just launch system chaincode for default ledger
	err = embedded_chaincode.RegisterSysCCs(theNode.DefaultLedger(), ccplatform)
	if err != nil {
		return fmt.Errorf("launch system chaincode fail: %s", err)
	}

	var apisrv, evtsrv node.ServicePoint
	var evtConf *viper.Viper
	//api, also bind the event hub, and "service" configuration in YA-fabric 0.7/0.8 is abandoned
	if viper.IsSet("node.api") {
		if apisrv, err = node.CreateServerPoint(config.SubViper("node.api")); err != nil {
			return fmt.Errorf("Error setting for API service: %s", err)
		}
		theNode.AddServicePoint(apisrv)
		evtsrv = apisrv
		evtConf = config.SubViper("node.api.events")
	} else {
		//for old fashion, we just bind it into deafult peer
		apisrv = theNode.DefaultPeer().GetServerPoint()
		//and respect the event configuration
		if evtsrv, err = node.CreateServerPoint(config.SubViper("peer.validator.events")); err != nil {
			return fmt.Errorf("Error setting for event service: %s", err)
		}
		evtConf = config.SubViper("peer.validator.events")
	}

	pb.RegisterAdminServer(apisrv.Server, api.NewAdminServer())
	pb.RegisterDevopsServer(apisrv.Server, api.NewDevopsServer(theNode))
	pb.RegisterEventsServer(evtsrv.Server, producer.NewEventsServer(
		uint(evtConf.GetInt("buffersize")),
		evtConf.GetInt("timeout")))

	//TODO: openchain should be able to use mutiple peer
	nbif, _ := theNode.DefaultPeer().Peer.GetNeighbour()
	if ocsrv, err := api.NewOpenchainServerWithPeerInfo(nbif); err != nil {
		return fmt.Errorf("Error creating OpenchainServer: %s", err)
	} else {
		pb.RegisterOpenchainServer(apisrv.Server, ocsrv)
	}

	//finally the rest, may be abandoned later
	// if viper.GetBool("rest.enabled") {
	// 	go rest.StartOpenchainRESTServer(serverOpenchain, serverDevops)
	// }

	return nil

}
