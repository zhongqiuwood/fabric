package txnetwork

import (
	"fmt"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/gossip/stub"
	pb "github.com/abchain/fabric/protos"
	proto "github.com/golang/protobuf/proto"
)

const (
	syncTxCatName = "synctx"
)

type txsyncCat struct {
	policy   gossip.CatalogPolicies
	tasklist []string
}

func init() {
	stub.RegisterCat = append(stub.RegisterCat, initTxSync)
}

func initTxSync(stub *gossip.GossipStub) {

	policy := gossip.NewCatalogPolicyDefault()
	policy.SetPullOnly()

	peerG := new(peersGlobal)
	peerG.network = getTxNetwork(stub)
	peerG.txNetworkPeers = peerG.network.peers

	selfstatus := model.NewScuttlebuttStatus(peerG)
	m := model.NewGossipModel(selfstatus)
	setself := func(newID string, state *pb.PeerTxState) {
		m.Lock()
		defer m.Unlock()
		selfstatus.SetSelfPeer(newID, &peerStatus{state})
		logger.Infof("TXPeers cat reset self peer to %s", newID)
	}

	//use extended mode of scuttlebutt scheme, see code and wiki
	selfstatus.Extended = true
	if selfs, id := peerG.QuerySelf(); selfs != nil {
		setself(id, selfs)
	}

	h := gossip.NewCatalogHandlerImpl(stub.GetSStub(),
		stub.GetStubContext(), globalcat, m)
	stub.AddCatalogHandler(h)
	stub.SubScribeNewPeerNotify(newPeerNotify{h})
	peerG.network.RegSetSelfPeer(setself)
}

type addTask []string
type finishedTask *pb

//Implement for CatalogHelper
func (c *txsyncCat) Name() string                        { return syncTxCatName }
func (c *txsyncCat) GetPolicies() gossip.CatalogPolicies { return c.policy }

func (c *txsyncCat) GenDigest() model.Digest {

}

func (c *txsyncCat) Update(model.Update) error {

}

func (c *txsyncCat) MakeUpdate(model.Digest) model.Update {

}

func (c *txsyncCat) TransDigestToPb(d_in model.Digest) *pb.GossipMsg_Digest {

}

func (c *txsyncCat) TransPbToDigest(msg *pb.GossipMsg_Digest) model.Digest {

}

func (c *txsyncCat) UpdateMessage() proto.Message { return new(pb.Gossip_TxState) }

func (c *txsyncCat) EncodeUpdate(cpo gossip.CatalogPeerPolicies, u_in model.Update, msg_in proto.Message) proto.Message {

}

func (c *txsyncCat) DecodeUpdate(cpo gossip.CatalogPeerPolicies, msg_in proto.Message) (model.Update, error) {

}
