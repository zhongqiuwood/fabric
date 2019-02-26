package txnetwork

import (
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"sync"
)

var logger = logging.MustGetLogger("gossip_cat")

var global struct {
	sync.Mutex
	ind     map[*gossip.GossipStub]*txNetworkGlobal
	entries map[*gossip.GossipStub]*TxNetworkEntry
}

func init() {
	global.ind = make(map[*gossip.GossipStub]*txNetworkGlobal)
	global.entries = make(map[*gossip.GossipStub]*TxNetworkEntry)
}

//txNetwork is a singleton related with stub
func getTxNetwork(stub *gossip.GossipStub) *txNetworkGlobal {
	global.Lock()
	defer global.Unlock()

	if n, ok := global.ind[stub]; ok {
		return n
	}

	ret := createNetworkGlobal()
	global.ind[stub] = ret
	return ret
}

//a standard vclock use the num field in protos
type standardVClock uint64

func toStandardVClock(v model.VClock) standardVClock {

	ret, ok := v.(standardVClock)
	if !ok {
		if v == model.BottomClock {
			return standardVClock(0)
		} else if v == model.TopClock {
			return standardVClock(^uint64(0))
		}
		panic("Type error, not standardVClock")
	}
	return ret
}

func (a standardVClock) Less(b_in model.VClock) bool {

	b, ok := b_in.(standardVClock)
	if !ok {
		if b_in == model.BottomClock {
			return false
		} else if b_in == model.TopClock {
			return true
		}

		panic("Wrong type, not standardVClock")
	}

	return a < b
}

func toPbDigestStd(d model.ScuttlebuttDigest, epoch []byte) *pb.GossipMsg_Digest {
	dmsg := new(pb.GossipMsg_Digest_PeerStates)

	if len(epoch) != 0 {
		dmsg.Epoch = epoch
	}

	dmsg.PeerD = make([]*pb.GossipMsg_Digest_PeerState, 0, len(d.PeerDigest()))

	for _, pd := range d.PeerDigest() {

		dmsg.PeerD = append(dmsg.PeerD, &pb.GossipMsg_Digest_PeerState{
			PeerName: pd.Id,
			Num:      uint64(toStandardVClock(pd.V)),
		})
	}

	dmsg.IsFull = !d.IsPartial()
	return &pb.GossipMsg_Digest{D: &pb.GossipMsg_Digest_Peer{Peer: dmsg}}
}

func parsePbDigestStd(msg *pb.GossipMsg_Digest, core interface{}) model.ScuttlebuttDigest {

	dmsg := msg.GetPeer()
	dout := model.NewscuttlebuttDigest(model.Digest(core))

	for _, ps := range dmsg.GetPeerD() {
		dout.SetPeerDigest(ps.PeerName, standardVClock(ps.GetNum()))
	}

	if !dmsg.GetIsFull() {
		dout.MarkDigestIsPartial()
	}

	return dout
}

//any handler receive co-variate notify musth handle this update
type coVarUpdate struct{}

func (coVarUpdate) To() model.VClock { return model.TopClock }

//make model add an peer by simulate a digest on it
func standardUpdateFunc(ch gossip.CatalogHandler) func(string, bool) error {

	return func(id string, create bool) error {

		if create {

			logger.Debugf("Cat %s will add peer %s", ch.Name(), id)
			dig := model.NewscuttlebuttDigest(nil)
			dig.SetPeerDigest(id, model.BottomClock)

			_ = ch.Model().RecvPullDigest(dig)
			//should generate nothing on update, we may also detect it
			//and warning if the peer has been existed
			//after we known new peer, we can broadcast it or try to obtain
			//more information
			ch.SelfUpdate()
			return nil
		} else {
			logger.Debugf("Cat %s will update peer %s", ch.Name(), id)
			u := model.NewscuttlebuttUpdate(nil)
			u.UpdatePeer(id, coVarUpdate{})

			return ch.Model().RecvUpdate(u)
		}

	}
}

//remove any peer in a scuttlebutt model
func standardEvictFunc(ch gossip.CatalogHandler) func([]string) error {
	return func(ids []string) error {

		ru := model.NewscuttlebuttUpdate(nil)
		ru.RemovePeers(ids)
		logger.Debugf("Cat %s will remove peers %v", ch.Name(), ids)

		return ch.Model().RecvUpdate(ru)
	}
}
