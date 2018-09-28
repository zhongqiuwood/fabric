package txnetwork

import (
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"sync"
)

var logger = logging.MustGetLogger("gossip_cat")

type networkIndexs struct {
	sync.Mutex
	ind map[*gossip.GossipStub]*txNetworkGlobal
}

var global networkIndexs

func init() {
	global.ind = make(map[*gossip.GossipStub]*txNetworkGlobal)
}

func (g *networkIndexs) GetNetwork(stub *gossip.GossipStub) *txNetworkGlobal {
	g.Lock()
	defer g.Unlock()

	if n, ok := g.ind[stub]; ok {
		return n
	} else {
		return nil
	}
}

func (g *networkIndexs) CreateNetwork(stub *gossip.GossipStub) *txNetworkGlobal {
	g.Lock()
	defer g.Unlock()

	if n, ok := g.ind[stub]; ok {
		return n
	}

	ret := CreateTxNetworkGlobal()
	g.ind[stub] = ret
	return ret
}

//a standard vclock use the num field in protos
type standardVClock uint64

func toStandardVClock(v model.VClock) standardVClock {

	if v == nil {
		return standardVClock(0)
	}

	ret, ok := v.(standardVClock)
	if !ok {
		panic("Type error, not standardVClock")
	}
	return ret
}

func (a standardVClock) Less(b_in model.VClock) bool {
	if b_in == nil {
		return false
	}

	b, ok := b_in.(standardVClock)
	if !ok {
		panic("Wrong type, not standardVClock")
	}

	return a < b
}

func toPbDigestStd(d model.ScuttlebuttDigest, epoch []byte) *pb.Gossip_Digest {
	msg := new(pb.Gossip_Digest)

	if len(epoch) != 0 {
		msg.Epoch = epoch
	}

	msg.Data = make(map[string]*pb.Gossip_Digest_PeerState)

	for id, pd := range d.PeerDigest() {

		msg.Data[id] = &pb.Gossip_Digest_PeerState{
			Num: uint64(pd.(standardVClock)),
		}
	}

	msg.IsFull = !d.IsPartial()
	return msg
}

func parsePbDigestStd(msg *pb.Gossip_Digest, core interface{}) model.ScuttlebuttDigest {

	dout := model.NewscuttlebuttDigest(model.Digest(core))

	for id, ps := range msg.Data {
		dout.SetPeerDigest(id, standardVClock(ps.GetNum()))
	}

	if !msg.GetIsFull() {
		dout.MarkDigestIsPartial()
	}

	return dout
}

//notify remove any peer in a scuttlebutt model
func registerEvictFunc(target *txNetworkGlobal, catname string, m *model.Model) {
	target.RegNotify(func(ids []string) error {

		ru := model.NewscuttlebuttUpdate(nil)
		ru.RemovePeers(ids)
		logger.Debugf("Cat %s will remove peers %v", catname, ids)

		err := m.RecvUpdate(ru)
		if err != nil {
			logger.Errorf("Cat %s remove peer fail: %s", catname, err)
		}
		return err

	})
}
