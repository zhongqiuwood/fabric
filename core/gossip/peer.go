package gossip

import (
	"math/rand"
	"time"

	"github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("gossip")

type Gossip interface {
	BroadcastTx([]*pb.Transaction) error
}

// PeerAction struct
type PeerAction struct {
	id                 *pb.PeerID
	activeTime         int64
	digestSeq          uint64
	digestSendTime     int64
	digestResponseTime int64
	updateSendTime     int64
	updateReceiveTime  int64
}

// GossipStub struct
type GossipStub struct {
	*pb.StreamStub
	peer.Discoverer
	model       *Model
	peerActions map[string]*PeerAction
}

type GossipHandler struct {
	peerId *pb.PeerID
}

func init() {
	stub.DefaultFactory = func(id *pb.PeerID) stub.GossipHandler {
		logger.Debug("create handler for peer", id)
		gossipStub.peerActions[id.String()] = &PeerAction{
			id:                 id,
			digestSeq:          0,
			digestSendTime:     0,
			digestResponseTime: 0,
		}
		return &GossipHandler{peerId: id}
	}
}

func (t *GossipHandler) HandleMessage(m *pb.Gossip) error {
	now := time.Now().Unix()
	p, ok := gossipStub.peerActions[t.peerId.String()]
	if ok {
		p.activeTime = now
	}
	if m.GetDigest() != nil {
		// process digest
		gossipStub.model.applyDigest(m)
		if ok {
			p.digestResponseTime = now
		}
	} else if m.GetUpdate() != nil {
		// process update
		gossipStub.model.applyUpdate(m)
		if ok {
			p.digestResponseTime = now
		}
	}
	return nil
}

func (t *GossipHandler) Stop() {
	// remove peer from actions
	_, ok := gossipStub.peerActions[t.peerId.String()]
	if ok {
		delete(gossipStub.peerActions, t.peerId.String())
	}
}

var gossipStub *GossipStub

//init the singleton of gossipstub
func NewGossip(p peer.Peer) {

	nb, err := p.GetNeighbour()
	logger.Debug("Gossip module inited")

	model := &Model{
		merger: &VersionMergerDummy{},
	}
	if err != nil {

		logger.Errorf("No neighbour for this peer (%s), gossip run without access control", err)
		gossipStub = &GossipStub{
			StreamStub: p.GetStreamStub("gossip"),
			model:      model,
		}

	} else {

		dis, err := nb.GetDiscoverer()
		if err != nil {
			logger.Errorf("No discovery for this peer (%s), gossip run without access control", err)
		}

		gossipStub = &GossipStub{
			StreamStub: p.GetStreamStub("gossip"),
			Discoverer: dis,
			model:      model,
		}
	}
	gossipStub.peerActions = map[string]*PeerAction{}

}

// GetGossip - gives a reference to a 'singleton' GossipStub
func GetGossip() Gossip {

	return gossipStub
}

func (s *GossipStub) BroadcastTx(txs []*pb.Transaction) error {
	s.model.updateSelfTxs(txs) // query state as tx.Txid
	return nil
	//return fmt.Errorf("No implement")
}

func (s *GossipStub) sendTxDigests(refer *PeerAction) {
	var now = time.Now().Unix()
	var targetIDs []*pb.PeerID
	if refer.digestSendTime+30 < now {
		targetIDs = append(targetIDs, refer.id)
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(targetIDs) < 3 {
		number := 0
		rindex := rnd.Intn(len(s.peerActions))
		macthed := false
		for _, peer := range s.peerActions {
			if peer.id == refer.id {
				number++
				continue
			}
			if number == rindex {
				macthed = true
				targetIDs = append(targetIDs, peer.id)
			}
			number++
		}
		if !macthed {
			break
		}
	}
	if len(targetIDs) == 0 {
		logger.Debugf("No digest need to send to any peers, with refer(%s)", refer.id.String())
		return
	}

	handlers := s.PickHandlers(targetIDs)
	message := s.model.digestMessage("tx", 0)
	for i, handler := range handlers {
		id := targetIDs[i] // TODO: len(handlers) < len(targetIDs)
		err := handler.SendMessage(message)
		if err != nil {
			logger.Errorf("Send digest to peer(%s) failed: %s", id.String(), err)
		} else {
			s.peerActions[id.String()].digestSendTime = now
		}
	}
}

func (s *GossipStub) sendTxUpdates(refer *PeerAction, txs []*pb.Transaction) {
	var now = time.Now().Unix()
	var targetIDs []*pb.PeerID
	if refer.digestSendTime+30 < now {
		targetIDs = append(targetIDs, refer.id)
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(targetIDs) < 3 {
		number := 0
		rindex := rnd.Intn(len(s.peerActions))
		macthed := false
		for _, peer := range s.peerActions {
			if peer.id == refer.id {
				number++
				continue
			}
			if number == rindex {
				macthed = true
				targetIDs = append(targetIDs, peer.id)
			}
			number++
		}
		if !macthed {
			break
		}
	}
	if len(targetIDs) == 0 {
		logger.Debugf("No update need to send to any peers, with refer(%s)", refer.id.String())
		return
	}

	handlers := s.PickHandlers(targetIDs)
	message := s.model.gossipTxMessage(txs)
	for i, handler := range handlers {
		id := targetIDs[i] // TODO: len(handlers) < len(targetIDs)
		err := handler.SendMessage(message)
		if err != nil {
			logger.Errorf("Send update to peer(%s) failed: %s", id.String(), err)
		} else {
			s.peerActions[id.String()].updateSendTime = now
		}
	}
}