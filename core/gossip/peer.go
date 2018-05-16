package gossip

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/abchain/fabric/core/gossip/stub"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	logging "github.com/op/go-logging"
)

var logger = logging.MustGetLogger("gossip")

// Gossip interface
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
	ledger      *ledger.Ledger
	model       *Model
	peerActions map[string]*PeerAction
}

// GossipHandler struct
type GossipHandler struct {
	peerID *pb.PeerID
}

func init() {
	stub.DefaultFactory = func(id *pb.PeerID) stub.GossipHandler {
		logger.Debug("create handler for peer", id)
		action := &PeerAction{
			id:                 id,
			digestSeq:          0,
			digestSendTime:     0,
			digestResponseTime: 0,
			updateSendTime:     0,
			updateReceiveTime:  0,
		}
		gossipStub.peerActions[id.String()] = action
		// send initial digests to target
		gossipStub.sendTxDigests(action, 1)
		return &GossipHandler{peerID: id}
	}
}

// HandleMessage method
func (t *GossipHandler) HandleMessage(m *pb.Gossip) error {
	now := time.Now().Unix()
	p, ok := gossipStub.peerActions[t.peerID.String()]
	if !ok {
		return fmt.Errorf("Peer not found")
	}

	p.activeTime = now
	if m.GetDigest() != nil {
		// process digest
		err := gossipStub.model.applyDigest(t.peerID, m)
		if err != nil {
			// send digest if last diest send time ok
			gossipStub.sendTxDigests(p, 1)

			// mark and send update to peer
			p.digestResponseTime = now
			empty := []*pb.Transaction{}
			gossipStub.sendTxUpdates(p, empty, 1)
		}
	} else if m.GetUpdate() != nil {
		// process update
		txs, err := gossipStub.model.applyUpdate(t.peerID, m)
		if err != nil {
			p.digestResponseTime = now
			gossipStub.ledger.PutTransactions(txs)
		}
	}
	return nil
}

// Stop method
func (t *GossipHandler) Stop() {
	// remove peer from actions
	_, ok := gossipStub.peerActions[t.peerID.String()]
	if ok {
		delete(gossipStub.peerActions, t.peerID.String())
	}
}

var gossipStub *GossipStub

// NewGossip : init the singleton of gossipstub
func NewGossip(p peer.Peer) {

	nb, err := p.GetNeighbour()
	logger.Debug("Gossip module inited")

	model := &Model{
		merger: &VersionMergerDummy{},
		crypto: &CryptoImpl{},
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

	peerID := ""
	mypeer, err := p.GetPeerEndpoint()
	if err != nil {
		peerID = mypeer.ID.String()
	}

	lg, err := ledger.GetLedger()
	if err != nil {
		logger.Errorf("Get ledger error: %s", err)
		return
	}
	state, err := lg.GetCurrentStateHash()
	if err != nil {
		logger.Errorf("Get current state hash error: %s", err)
		return
	}
	chain, err := lg.GetBlockchainInfo()
	if err != nil {
		logger.Errorf("Get block chain info error: %s", err)
		return
	}
	block, err := lg.GetBlockByNumber(chain.Height - 1)
	if err != nil {
		logger.Errorf("Get block info info error: %s", err)
		return
	}

	number := len(block.Txids)
	logger.Infof("Peer(%s), current state hash(%x/%x), block height(%d), number(%d)",
		peerID, state, chain.CurrentBlockHash, chain.Height, number)

	gossipStub.ledger = lg
	gossipStub.peerActions = map[string]*PeerAction{}
	gossipStub.model.init(peerID, state, uint64(number))
}

// GetGossip - gives a reference to a 'singleton' GossipStub
func GetGossip() Gossip {

	return gossipStub
}

// BroadcastTx method
func (s *GossipStub) BroadcastTx(txs []*pb.Transaction) error {
	state, err := s.ledger.GetCurrentStateHash()
	if err != nil {
		return err
	}

	// update self state
	s.model.updateSelfTxs(state, txs)

	// update self state to 3 other peers
	s.sendTxDigests(nil, 3)

	// broadcast tx to other peers
	s.sendTxUpdates(nil, txs, 3)
	return nil
}

// SetModelMerger method
func (s *GossipStub) SetModelMerger(merger VersionMergerInterface) {
	s.model.setMerger(merger)
}

func (s *GossipStub) sendTxDigests(refer *PeerAction, maxn int) {
	var now = time.Now().Unix()
	var targetIDs []*pb.PeerID
	if refer != nil && refer.digestSendTime+10 < now {
		targetIDs = append(targetIDs, refer.id)
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(targetIDs) < maxn {
		number := 0
		rindex := rnd.Intn(len(s.peerActions))
		macthed := false
		for _, peer := range s.peerActions {
			if refer != nil && peer.id == refer.id {
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

	referID := ""
	if refer != nil {
		referID = refer.id.String()
	}

	if len(targetIDs) == 0 {
		logger.Debugf("No digest need to send to any peers, with refer(%s)", referID)
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

func (s *GossipStub) sendTxUpdates(referer *PeerAction, txs []*pb.Transaction, maxn int) error {
	var now = time.Now().Unix()
	var targetIDs []*pb.PeerID
	if referer != nil && referer.updateSendTime+10 < now {
		targetIDs = append(targetIDs, referer.id)
	}

	if len(targetIDs) == 0 || len(txs) > 0 {
		// no targets or txs not empty
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for len(targetIDs) < maxn {
			number := 0
			rindex := rnd.Intn(len(s.peerActions))
			macthed := false
			for _, peer := range s.peerActions {
				if referer != nil && peer.id == referer.id {
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
	}

	refererID := ""
	if referer != nil {
		refererID = referer.id.String()
	}
	if len(targetIDs) == 0 {
		logger.Debugf("No update need to send to any peers, with referer(%s)", refererID)
		return fmt.Errorf("No peers")
	}

	hash, err := s.ledger.GetCurrentStateHash()
	if err != nil {
		return err
	}
	if len(targetIDs) == 1 && len(txs) == 0 {
		// fill transactions with state
		txstate, err := s.model.getPeerTxState(targetIDs[0].String())
		if err != nil {
			logger.Debugf("No update need to send to peer(%s), with error(%s)", targetIDs[0], err)
			return err
		}
		// at most 3 txs
		ntxs, err := s.ledger.GetTransactionsByRange(txstate.hash, int(txstate.number)+1, int(txstate.number)+4)
		if err != nil || len(ntxs) == 0 {
			logger.Debugf("No update need to send to peer(%s), with error(%s)", targetIDs[0], err)
			return err
		}
		txs = ntxs
		hash = txstate.hash
	}

	if len(txs) == 0 {
		// no txs send
		return fmt.Errorf("No txs send")
	}

	handlers := s.PickHandlers(targetIDs)
	message := s.model.gossipTxMessage(hash, txs)
	for i, handler := range handlers {
		id := targetIDs[i] // TODO: len(handlers) < len(targetIDs)
		err := handler.SendMessage(message)
		if err != nil {
			logger.Errorf("Send update to peer(%s) failed: %s", id.String(), err)
		} else {
			s.peerActions[id.String()].updateSendTime = now
		}
	}

	return nil
}
