package gossip

import (
	"bytes"
	"fmt"

	"github.com/golang/protobuf/proto"

	pb "github.com/abchain/fabric/protos"
)

// StateVersion struct
type StateVersion struct {
	known  bool
	hash   []byte
	number uint64
}

// PeerState struct
type PeerState struct {
	id     string                   // peer id
	states map[string]*StateVersion // key: catalog
}

// Model struct
type Model struct {
	self   PeerState
	store  map[string]*PeerState // key: peer id
	merger VersionMergerInterface
	crypto CryptoInterface
	nseq   uint64
}

func newDefaultModel() *Model {
	m := &Model{
		self: PeerState{
			id:     "",
			states: map[string]*StateVersion{},
		},
		store:  map[string]*PeerState{},
		merger: &VersionMergerDummy{},
		crypto: &CryptoImpl{},
		nseq:   0,
	}
	return m
}

func (m *Model) init(id string, state []byte, number uint64) {
	m.self = PeerState{
		id:     id,
		states: map[string]*StateVersion{},
	}

	m.self.states["tx"] = &StateVersion{
		known:  true,
		hash:   state,
		number: number,
	}
}

func (m *Model) get(peerID string, catalog string) *StateVersion {
	state, ok := m.store[peerID]
	if !ok {
		return nil
	}
	version, ok := state.states[catalog]
	if !ok {
		return nil
	}
	return version
}

func (m *Model) set(peerID string, catalog string, state *PeerState) error {
	//return m.localUpdate(source)
	return nil
}

func (m *Model) setMerger(merger VersionMergerInterface) {
	m.merger = merger
	if m.merger == nil {
		m.merger = &VersionMergerDummy{}
	}
}

func (m *Model) keys(catalog string) []string {
	keys := []string{}
	for id, peer := range m.store {
		_, ok := peer.states[catalog]
		if ok {
			keys = append(keys, id)
		}
	}
	return keys
}

func (m *Model) forEach(iter func(id string, peer *PeerState) error) error {
	var err error
	for k, p := range m.store {
		err = iter(k, p)
		if err != nil {
			break
		}
	}
	return err
}

func (m *Model) getPeerTxState(id string) (*StateVersion, error) {
	state, ok := m.store[id]
	if !ok {
		return nil, fmt.Errorf("Peer id(%s) state not found", id)
	}

	version, ok := state.states["tx"]
	if !ok {
		return nil, fmt.Errorf("Peer id(%s) tx not found", id)
	}
	return version, nil
}

func (m *Model) validateTxs(hash []byte, txs []*pb.Transaction) []*pb.Transaction {
	// TODO: add validate method
	newTxs := []*pb.Transaction{}
	for _, tx := range txs {
		ok := m.crypto.ValidateTx(tx)
		if !ok {
			// tx already exists or error
			continue
		}
		newTxs = append(newTxs, tx)
	}
	return newTxs
}

func (m *Model) applyDigest(referer *pb.PeerID, message *pb.Gossip) error {
	if m.merger == nil {
		return fmt.Errorf("No merger implement")
	}

	digest := message.GetDigest()
	if digest == nil {
		return nil
	}

	logger.Debugf("Apply digest(%s) with %d items", message.Catalog, len(digest.Data))
	for id, state := range digest.Data {
		if id == m.self.id {
			continue
		}
		peer, ok := m.store[id]
		remote := &StateVersion{hash: state.State, number: state.Num}
		if m.crypto != nil && !m.crypto.Verify(referer.String(), id, message.Catalog, state) {
			return fmt.Errorf("New state id(%s), catalog(%s), state(%x) verify failed", id, message.Catalog, state.State)
		}
		if !ok {
			newPeer := PeerState{id: id, states: map[string]*StateVersion{}}
			newPeer.states[message.Catalog] = remote
			m.store[id] = &newPeer
			logger.Debugf("Apply peer(%s) new state with remote(%x,%d)", id, remote.hash, remote.number)
			continue
		}
		local, ok := peer.states[message.Catalog]
		if !ok || m.merger.NeedMerge(local, remote) {
			peer.states[message.Catalog] = remote
			logger.Debugf("Apply peer(%s), new state with local(%x,%d), remote(%x,%d)", id, local.hash, local.number, remote.hash, remote.number)
		}
	}

	return nil
}

func (m *Model) applyUpdate(referer *pb.PeerID, message *pb.Gossip) ([]*pb.Transaction, error) {
	// lg, err := ledger.GetLedger()
	// if err != nil {
	// 	return nil, err
	// }

	ntxs := []*pb.Transaction{}
	update := message.GetUpdate()
	if update == nil {
		return nil, fmt.Errorf("Message not update with catalog(%s)", message.Catalog)
	}

	switch message.Catalog {
	case "tx":
		txs := &pb.Gossip_Tx{}
		err := proto.Unmarshal(update.Payload, txs)
		if err != nil {
			logger.Errorf("Unmarshal gossip txs error: %s", err)
			return nil, err
		}
		ntxs = m.validateTxs(txs.State, txs.Txs.Transactions)
		if len(ntxs) == 0 {
			return nil, fmt.Errorf("Validate tx failed, ntxs(%d)", len(txs.Txs.Transactions))
		}
		// trim ledger
		// err := lg.PutTransactions(ntxs)
		// if err != nil {
		// 	return nil, fmt.Errorf("Put transactions error: %s", err)
		// }
		m.updateSelf("tx", txs.State, len(ntxs))
		logger.Debugf("Apply %d updates with state(%x)", len(ntxs), txs.State)
		break

	default:
		return nil, fmt.Errorf("Update catalog(%s) not implement", message.Catalog)
	}

	return ntxs, nil
}

func (m *Model) updateSelfTxs(state []byte, txs []*pb.Transaction) error {
	// trim ledger
	// lg, err := ledger.GetLedger()
	// if err != nil {
	// 	return err
	// }

	// hash, err := lg.GetCurrentStateHash()
	// if err != nil {
	// 	return err
	// }
	m.updateSelf("tx", state, len(txs))
	return nil
}

func (m *Model) updateSelf(catalog string, statehash []byte, count int) {
	logger.Debugf("Apply self catalog(%s) state(%x) with count(%d)", catalog, statehash, count)

	if state, ok := m.self.states[catalog]; ok {
		if bytes.Compare(state.hash, statehash) == 0 {
			state.number += uint64(count)
		} else {
			m.self.states[catalog] = &StateVersion{known: true, hash: statehash, number: uint64(count)}
		}
	} else {
		m.self.states[catalog] = &StateVersion{known: true, hash: statehash, number: uint64(count)}
	}
}

// history return map with key:=peerID
func (m *Model) history(catalog string) (map[string]*StateVersion, error) {
	results := map[string]*StateVersion{}
	for id, peer := range m.store {
		state, ok := peer.states[catalog]
		if ok {
			results[id] = state
		}
	}
	if state, ok := m.self.states[catalog]; ok {
		results[m.self.id] = state
	}
	return results, nil
}

func (m *Model) digestMessage(catalog string, maxn int) *pb.Gossip {

	message := &pb.Gossip{}
	message.Catalog = catalog
	message.Seq = m.nseq

	digest := &pb.Gossip_Digest{
		Data: map[string]*pb.Gossip_Digest_PeerState{},
	}

	// my self
	state, ok := m.self.states[catalog]
	if ok {
		dps := &pb.Gossip_Digest_PeerState{
			State:     state.hash,
			Num:       state.number,
			Signature: []byte(""),
		}
		m.crypto.Sign(catalog, dps)
		digest.Data[m.self.id] = dps
	}

	// other peers
	for id, peer := range m.store {
		state, ok := peer.states[catalog]
		if !ok {
			continue
		}
		dps := &pb.Gossip_Digest_PeerState{
			State:     state.hash,
			Num:       state.number,
			Signature: []byte(""),
		}
		m.crypto.Sign(catalog, dps)
		digest.Data[id] = dps
		if maxn > 0 && len(digest.Data) >= maxn {
			break
		}
	}

	// bytes, err := proto.Marshal(digest)
	// if err != nil {
	// 	return nil
	// }

	message.M = &pb.Gossip_Digest_{digest}
	m.nseq++

	return message
}

func (m *Model) gossipTxMessage(hash []byte, txs []*pb.Transaction) *pb.Gossip {

	message := &pb.Gossip{}
	message.Catalog = "tx"
	message.Seq = m.nseq

	gossiptx := &pb.Gossip_Tx{}
	gossiptx.Num = uint32(len(txs))
	gossiptx.State = hash // TODO: apply state bytes
	gossiptx.Txs = &pb.TransactionBlock{}
	gossiptx.Txs.Transactions = txs

	bytes, err := proto.Marshal(gossiptx)
	if err != nil {
		return nil
	}

	update := &pb.Gossip_Update{bytes}
	message.M = &pb.Gossip_Update_{update}

	m.nseq++

	return message
}
