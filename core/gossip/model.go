package gossip

import (
	"bytes"
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/peer"
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

func (m *Model) init() {
	m.self = PeerState{
		id:     "",
		states: map[string]*StateVersion{},
	}

	mypeer, err := peer.GetPeerEndpoint()
	if err != nil {
		m.self.id = mypeer.ID.String()
	}

	lg, err := ledger.GetLedger()
	if err != nil {
		logger.Errorf("Get ledger error: %s", err)
		return
	}
	hash, err := lg.GetCurrentStateHash()
	if err != nil {
		logger.Errorf("Get current state hash error: %s", err)
		return
	}
	chain, err := lg.GetBlockchainInfo()
	if err != nil {
		logger.Errorf("Get block chain info error: %s", err)
		return
	}
	block, err := lg.GetBlockByNumber(chain.Height)
	if err != nil {
		logger.Errorf("Get block info info error: %s", err)
		return
	}

	m.self.states["tx"] = &StateVersion{
		known:  true,
		hash:   hash,
		number: uint64(len(block.Txids)),
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

func (m *Model) getPeerTransactions(id string, maxn int) ([]*pb.Transaction, []byte, error) {
	state, ok := m.store[id]
	if !ok {
		return nil, nil, fmt.Errorf("Peer id(%s) state not found", id)
	}

	version, ok := state.states["tx"]
	if !ok {
		return nil, nil, fmt.Errorf("Peer id(%s) tx not found", id)
	}

	lg, err := ledger.GetLedger()
	if err != nil {
		return nil, nil, err
	}

	txs, err := lg.GetTransactionsByRange(version.hash, int(version.number)+1, int(version.number)+maxn)
	if err != nil {
		return nil, nil, err
	}
	return txs, version.hash, nil
}

func (m *Model) validateTxs(hash []byte, txs []*pb.Transaction) []*pb.Transaction {
	// TODO: add validate method
	newTxs := []*pb.Transaction{}
	lg, err := ledger.GetLedger()
	if err != nil {
		logger.Errorf("Get ledger to validate error: %s", err)
		return newTxs
	}
	for _, tx := range txs {
		_, err := lg.GetTransactionByID(tx.Txid)
		if err == nil {
			// tx already exists
			continue
		}
		newTxs = append(newTxs, tx)
	}
	return newTxs
}

func (m *Model) applyDigest(message *pb.Gossip) error {
	if m.merger == nil {
		return fmt.Errorf("No merger implement")
	}

	digest := message.GetDigest()
	if digest == nil {
		return fmt.Errorf("Message not diest with catalog(%s)", message.Catalog)
	}

	for id, state := range digest.Data {
		if id == m.self.id {
			continue
		}
		peer, ok := m.store[id]
		remote := &StateVersion{hash: state.State, number: state.Num}
		if m.crypto != nil && !m.crypto.Verify(id, message.Catalog, state) {
			continue
		}
		if !ok {
			newPeer := PeerState{id: id, states: map[string]*StateVersion{}}
			newPeer.states[message.Catalog] = remote
			m.store[id] = &newPeer
			continue
		}
		local, ok := peer.states[message.Catalog]
		if !ok || m.merger.NeedMerge(local, remote) {
			peer.states[message.Catalog] = remote
		}
	}

	return nil
}

func (m *Model) applyUpdate(message *pb.Gossip) error {
	lg, err := ledger.GetLedger()
	if err != nil {
		return err
	}

	update := message.GetUpdate()
	switch message.Catalog {
	case "tx":
		txs := &pb.Gossip_Tx{}
		err = proto.Unmarshal(update.Payload, txs)
		if err != nil {
			logger.Errorf("Unmarshal gossip txs error: %s", err)
			return nil
		}
		ntxs := m.validateTxs(txs.State, txs.Txs.Transactions)
		if len(ntxs) == 0 {
			return fmt.Errorf("Validate tx failed")
		}
		err := lg.PutTransactions(ntxs)
		if err != nil {
			return fmt.Errorf("Put transactions error: %s", err)
		}
		m.updateSelf("tx", txs.State, len(ntxs))
		break

	default:
		return fmt.Errorf("Update catalog(%s) not implement", message.Catalog)
	}

	return nil
}

func (m *Model) updateSelfTxs(txs []*pb.Transaction) error {
	lg, err := ledger.GetLedger()
	if err != nil {
		return err
	}

	hash, err := lg.GetCurrentStateHash()
	if err != nil {
		return err
	}
	m.updateSelf("tx", hash, len(txs))
	return nil
}

func (m *Model) updateSelf(catalog string, statehash []byte, count int) {
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

	digest := &pb.Gossip_Digest{}
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
