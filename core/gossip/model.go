package gossip

import (
	"fmt"

	pb "github.com/abchain/fabric/protos"
)

// StateVersion struct
type StateVersion struct {
	known  bool
	hash   string
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
		remote := &StateVersion{hash: string(state.State[:len(state.State)]), number: state.Num}
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
	return nil
}

func (m *Model) updateSelf(catalog string, statehash string) {
	if state, ok := m.self.states[catalog]; ok {
		if state.hash == statehash {
			state.number++
		} else {
			m.self.states[catalog] = &StateVersion{known: true, hash: statehash, number: 1}
		}
	} else {
		m.self.states[catalog] = &StateVersion{known: true, hash: statehash, number: 1}
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
			State:     []byte(state.hash),
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

	// message.M = bytes
	m.nseq++

	return message
}

func (m *Model) gossipTxMessage(txs []*pb.Transaction) *pb.Gossip {

	message := &pb.Gossip{}
	message.Catalog = "tx"
	message.Seq = m.nseq

	// gossip := &pb.Gossip_Update{}
	// for _, tx := range txs {
	// }

	m.nseq++

	return message
}
