package gossip

import (
	pb "github.com/abchain/fabric/protos"
)

// PeerMessage struct
type PeerMessage struct {
	id      string
	key     string
	version uint64
}

// Source struct
type Source struct {
	key          string
	version      uint64
	ctime        uint64
	utime        uint64
	rawMessage   *pb.Message
	peerVersions map[string]uint64
}

// Model struct
type Model struct {
	// key: digest of message
	// value: Source
	store map[string]*Source
}

func (m *Model) get(key string) *Source {
	source, ok := m.store[key]
	if !ok {
		return nil
	}
	return source
}

func (m *Model) set(source *Source) error {
	return m.localUpdate(source)
}

func (m *Model) keys() []string {
	keys := []string{}
	for key := range m.store {
		keys = append(keys, key)
	}
	return keys
}

func (m *Model) forEach(iter func(k string, v *Source) error) error {
	var err error
	for key, source := range m.store {
		err = iter(key, source)
		if err != nil {
			break
		}
	}
	return err
}

func (m *Model) applyUpdate(source *Source) error {
	old := m.get(source.key)
	if old != nil && old.key > source.key {
		return nil
	}
	m.store[source.key] = source
	return nil
}

func (m *Model) localUpdate(source *Source) error {
	return nil
}

func (m *Model) history(sources map[string]*Source) map[string]*Source {
	results := map[string]*Source{}
	for k, s := range m.store {
		old, ok := sources[k]
		if !ok || old.version < s.version {
			results[k] = s
		}
	}
	return results
}
