package acl

import (
	_ "fmt"
	_ "math/rand"
	_ "time"
)

type AccessControl interface {
	BlockPeer(string)
}

type forbidPeers struct {
	banPeerIDs map[string]int64
}
