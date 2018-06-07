package gossip

import (
	pb "github.com/abchain/fabric/protos"
)

// CryptoInterface interface
type GossipCrypto interface {
	Verify(peerID string, catalog string, message *pb.Gossip_Digest_PeerState) bool
	Sign(catalog string, message *pb.Gossip_Digest_PeerState) (*pb.Gossip_Digest_PeerState, error)
}

// CryptoImpl struct
type cryptoImpl struct {
}

// Verify function
func (c *cryptoImpl) Verify(peerID string, catalog string, message *pb.Gossip_Digest_PeerState) bool {
	return true
}

// Sign function
func (c *cryptoImpl) Sign(catalog string, message *pb.Gossip_Digest_PeerState) (*pb.Gossip_Digest_PeerState, error) {
	message.Signature = []byte("YES")
	return message, nil
}

func NewDefaultGossipCrypto() *cryptoImpl {

	return &cryptoImpl{}
}
