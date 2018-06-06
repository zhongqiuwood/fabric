package gossip

import (
	pb "github.com/abchain/fabric/protos"
)

// CryptoInterface interface
type GossipCrypto interface {
	ValidateTx(tx *pb.Transaction) bool
	Verify(peerID string, catalog string, message *pb.Gossip_Digest_PeerState) bool
	Sign(catalog string, message *pb.Gossip_Digest_PeerState) (*pb.Gossip_Digest_PeerState, error)
}

// CryptoImpl struct
type cryptoImpl struct {
	GossipCrypto
}

// ValidateTx function
func (c *cryptoImpl) ValidateTx(tx *pb.Transaction) bool {
	return true
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
