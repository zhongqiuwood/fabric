package gossip

import (
	pb "github.com/abchain/fabric/protos"
)

// CryptoInterface interface
type CryptoInterface interface {
	Verify(peerID string, catalog string, message *pb.Gossip_Digest_PeerState) bool
	Sign(catelog string, message *pb.Gossip_Digest_PeerState) error
}

// CryptoImpl struct
type CryptoImpl struct {
	CryptoInterface
}

// Verify function
func (c *CryptoImpl) Verify(peerID string, catalog string, message *pb.Gossip_Digest_PeerState) bool {
	return true
}

// Sign function
func (c *CryptoImpl) Sign(catelog string, message *pb.Gossip_Digest_PeerState) error {
	message.Signature = []byte("YES")
	return nil
}
