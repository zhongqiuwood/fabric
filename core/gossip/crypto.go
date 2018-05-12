package gossip

import (
	pb "github.com/abchain/fabric/protos"
)

// CryptoInterface interface
type CryptoInterface interface {
	ValidateTx(tx *pb.Transaction) bool
	Verify(refererID string, peerID string, catalog string, message *pb.Gossip_Digest_PeerState) bool
	Sign(catelog string, message *pb.Gossip_Digest_PeerState) error
}

// CryptoImpl struct
type CryptoImpl struct {
	CryptoInterface
}

// ValidateTx function
func (c *CryptoImpl) ValidateTx(tx *pb.Transaction) bool {
	return true
}

// Verify function
func (c *CryptoImpl) Verify(refererID string, peerID string, catalog string, message *pb.Gossip_Digest_PeerState) bool {
	return true
}

// Sign function
func (c *CryptoImpl) Sign(catelog string, message *pb.Gossip_Digest_PeerState) error {
	message.Signature = []byte("YES")
	return nil
}
