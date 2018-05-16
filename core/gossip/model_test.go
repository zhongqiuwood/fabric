package gossip

import (
	"testing"

	pb "github.com/abchain/fabric/protos"
)

func TestModel(t *testing.T) {
	m := &Model{
		merger: &VersionMergerDummy{},
		crypto: &CryptoImpl{},
	}
	m.init("peerid", []byte("1"), 1)

	message := &pb.Gossip{}
	referer := &pb.PeerID{}

	catalog := "tx"
	message.Catalog = catalog
	message.Seq = m.nseq

	digest := &pb.Gossip_Digest{
		Data: map[string]*pb.Gossip_Digest_PeerState{},
	}
	dps := &pb.Gossip_Digest_PeerState{
		State:     []byte("test"),
		Num:       1,
		Signature: []byte(""),
	}
	m.crypto.Sign(catalog, dps)
	digest.Data["peerid"] = dps

	// bytes, err := proto.Marshal(digest)
	// if err != nil {
	// 	return nil
	// }

	message.M = &pb.Gossip_Digest_{digest}

	m.applyDigest(referer, message)
}
