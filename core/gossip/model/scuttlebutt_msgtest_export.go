package gossip_model

import (
	pb "github.com/abchain/fabric/protos"
	_ "github.com/golang/protobuf/proto"
)

func TestPbToDigest(dig *pb.Gossip_Digest) Digest {

	ret := NewscuttlebuttDigest(nil)

	for id, v := range dig.Data {
		ret.SetPeerDigest(id, testVClock(int(v.Num)))
	}

	return ret

}

func TestDigestToPb(d_in Digest) *pb.Gossip_Digest {

	d, ok := d_in.(ScuttlebuttDigest)
	if !ok {
		panic("type error, not ScuttlebuttDigest")
	}

	ret := &pb.Gossip_Digest{Data: make(map[string]*pb.Gossip_Digest_PeerState)}

	for id, v := range d.PeerDigest() {
		ret.Data[id] = &pb.Gossip_Digest_PeerState{Num: uint64(transVClock(v))}
	}

	return ret

}
