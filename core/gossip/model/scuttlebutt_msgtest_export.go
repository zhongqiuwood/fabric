package gossip_model

import (
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
)

func TestPbToDigest(dig *pb.GossipMsg_Digest) Digest {

	ret := NewscuttlebuttDigest(nil)

	for id, v := range dig.Data {
		ret.SetPeerDigest(id, testVClock(int(v.Num)))
	}

	return ret

}

func TestDigestToPb(d_in Digest) *pb.GossipMsg_Digest {

	d, ok := d_in.(ScuttlebuttDigest)
	if !ok {
		panic("type error, not ScuttlebuttDigest")
	}

	ret := &pb.GossipMsg_Digest{Data: make(map[string]*pb.GossipMsg_Digest_PeerState)}

	for id, v := range d.PeerDigest() {
		ret.Data[id] = &pb.GossipMsg_Digest_PeerState{Num: uint64(transVClock(v))}
	}

	return ret

}

func TestUpdateEncode(u_in Update, msg *Test_Scuttlebutt) proto.Message {

	u, ok := u_in.(ScuttlebuttUpdate)

	if !ok {
		panic("type error, not ScuttlebuttUpdate")
	}

	msg.Peers = make(map[string]*Test_Scuttlebutt_Peer)

	for id, udata := range u.PeerUpdate() {

		us := transUpdate(udata)
		out := &Test_Scuttlebutt_Peer{make(map[string]int32)}
		for k, v := range us.data {
			out.Datas[k] = int32(v)
		}

		msg.Peers[id] = out

	}

	return msg
}

func TestUpdateDecode(msg_in proto.Message) Update {

	msg, ok := msg_in.(*Test_Scuttlebutt)

	if !ok {
		panic("type error, not Test_Scuttlebutt")
	}

	out := NewscuttlebuttUpdate(nil)

	for id, data := range msg.Peers {

		pu := &testPeerStatus{make(map[string]int)}

		for k, v := range data.Datas {
			pu.data[k] = int(v)
		}

		out.UpdatePeer(id, pu)
	}

	return out

}
