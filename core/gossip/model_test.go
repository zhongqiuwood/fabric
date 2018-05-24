package gossip

import (
	"testing"

	"github.com/golang/protobuf/proto"

	pb "github.com/abchain/fabric/protos"
	logging "github.com/op/go-logging"
)

func TestModelDigest(t *testing.T) {
	logging.SetLevel(logging.DEBUG, "")

	m := newDefaultModel()
	m.init("mypeerid", []byte("00000000"), 1)

	message := &pb.Gossip{}
	referer := &pb.PeerID{}

	catalog := "tx"
	message.Catalog = catalog
	message.Seq = m.nseq

	digest := &pb.Gossip_Digest{
		Data: map[string]*pb.Gossip_Digest_PeerState{},
	}
	dps1 := &pb.Gossip_Digest_PeerState{
		State:     []byte("222222222"),
		Num:       1,
		Signature: []byte(""),
	}
	dps2 := &pb.Gossip_Digest_PeerState{
		State:     []byte("222222222"),
		Num:       2,
		Signature: []byte(""),
	}
	m.crypto.Sign(catalog, dps1)
	m.crypto.Sign(catalog, dps2)
	digest.Data["otherpeerid1"] = dps1
	digest.Data["otherpeerid2"] = dps2

	// bytes, err := proto.Marshal(digest)
	// if err != nil {
	// 	return nil
	// }

	message.M = &pb.Gossip_Digest_{digest}

	// initial
	err := m.applyDigest(referer, message)
	if err != nil {
		t.Errorf("Apply digest1 error: %s", err)
		t.Fail()
		return
	}

	// forward
	dps1.Num = 2
	err = m.applyDigest(referer, message)
	if err != nil {
		t.Errorf("Apply digest2 error: %s", err)
		t.Fail()
		return
	}

	// reverse
	dps1.Num = 1
	err = m.applyDigest(referer, message)
	if err != nil {
		t.Errorf("Apply digest2 error: %s", err)
		t.Fail()
		return
	}

	t.Logf("Apply digest ok")
}

func TestModelUpdate(t *testing.T) {
	logging.SetLevel(logging.DEBUG, "")

	m := newDefaultModel()
	m.init("peerid", []byte("000000000"), 1)

	message := &pb.Gossip{}
	referer := &pb.PeerID{}

	catalog := "tx"
	message.Catalog = catalog
	message.Seq = m.nseq

	txs := []*pb.Transaction{}
	txs = append(txs, &pb.Transaction{
		ChaincodeID: []byte("mycc"),
		Txid:        "1111111111",
	})
	siptx := &pb.Gossip_Tx{
		State: []byte("222222222"),
		Num:   2,
		Txs: &pb.TransactionBlock{
			Transactions: txs,
		},
	}
	payload, err := proto.Marshal(siptx)
	if err != nil {
		t.Fail()
		return
	}

	update := &pb.Gossip_Update{
		Payload: payload,
	}
	message.M = &pb.Gossip_Update_{update}

	ntxs, err := m.applyUpdate(referer, message)
	if err != nil {
		t.Errorf("Apply update error: %s", err)
		t.Fail()
		return
	}

	t.Logf("Apply update ok: %d txs", len(ntxs))
}
