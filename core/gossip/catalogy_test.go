package gossip_test

import (
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"testing"
	"time"
)

type testCatalogy struct {
	gossip.CatalogPolicies
}

func (tc *testCatalogy) Name() string { return "TestCat" }

func (tc *testCatalogy) GetPolicies() gossip.CatalogPolicies { return tc.CatalogPolicies }
func (tc *testCatalogy) GetStatus() model.Status {

}

func (tc *testCatalogy) TransDigestToPb(d_in model.Digest) *pb.Gossip_Digest {
	d, ok := d_in.(model.ScuttlebuttDigest)

	ret := &pb.Gossip_Digest{Data: make(map[string]*pb.Gossip_Digest_PeerState)}

	for id, v := range d.PeerDigest() {
		ret.Data[id] = uint64(model.TransScuttlebuttTestVClock(v))
	}

	return ret
}

func (tc *testCatalogy) TransPbToDigest(dig *pb.Gossip_Digest) model.Digest {

	ret := model.NewscuttlebuttDigest(nil)

	for id, v := range dig.Data {
		ret.SetPeerDigest(id, TransIntToScuttlebuttTestVClock(int(v.Num)))
	}

	return ret
}

func (tc *testCatalogy) EncodeUpdate(cpo CatalogPeerPolicies, u_in model.Update) proto.Message {

}

func (tc *testCatalogy) DecodeUpdate(cpo CatalogPeerPolicies, bytes []byte) (model.Update, error) {

	msg := &gossip.Test_Scuttlebutt{}

	err := proto.Unmarshal(bytes, msg)

	if err != nil {
		return nil, err
	}
}

type testFactory struct {
}

func (t GossipFactory) NewStreamHandlerImpl(id *pb.PeerID, initiated bool) (pb.StreamHandlerImpl, error) {
	if t == nil {
		return nil, fmt.Errorf("No default factory")
	}

	return &GossipHandlerImpl{t(id)}, nil
}

func (t GossipFactory) NewClientStream(conn *grpc.ClientConn) (grpc.ClientStream, error) {
	if t == nil {
		return nil, fmt.Errorf("No default factory")
	}

	serverClient := pb.NewPeerClient(conn)
	ctx := context.Background()
	stream, err := serverClient.GossipIn(ctx)

	if err != nil {
		return nil, err
	}

	return stream, nil
}

func TestCatalogyIn2Peer(t *testing.T) {

}
