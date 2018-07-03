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
	model.Status
	gossip.CatalogPolicies
}

func (tc *testCatalogy) Name() string                        { return "TestCat" }
func (tc *testCatalogy) GetPolicies() gossip.CatalogPolicies { return tc.CatalogPolicies }
func (tc *testCatalogy) GetStatus() model.Status             { return tc.Status }

func (tc *testCatalogy) TransDigestToPb(d_in model.Digest) *pb.Gossip_Digest {
	return model.TestDigestToPb(d_in)
}

func (tc *testCatalogy) TransPbToDigest(dig *pb.Gossip_Digest) model.Digest {
	return model.TestPbToDigest(dig)
}

func (tc *testCatalogy) UpdateMessage() proto.Message {
	return make(model.Test_Scuttlebutt)
}

func (tc *testCatalogy) EncodeUpdate(cpo CatalogPeerPolicies, u model.Update, msg_in proto.Message) proto.Message {
	return model.TestUpdateEncode(u, msg_in.(*model.Test_Scuttlebutt))
}

func (tc *testCatalogy) DecodeUpdate(cpo CatalogPeerPolicies, msg proto.Message) (model.Update, error) {
	return model.TestUpdateDecode(msg)
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
