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

const (
	testCat 
)

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

func restoreGossipModule(old []func(*gossip.GossipStub)) {
	gossip.RegisterCat = old
}

func initGossipModule(t *testing.T) []func(*gossip.GossipStub) {

	oldv := gossip.RegisterCat

	gossip.RegisterCat = append(gossip.RegisterCat, func(stub *gossip.GossipStub) {
		
	})

	return oldv
}

func TestCatalogyIn2Peer(t *testing.T) {

}
