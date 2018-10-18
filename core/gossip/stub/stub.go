package stub

import (
	"github.com/abchain/fabric/core/gossip"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"github.com/op/go-logging"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var logger = logging.MustGetLogger("gossip")

//create the corresponding streamstub and bind it with peer and service
func InitGossipStream(bindPeer peer.Peer, bindSrv *grpc.Server) *pb.StreamStub {

	gstub := gossip.NewGossipWithPeer(bindPeer)
	if gstub == nil {
		return nil
	}

	//bind server
	pb.RegisterGossipServer(bindSrv, GossipFactory{gstub})

	return gstub.GetSStub()
}

func init() {
	gossip.GossipFactory = func(gstub *gossip.GossipStub) pb.StreamHandlerFactory {
		return GossipFactory{gstub}
	}

	gossip.ObtainHandler = func(h *pb.StreamHandler) gossip.GossipHandler {
		hh, ok := h.StreamHandlerImpl.(*GossipHandlerImpl)
		if !ok {
			panic("type error, not GossipHandlerImpl")
		}

		return hh.GossipHandler
	}
}

type GossipHandlerImpl struct {
	gossip.GossipHandler
}

func (h GossipHandlerImpl) Tag() string { return "Gossip" }

func (h GossipHandlerImpl) EnableLoss() bool { return true }

func (h GossipHandlerImpl) NewMessage() proto.Message { return new(pb.GossipMsg) }

func (h GossipHandlerImpl) HandleMessage(m proto.Message) error {
	return h.GossipHandler.HandleMessage(m.(*pb.GossipMsg))
}

func (h GossipHandlerImpl) BeforeSendMessage(proto.Message) error {
	return nil
}
func (h GossipHandlerImpl) OnWriteError(e error) {
	logger.Error("Gossip handler encounter writer error:", e)
}

type GossipFactory struct {
	*gossip.GossipStub
}

func (t GossipFactory) NewStreamHandlerImpl(id *pb.PeerID, sstub *pb.StreamStub, initiated bool) (pb.StreamHandlerImpl, error) {

	return &GossipHandlerImpl{t.CreateGossipHandler(id)}, nil
}

func (t GossipFactory) NewClientStream(conn *grpc.ClientConn) (grpc.ClientStream, error) {
	serverClient := pb.NewGossipClient(conn)
	ctx := context.Background()
	stream, err := serverClient.In(ctx)

	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (t GossipFactory) In(stream pb.Gossip_InServer) error {
	return t.GetSStub().HandleServer(stream)
}
