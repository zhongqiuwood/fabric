package stub

import (
	"fmt"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
	"github.com/op/go-logging"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var logger = logging.MustGetLogger("gossipstub")

type GossipHandler interface {
	HandleMessage(*pb.Gossip) error
	Stop()
}

type GossipHandlerImpl struct {
	GossipHandler
}

func ObtainHandler(h *pb.StreamHandler) GossipHandler {
	hh, ok := h.StreamHandlerImpl.(*GossipHandlerImpl)
	if !ok {
		panic("type error, not GossipHandlerImpl")
	}

	return hh.GossipHandler
}

func (h *GossipHandlerImpl) Tag() string { return "Gossip" }

func (h *GossipHandlerImpl) EnableLoss() bool { return true }

func (h *GossipHandlerImpl) NewMessage() proto.Message { return new(pb.Gossip) }

func (h *GossipHandlerImpl) HandleMessage(m proto.Message) error {
	return h.GossipHandler.HandleMessage(m.(*pb.Gossip))
}

func (h *GossipHandlerImpl) BeforeSendMessage(proto.Message) error {
	return nil
}
func (h *GossipHandlerImpl) OnWriteError(e error) {
	logger.Error("Gossip handler encounter writer error:", e)
}

func (h *GossipHandlerImpl) OnHandleStream() {

}



type GossipFactory func(*pb.PeerID, *pb.StreamStub) GossipHandler

var DefaultFactory GossipFactory

func GetDefaultFactory() pb.StreamHandlerFactory { return DefaultFactory }

func (t GossipFactory) NewStreamHandlerImpl(id *pb.PeerID, sstub *pb.StreamStub, initiated bool) (pb.StreamHandlerImpl, error) {
	if t == nil {
		return nil, fmt.Errorf("No default factory")
	}

	return &GossipHandlerImpl{t(id, sstub)}, nil
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
