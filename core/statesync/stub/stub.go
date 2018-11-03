package stub

import (
	"fmt"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/abchain/fabric/core/peer"
)

type SyncFactory func(*pb.PeerID) pb.StreamHandlerImpl

var DefaultSyncFactory SyncFactory

func GetDefaultFactory() pb.StreamHandlerFactory {
	return DefaultSyncFactory
}

func (t SyncFactory) NewStreamHandlerImpl(id *pb.PeerID, sstub *pb.StreamStub, initiated bool) (pb.StreamHandlerImpl, error) {
	if t == nil {
		return nil, fmt.Errorf("No default factory")
	}

	return t(id), nil
}

func (t SyncFactory) NewClientStream(conn *grpc.ClientConn) (grpc.ClientStream, error) {

	if t == nil {
		return nil, fmt.Errorf("No default factory")
	}

	serverClient := pb.NewPeerClient(conn)
	ctx := context.Background()
	stream, err := serverClient.SyncIn(ctx)

	if err != nil {
		return nil, err
	}


	return stream, nil
}
