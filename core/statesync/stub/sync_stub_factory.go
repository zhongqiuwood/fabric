package stub

import (
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/abchain/fabric/core/statesync"
	"github.com/abchain/fabric/core/peer"
)

//type SyncFactory func(*pb.PeerID, string, *pb.StreamStub) pb.StreamHandlerImpl
type SyncFactory struct {
	stateSyncStub *statesync.StateSyncStub
}

func InitStateSyncStub(bindPeer peer.Peer, ledgerName string, srv *grpc.Server) *statesync.StateSyncStub {

	sstub := statesync.NewStateSyncStubWithPeer(bindPeer, ledgerName)
	if sstub == nil {
		return nil
	}

	err := bindPeer.AddStreamStub("sync", SyncFactory{sstub}, sstub)
	if err != nil {
		return nil
	}

	sstub.StreamStub = bindPeer.GetStreamStub("sync")
	if sstub.StreamStub == nil {
		//sanity check
		panic("When streamstub is succefully added, it should not vanish here")
	}

	pb.RegisterSyncServer(srv, SyncFactory{sstub})

	return sstub
}

func (t SyncFactory) NewStreamHandlerImpl(id *pb.PeerID, sstub *pb.StreamStub, initiated bool) (pb.StreamHandlerImpl, error) {

	return t.stateSyncStub.CreateSyncHandler(id, sstub), nil
}

func (t SyncFactory) NewClientStream(conn *grpc.ClientConn) (grpc.ClientStream, error) {

	serverClient := pb.NewSyncClient(conn)
	ctx := context.Background()
	stream, err := serverClient.Data(ctx)

	if err != nil {
		return nil, err
	}

	return stream, nil
}


func (t SyncFactory) Data(stream pb.Sync_DataServer) error {
	return t.stateSyncStub.HandleServer(stream)
}

func (t SyncFactory) Control(stream pb.Sync_ControlServer) error {

	return nil
}
