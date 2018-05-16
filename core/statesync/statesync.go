package statesync

import (
	"fmt"
	_ "github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/peer"
	"github.com/abchain/fabric/core/statesync/stub"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	_ "github.com/spf13/viper"
	"golang.org/x/net/context"
	"sync"
)

var logger = logging.MustGetLogger("statesync")

func init() {
	stub.DefaultFactory = func(id *pb.PeerID) pb.StreamHandlerImpl {
		logger.Debug("create handler for peer", id)
		return &StateSyncHandler{peerId: id}
	}
}

var syncer *StateSync
var syncerErr error
var once sync.Once

func GetNewStateSync(p peer.Peer) (*StateSync, error) {

	stub := p.GetStreamStub("sync")
	if stub == nil {
		return nil, fmt.Errorf("peer have no sync streamstub")
	}

	return &StateSync{stub}
}

// gives a reference to a singleton
func GetStateSync() (*StateSync, error) {
	return syncer, syncerErr
}

func NewStateSync(p peer.Peer) {
	logger.Debug("State sync module inited")

	once.Do(func() {
		syncer, syncerErr = GetNewStateSync(p)
		if syncerErr != nil {
			logger.Error("Create new state syncer fail:", syncerErr)
		}
	})
}

type StateSync struct {
	*pb.StreamStub
}

type syncOpt struct {
}

func NewSyncOption() *syncOpt {
	return new(syncOpt)
}

//main entry for statesync: it is sync, thread-unsafe and NOT re-entriable, but you can call IsBusy
//at another thread and check its status
func (s *StateSync) SyncToState(ctx context.Context, targetState []byte, opt *syncOpt) error {

	return fmt.Errorf("Not implied")
}

type StateSyncHandler struct {
	*handler
}

func (h *StateSyncHandler) Tag() string { return "StateSync" }

func (h *StateSyncHandler) EnableLoss() bool { return false }

func (h *StateSyncHandler) NewMessage() proto.Message { return new(pb.SyncMsg) }

func (h *StateSyncHandler) HandleMessage(m proto.Message) error {

	wrapmsg := m.(*pb.SyncMsg)

	//return h.HandleMessage()
}

func (h *StateSyncHandler) BeforeSendMessage(proto.Message) error {
	return nil
}
func (h *StateSyncHandler) OnWriteError(e error) {
	logger.Error("Sync handler encounter writer error:", e)
}
