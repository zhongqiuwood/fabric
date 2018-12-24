package persist

import (
	"fmt"
	"github.com/abchain/fabric/core/db"
	"github.com/golang/proto"
	pb "github.com/abchain/fabric/protos"

	"github.com/op/go-logging"
)

const syncPositionKey = "syncPositionKey"

var logger = logging.MustGetLogger("persist")


func StoreSyncPosition(level, num uint64) error {

	data := &pb.SyncState{level, num, nil}
	logger.Infof("Sync position committed: <level, num> <%d, %d>", level, num)

	raw, err := proto.Marshal(data)
	if err != nil {
		err = fmt.Errorf("Could not marshal discovery list message: %s", err)
		return err
	}

	return store(syncPositionKey, raw)
}

func store(key string, value []byte) error {
	dbhandler := db.GetGlobalDBHandle()
	return dbhandler.PutValue(db.PersistCF, []byte(key), value)
}

func load(key string) ([]byte, error) {
	dbhandler := db.GetGlobalDBHandle()
	return dbhandler.GetValue(db.PersistCF, []byte(key))
}

func LoadSyncPosition() (level, num uint64) {

	raw, err := load(syncPositionKey)

	if err != nil {
		logger.Debugf("LoadSyncPosition err:", err)
		return 0, 0
	}

	if raw == nil {
		logger.Debugf("LoadSyncPosition raw == nil")
		return 0, 0
	}

	position := &pb.SyncState{}
	err = proto.Unmarshal(raw, position)

	if err != nil {
		err = fmt.Errorf("Could not Unmarshal SyncStateChunkArray: %s", err)
		logger.Debugf("LoadSyncPosition if raw == nil || err != nil: %s", err)
		return 0, 0
	}

	return position.Level, position.BucketNum
}

func ClearSyncPosition() error {
	dbhandler := db.GetGlobalDBHandle()
	return dbhandler.DeleteKey(db.PersistCF, []byte(syncPositionKey))
}

