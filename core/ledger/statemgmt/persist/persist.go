package persist

import (
	"github.com/abchain/fabric/core/db"
	"github.com/op/go-logging"
)

const syncPositionKey = "syncPositionKey"

var logger = logging.MustGetLogger("persist")


func StoreSyncPosition(data []byte) error {
	return store(syncPositionKey, data)
}

func store(key string, value []byte) error {
	dbhandler := db.GetGlobalDBHandle()
	return dbhandler.PutValue(db.PersistCF, []byte(key), value)
}

func load(key string) ([]byte, error) {
	dbhandler := db.GetGlobalDBHandle()
	return dbhandler.GetValue(db.PersistCF, []byte(key))
}

func LoadSyncPosition() []byte {

	data, err := load(syncPositionKey)
	if err != nil {
		logger.Debugf("LoadSyncPosition err:", err)
		return nil
	}
	return data
}

func ClearSyncPosition() error {
	dbhandler := db.GetGlobalDBHandle()
	return dbhandler.DeleteKey(db.PersistCF, []byte(syncPositionKey))
}

