package db

import (
	"github.com/tecbot/gorocksdb"
)

func (srcDb *baseHandler) MoveColumnFamily(srcname string, dstDb IDataBaseHandler,
	dstname string, rmSrcCf bool) (uint64, error) {

	var err error
	itr := srcDb.GetIterator(srcname)
	var totalKVs uint64
	totalKVs = 0
	itr.SeekToFirst()

	for ; itr.Valid(); itr.Next() {
		k := itr.Key()
		v := itr.Value()
		err = dstDb.PutValue(dstname, k.Data(), v.Data(), nil)

		if err != nil {
			dbLogger.Error("Put value fail", err)
		}

		if rmSrcCf {
			srcDb.DeleteKey(srcname, k.Data(), nil)
		}
		k.Free()
		v.Free()
		if err != nil {
			break
		}
		totalKVs++
	}

	itr.Close()

	dbLogger.Infof("Moved %d KVs from %s.%s to %s.%s",
		totalKVs, srcDb.dbName, srcname, dstDb.GetDbName(), dstname)

	if err != nil {
		dbLogger.Errorf("An error happened during moving: %s", err)
	}

	return totalKVs, err
}
