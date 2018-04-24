package db

import (
	"fmt"
	"github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"github.com/tecbot/gorocksdb"
	"path/filepath"
	"sync"
)

var CurrentDBPathKey = []byte("blockCount")

func getCheckPointPath(dbname string) string {
	dbPath := viper.GetString("peer.fileSystemPath")
	return filepath.Join(util.CanonicalizePath(dbPath), "checkpoint", dbname)
}

func activeDbName(statehash string) string {
	return "db_from_" + statehash
}

func createCheckpoint(db *gorocksdb.DB, cpPath string) error {
	checkpoint, err := db.NewCheckpoint()
	if err != nil {
		return fmt.Errorf("[%s] Create checkpoint object fail: %s", printGID, err)
	}
	defer checkpoint.Destroy()
	err = checkpoint.CreateCheckpoint(cpPath, 0)
	if err != nil {
		return fmt.Errorf("[%s] Copy checkpoint fail: %s", printGID, err)
	}

	return nil
}

func (oc *OpenchainDB) StateSwitch(statehash string) error {

	dbLogger.Infof("[%s] Start state switch to %s", printGID, statehash)

	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()

	//open checkpoint on read-only mode
	chkp, err := gorocksdb.OpenDbForReadOnly(opts, getCheckPointPath(statehash), true)
	if err != nil {
		return fmt.Errorf("[%s] Open checkpoint [%s] fail: %s", printGID, statehash, err)
	}

	defer chkp.Close()

	newdbPath := getDBPath(activeDbName(statehash))
	dbLogger.Infof("[%s] Create new state db on %s", printGID, newdbPath)

	//copy this checkpoint to active db path
	err = createCheckpoint(chkp, newdbPath)
	if err != nil {
		return err
	}

	//now open the new state ...
	dbopts := DefaultOption()
	defer dbopts.Destroy()
	newdb := &OpenchainDB{}
	newdb.OpenOpt = dbopts
	err = newdb.open(newdbPath)
	if err != nil {
		return err
	}

	//prepare ok, now do the switch ...
	oc.Lock()
	defer oc.Unlock()

	olddb := oc

	//every is copied except for the mutex
	oc.baseHandler = newdb.baseHandler
	oc.openchainCFs = newdb.openchainCFs
	oc.extendedLock = newdb.extendedLock

	//done, now release one refcount, and close db if needed
	select {
	case <-olddb.extendedLock:
	default:
		dbLogger.Infof("[%s] Release current db on %s", printGID, olddb.dbTag)
		olddb.close()
	}

	dbLogger.Infof("[%s] State switch to %s done", printGID, statehash)

	return nil
}

func (baseHandler *baseHandler) produceDbByCheckPoint(dbName string, blockNumber uint64, statehash string, cf []string) error {
	cpPath := baseHandler.getCheckpointPath(baseHandler.dbName, blockNumber, statehash)
	baseHandler.dbName = dbName
	baseHandler.opendb(cpPath, cf)
	targetDir := getDBPath(dbName)
	return baseHandler.createCheckpoint(targetDir)
}

func (oc *OpenchainDB) ProduceCheckpoint(newBlockNumber uint64, statehash string) error {
	cpPath := baseHandler.getCheckpointPath(baseHandler.dbName, newBlockNumber, statehash)
	return baseHandler.createCheckpoint(cpPath)
}

func (baseHandler *baseHandler) getCheckpointPath(dbName string, newBlockNumber uint64, statehash string) string {

	checkpointTop := util.CanonicalizePath(getCheckPointPath(dbName))
	util.MkdirIfNotExist(checkpointTop)
	//return checkpointTop + dbg.Int2string(newBlockNumber) + "-" + statehash
	return checkpointTop + statehash
}

func (openchainDB *OpenchainDB) OpenCheckPoint(dbName string, blockNumber uint64, statehash string) error {
	openchainDB.closeDBHandler()
	targetDir := getDBPath(dbName)
	err := os.RemoveAll(targetDir)
	if err == nil {
		openchainDB.produceDbByCheckPoint(dbName, blockNumber, statehash, columnfamilies)
		openchainDB.closeDBHandler()
		openchainDB.open(dbName, columnfamilies)
	} else {
		dbLogger.Errorf("[%s] Error: %s", printGID, err)
	}
	return err
}
