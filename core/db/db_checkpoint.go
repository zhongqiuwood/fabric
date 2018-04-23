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

func getCheckPointPath(dbname string) string {
	dbPath := viper.GetString("peer.fileSystemPath")
	return filepath.Join(util.CanonicalizePath(dbPath), "checkpoint", dbname)
}

func (baseHandler *baseHandler) produceDbByCheckPoint(dbName string, blockNumber uint64, statehash string, cf []string) error {
	cpPath := baseHandler.getCheckpointPath(baseHandler.dbName, blockNumber, statehash)
	baseHandler.dbName = dbName
	baseHandler.opendb(cpPath, cf)
	targetDir := getDBPath(dbName)
	return baseHandler.createCheckpoint(targetDir)
}

func (baseHandler *baseHandler) ProduceCheckpoint(newBlockNumber uint64, statehash string) error {
	cpPath := baseHandler.getCheckpointPath(baseHandler.dbName, newBlockNumber, statehash)
	return baseHandler.createCheckpoint(cpPath)
}

func (baseHandler *baseHandler) getCheckpointPath(dbName string, newBlockNumber uint64, statehash string) string {

	checkpointTop := util.CanonicalizePath(getCheckPointPath(dbName))
	util.MkdirIfNotExist(checkpointTop)
	//return checkpointTop + dbg.Int2string(newBlockNumber) + "-" + statehash
	return checkpointTop + statehash
}

func (openchainDB *baseHandler) createCheckpoint(cpPath string) error {
	sourceDB := openchainDB.dbHandler
	checkpoint, err := sourceDB.NewCheckpoint()
	if err != nil {
		dbLogger.Errorf("[%s] NewCheckpoint Error: %s", printGID, err)
		return err
	}
	defer checkpoint.Destroy()
	err = checkpoint.CreateCheckpoint(cpPath, 0)
	if err != nil {
		dbLogger.Errorf("[%s] Failed to produce checkpoint: %s, <%s>", printGID, err, cpPath)
	} else {
		dbLogger.Infof("[%s] Produced checkpoint successfully: %s", printGID, cpPath)
	}
	return err
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
