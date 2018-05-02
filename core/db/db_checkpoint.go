package db

import (
	"fmt"
	"github.com/abchain/fabric/core/util"
	_ "github.com/abchain/fabric/protos"
	"github.com/spf13/viper"
	"github.com/tecbot/gorocksdb"
	"path/filepath"
)

var CurrentDBPathKey = []byte("blockCount")

func getCheckPointPath(dbname string) string {
	chkpPath := filepath.Join(util.CanonicalizeFilePath(viper.GetString("peer.fileSystemPath")),
		"checkpoint")

	util.MkdirIfNotExist(chkpPath)

	return util.CanonicalizePath(chkpPath) + dbname
}

func activeDbName(tag []byte) string {
	if tag == nil {
		return "db"
	} else {
		return "db_" + string(tag)
	}
}

func encodeStatehash(statehash []byte) string {
	return fmt.Sprintf("%x", statehash)
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

	dbLogger.Infof("[%s] Start state switching to %s", printGID, statehash)

	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()

	//open checkpoint on read-only mode
	chkp, err := gorocksdb.OpenDbForReadOnly(opts, getCheckPointPath(statehash), true)
	if err != nil {
		return fmt.Errorf("[%s] Open checkpoint [%s] fail: %s", printGID, statehash, err)
	}

	defer chkp.Close()

	newtag := util.GenerateBytesUUID()
	newdbPath := getDBPath(activeDbName(newtag))
	dbLogger.Infof("[%s] Create new state db on %s", printGID, newdbPath)

	//copy this checkpoint to active db path
	err = createCheckpoint(chkp, newdbPath)
	if err != nil {
		return err
	}

	//write the new db tag, if we fail here, we just have an dsicarded path
	err = globalDataDB.put(globalDataDB.persistCF, []byte(currentDBKey), newtag)
	if err != nil {
		dbLogger.Errorf("[%s] Can't write globaldb: <%s>. Fail to create new state db at %s",
			printGID, err, newdbPath)
		return err
	}

	//now open the new state ...
	dbopts := DefaultOption()
	defer dbopts.Destroy()
	newdb := &ocDB{}
	newdb.OpenOpt = dbopts
	err = newdb.open(newdbPath)
	if err != nil {
		return err
	}

	newdb.OpenOpt = nil

	//prepare ok, now do the switch ...
	oc.Lock()
	defer oc.Unlock()

	olddb := oc.db
	oc.db = newdb

	//done, now release one refcount, and close db if needed
	olddb.finalDrop = false //DATA RACE? No, it should be SAFE
	select {
	case <-olddb.extendedLock:
	default:
		dbLogger.Infof("[%s] Release current db <%s>", printGID, olddb.dbName)
		olddb.close()
		olddb.dropDB()
	}

	dbLogger.Infof("[%s] State switch to %s done", printGID, statehash)

	return nil
}
