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

const checkpointNamePrefix = "checkpoint."

func (oc *OpenchainDB) CheckpointCurrent(statehash []byte) error {
	statename := encodeStatehash(statehash)

	//todo: can use persist data to check if checkpoint is existed
	err := createCheckpoint(oc.db.DB, getCheckPointPath(statename))
	if err != nil {
		return err
	}

	err = globalDataDB.PutValue(PersistCF, []byte(checkpointNamePrefix+statename), statehash)
	if err != nil {
		return err
	}

	return nil
}

func (oc *OpenchainDB) StateSwitch(statehash []byte) error {

	statename := encodeStatehash(statehash)

	dbLogger.Infof("[%s] Start state switching to %s", printGID, statename)

	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()
	opts.SetCreateIfMissing(false)
	opts.SetCreateIfMissingColumnFamilies(false)

	//open checkpoint. CAUTION: you CAN NOT build checkpoint from RO-opened db
	cfname := append(columnfamilies, "default")
	cfopts := make([]*gorocksdb.Options, len(cfname))
	for i, _ := range cfopts {
		cfopts[i] = opts
	}
	chkp, cfhandles, err := gorocksdb.OpenDbColumnFamilies(opts,
		getCheckPointPath(statename), cfname, cfopts)
	if err != nil {
		return fmt.Errorf("[%s] Open checkpoint [%s] fail: %s", printGID, statename, err)
	}

	clear := func() {
		for _, cf := range cfhandles {
			cf.Destroy()
		}
		chkp.Close()
	}
	defer clear()

	newtag := []byte(util.GenerateUUID())
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
		dbLogger.Infof("[%s] Delay release current db <%s>", printGID, olddb.dbName)
	default:
		dbLogger.Infof("[%s] Release current db <%s>", printGID, olddb.dbName)
		olddb.close()
		olddb.dropDB()
	}

	dbLogger.Infof("[%s] State switch to %s done", printGID, statename)

	return nil
}
