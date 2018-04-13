package db

import (
	"github.com/spf13/viper"
	"github.com/abchain/fabric/core/util"
	"github.com/tecbot/gorocksdb"
	"github.com/abchain/fabric/dbg"
	"github.com/abchain/fabric/protos"
	"encoding/binary"
	"fmt"
)

// cf in txdb
const TxCF         = "txCF"
const GlobalCF     = "globalCF"
const ConsensusCF  = "consensusCF"
const PersistCF    = "persistCF"

// cf in db
const BlockchainCF = "blockchainCF"
const StateCF      = "stateCF"
const StateDeltaCF = "stateDeltaCF"
const IndexesCF    = "indexesCF"

var BlockCountKey = []byte("blockCount")
var VersionKey = []byte("ya_fabric_db_version")

const OriginalDataBaseVersion = 1
const GlobalDataBaseVersion = 1

type IDataBaseHandler interface {

	////////////////////////////////
	//operations should be invoked with rw lock
	GetIterator(cfname string) *gorocksdb.Iterator
	DeleteKey(cfname string, key []byte) error
	GetValue(cfname string, key []byte) ([]byte, error)
	PutValue(cfname string, key []byte, value []byte) error
	PutTransactions(transactions []*protos.Transaction, cfname string, writeBatch *gorocksdb.WriteBatch) error
	//operations should be in rw lock
	////////////////////////////////

	MoveColumnFamily(srcname string, dstDb IDataBaseHandler, dstname string, rmSrcCf bool) (uint64, error)
	GetDbName() string
	DumpGlobalState()
}

// base class of db handler and txdb handler
type BaseHandler struct {
	dbName string
	dbHandler   *gorocksdb.DB
	cfMap		map[string]*gorocksdb.ColumnFamilyHandle
}

// factory method to get db handler
func GetDataBaseHandler(dbVersion uint32) IDataBaseHandler{

	var dbhandler IDataBaseHandler
	if dbVersion == 0 {
		dbhandler = GetDBHandle()
	} else {
		dbhandler = GetGlobalDBHandle()
	}
	return dbhandler
}

func GetDBHandle() *OpenchainDB {
	return originalDB
}

func GetGlobalDBHandle() *GlobalDataDB {
	return globalDataDB
}

// Start the db, init the openchainDB instance and open the db. Note this method has no guarantee correct behavior concurrent invocation.
func Start(dbversion uint32) {

	dbg.Infof("Current db version=<%d>", dbversion)

	if dbversion == 1 {
		globalDataDB.open("txdb", txDbColumnfamilies)
	}
	originalDB.open("db", columnfamilies)
}

// Stop the db. Note this method has no guarantee correct behavior concurrent invocation.
func Stop(dbversion uint32) {
	originalDB.closeDBHandler()

	if dbversion == 1 {
		globalDataDB.closeDBHandler()
	}
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// method exposed by IDataBaseHandler interface
func (baseHandler *BaseHandler) GetDbName() string {
	return baseHandler.dbName
}

func (bashHandler *BaseHandler) PutTransactions(txs []*protos.Transaction,
	cfname string, wb *gorocksdb.WriteBatch) error {

	var opt *gorocksdb.WriteOptions
	opt = nil
	if wb == nil {
		wb = gorocksdb.NewWriteBatch()
		defer wb.Destroy()

		opt = gorocksdb.NewDefaultWriteOptions()
		defer opt.Destroy()
	}

	for _, tx := range txs {
		data, _ := tx.Bytes()
		dbg.Infof("<%s><%x>", tx.Txid, data)
		bashHandler.BatchPut(cfname, wb, []byte(tx.Txid), data)
	}
	var dbErr error
	if opt != nil {
		dbErr = bashHandler.BatchCommit(opt, wb)
		dbg.ChkErr(dbErr)
	}
	return dbErr
}

// GetIterator returns an iterator for the given column family
func (bashHandler *BaseHandler) GetIterator(cfName string) *gorocksdb.Iterator {
	cf := bashHandler.getCFByName(cfName)

	opt := gorocksdb.NewDefaultReadOptions()
	opt.SetFillCache(true)
	defer opt.Destroy()
	return bashHandler.dbHandler.NewIteratorCF(opt, cf)
}

// cache key & value in wb
func (baseHandler *BaseHandler) BatchPut(cfName string,
	writeBatch *gorocksdb.WriteBatch, key, value []byte) {
	cf := baseHandler.getCFByName(cfName)
	writeBatch.PutCF(cf, key, value)
}

// cache key & value in wb
func (baseHandler *BaseHandler) BatchDelete(cfName string,
	writeBatch *gorocksdb.WriteBatch, key []byte) {
	cf := baseHandler.getCFByName(cfName)
	writeBatch.DeleteCF(cf, key)
}


//////////////////////////////////////////////////////////////////////////
// rw lock,  write KVs into db
func (openchainDB *BaseHandler) BatchCommit(opts *gorocksdb.WriteOptions,
	writeBatch *gorocksdb.WriteBatch) error {
	return openchainDB.dbHandler.Write(opts, writeBatch)
}

// rw lock
func (bashHandler *BaseHandler) GetValue(cfName string, key []byte) ([]byte, error) {
	cf := bashHandler.getCFByName(cfName)

	opt := gorocksdb.NewDefaultReadOptions()
	defer opt.Destroy()

	slice, err := bashHandler.dbHandler.GetCF(opt, cf, key)

	if err != nil {
		dbg.Errorf("Error while trying to retrieve key: %s", key)
		dbLogger.Errorf("Error while trying to retrieve key: %s", key)
		return nil, err
	}

	defer slice.Free()
	if slice.Data() == nil {
		dbg.Errorf("No such value for column family<%s.%s>, key<%s>, key<%x>, key<%s>.",
			bashHandler.dbName,	cfName, string(key), key, dbg.Byte2string(key))
		return nil, nil
	}

	data := makeCopy(slice.Data())
	return data, nil
}

// rw lock
func (bashHandler *BaseHandler) PutValue(cfName string, key []byte, value []byte) error {
	cf := bashHandler.getCFByName(cfName)
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()
	err := bashHandler.dbHandler.PutCF(opt, cf, key, value)
	if err != nil {
		dbLogger.Errorf("Error while trying to write key: %s", key)
	}
	return err
}

// rw lock
func (bashHandler *BaseHandler) DeleteKey(cfName string, key []byte) error {
	cf := bashHandler.getCFByName(cfName)

	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()
	err := bashHandler.dbHandler.DeleteCF(opt, cf, key)
	if err != nil {
		dbLogger.Errorf("Error while trying to delete key: %s", key)
	}
	return err
}
// rw lock
//////////////////////////////////////////////////////////////////////////

// method exposed by IDataBaseHandler interface end
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

func (baseHandler *BaseHandler) getCFByName(cfName string) *gorocksdb.ColumnFamilyHandle {
	return baseHandler.cfMap[cfName]
}

func (baseHandler *BaseHandler) closeDBHandler() {
	for _, cf := range baseHandler.cfMap {
		if cf != nil {
			cf.Destroy()
		}
	}
	baseHandler.cfMap = make(map[string]*gorocksdb.ColumnFamilyHandle)
	baseHandler.dbHandler.Close()
}

// Open open underlying rocksdb
func (openchainDB *BaseHandler) opendb(dbPath string, cf []string) []*gorocksdb.ColumnFamilyHandle {

	missing := util.MkdirIfNotExist(dbPath)
	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()

	maxLogFileSize := viper.GetInt("peer.db.maxLogFileSize")
	if maxLogFileSize > 0 {
		dbLogger.Infof("Setting rocksdb maxLogFileSize to %d", maxLogFileSize)
		opts.SetMaxLogFileSize(maxLogFileSize)
	}

	keepLogFileNum := viper.GetInt("peer.db.keepLogFileNum")
	if keepLogFileNum > 0 {
		dbLogger.Infof("Setting rocksdb keepLogFileNum to %d", keepLogFileNum)
		opts.SetKeepLogFileNum(keepLogFileNum)
	}

	logLevelStr := viper.GetString("peer.db.loglevel")
	logLevel, ok := rocksDBLogLevelMap[logLevelStr]

	if ok {
		dbLogger.Infof("Setting rocks db InfoLogLevel to %d", logLevel)
		opts.SetInfoLogLevel(logLevel)
	}

	opts.SetCreateIfMissing(missing)
	opts.SetCreateIfMissingColumnFamilies(true)

	cfNames := []string{"default"}
	cfNames = append(cfNames, cf...)
	var cfOpts []*gorocksdb.Options
	for range cfNames {
		cfOpts = append(cfOpts, opts)
	}

	db, cfHandlers, err := gorocksdb.OpenDbColumnFamilies(opts, dbPath, cfNames, cfOpts)

	dbg.Infof("gorocksdb.OpenDbColumnFamilies<%s>, len cfHandlers<%d>", dbPath, len(cfHandlers))

	if err != nil {
		panic(fmt.Sprintf("Error opening DB: %s", err))
	}
	openchainDB.dbHandler = db
	return cfHandlers
}


func (baseHandler *BaseHandler) produceDbByCheckPoint(dbName string, blockNumber uint64, statehash string, cf []string) error {
	cpPath := baseHandler.getCheckpointPath(baseHandler.dbName, blockNumber, statehash)
	baseHandler.dbName = dbName
	baseHandler.opendb(cpPath, cf)
	targetDir := getDBPath(dbName)
	return baseHandler.createCheckpoint(targetDir)
}

func (baseHandler *BaseHandler) ProduceCheckpoint(newBlockNumber uint64, statehash string) error {
	cpPath := baseHandler.getCheckpointPath(baseHandler.dbName, newBlockNumber, statehash)
	return baseHandler.createCheckpoint(cpPath)
}

func (baseHandler *BaseHandler) getCheckpointPath(dbName string, newBlockNumber uint64, statehash string) string {

	checkpointTop := util.CanonicalizePath(getCheckPointPath(dbName))
	util.MkdirIfNotExist(checkpointTop)
	//return checkpointTop + dbg.Int2string(newBlockNumber) + "-" + statehash
	return checkpointTop + statehash
}

func (openchainDB *BaseHandler) createCheckpoint(cpPath string) error {
	sourceDB := openchainDB.dbHandler
	checkpoint, err := sourceDB.NewCheckpoint()
	if err != nil {
		dbg.Errorf("NewCheckpoint Error: %s", err)
		return err
	}
	defer checkpoint.Destroy()
	err = checkpoint.CreateCheckpoint(cpPath, 0)
	if err != nil {
		dbg.Errorf("Failed to produce checkpoint: %s, <%s>", err.Error(), cpPath)
	} else {
		dbg.Infof("Produced checkpoint successfully: %s", cpPath)
	}
	return err
}

func (srcDb *BaseHandler) MoveColumnFamily(srcname string, dstDb IDataBaseHandler,
	dstname string, rmSrcCf bool) (uint64, error) {

	var err error
	itr := srcDb.GetIterator(srcname)
	var totalKVs uint64
	totalKVs = 0
	itr.SeekToFirst()

	for ; itr.Valid(); itr.Next() {
		k := itr.Key()
		v := itr.Value()
		err = dstDb.PutValue(dstname, k.Data(), v.Data())
		dbg.ChkErr(err)

		if rmSrcCf {
			srcDb.DeleteKey(srcname, k.Data())
		}
		k.Free()
		v.Free()
		if err != nil {
			break
		}
		totalKVs++
	}

	itr.Close()

	dbg.Infof("Moved %d KVs from %s.%s to %s.%s",
		totalKVs, srcDb.dbName, srcname, dstDb.GetDbName(), dstname)

	if err != nil {
		dbg.Errorf("An error happened during moving: %s", err)
	}

	return totalKVs, err
}

func (openchainDB *BaseHandler) getSnapshotIterator(snapshot *gorocksdb.Snapshot,
	cfHandler *gorocksdb.ColumnFamilyHandle) *gorocksdb.Iterator {
	opt := gorocksdb.NewDefaultReadOptions()
	defer opt.Destroy()
	opt.SetSnapshot(snapshot)
	iter := openchainDB.dbHandler.NewIteratorCF(opt, cfHandler)
	return iter
}

func (openchainDB *BaseHandler) getFromSnapshot(snapshot *gorocksdb.Snapshot,
	cfHandler *gorocksdb.ColumnFamilyHandle, key []byte) ([]byte, error) {
	opt := gorocksdb.NewDefaultReadOptions()
	defer opt.Destroy()
	opt.SetSnapshot(snapshot)
	slice, err := openchainDB.dbHandler.GetCF(opt, cfHandler, key)
	if err != nil {
		dbLogger.Errorf("Error while trying to retrieve key: %s", key)
		return nil, err
	}
	defer slice.Free()
	data := append([]byte(nil), slice.Data()...)
	return data, nil
}

func getDBPath(dbname string) string {

	dbPath := viper.GetString("peer.fileSystemPath")
	if dbPath == "" {
		panic("DB path not specified in configuration file. Please check that property 'peer.fileSystemPath' is set")
	}
	return util.CanonicalizePath(dbPath) + dbname
}

func getCheckPointPath(dbname string) string {
	dbPath := viper.GetString("peer.fileSystemPath")
	if dbPath == "" {
		panic("DB path not specified in configuration file. Please check that property 'peer.fileSystemPath' is set")
	}
	return util.CanonicalizePath(dbPath) + "checkpoint/" + dbname
}

func makeCopy(src []byte) []byte {
	dest := make([]byte, len(src))
	copy(dest, src)
	return dest
}

func EncodeBlockNumberDBKey(blockNumber uint64) []byte {
	return EncodeUint64(blockNumber)
}

func EncodeUint64(number uint64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, number)
	return bytes
}

func DecodeToUint64(bytes []byte) uint64 {
	return binary.BigEndian.Uint64(bytes)
}

