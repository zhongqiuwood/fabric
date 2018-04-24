package db

import (
	"encoding/binary"
	"fmt"
	"github.com/abchain/fabric/core/util"
	flog "github.com/abchain/fabric/flogging"
	"github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"github.com/tecbot/gorocksdb"
	"path/filepath"
)

var dbLogger = logging.MustGetLogger("db")
var printGID = flog.GoRDef
var rocksDBLogLevelMap = map[string]gorocksdb.InfoLogLevel{
	"debug": gorocksdb.DebugInfoLogLevel,
	"info":  gorocksdb.InfoInfoLogLevel,
	"warn":  gorocksdb.WarnInfoLogLevel,
	"error": gorocksdb.ErrorInfoLogLevel,
	"fatal": gorocksdb.FatalInfoLogLevel}

// cf in txdb
const TxCF = "txCF"
const GlobalCF = "globalCF"
const ConsensusCF = "consensusCF"
const PersistCF = "persistCF"

// cf in db
const BlockchainCF = "blockchainCF"
const StateCF = "stateCF"
const StateDeltaCF = "stateDeltaCF"
const IndexesCF = "indexesCF"
const StateIndCF = "stateIndexCF"

var BlockCountKey = []byte("blockCount")
var VersionKey = []byte("ya_fabric_db_version")

const OriginalDataBaseVersion = 1
const GlobalDataBaseVersion = 1
const (
	//the maxium of long-run rocksdb interfaces can be open at the same time
	maxOpenedExtend = 128
)

type IDataBaseHandler interface {

	////////////////////////////////
	//operations should be invoked with rw lock
	GetIterator(cfname string) *gorocksdb.Iterator
	GetValue(cfname string, key []byte) ([]byte, error)
	DeleteKey(cfname string, key []byte, wb *gorocksdb.WriteBatch) error
	PutValue(cfname string, key []byte, value []byte, wb *gorocksdb.WriteBatch) error
	//operations should be in rw lock
	////////////////////////////////

	PutTransactions(transactions []*protos.Transaction, cfname string, wb *gorocksdb.WriteBatch) error
	MoveColumnFamily(srcname string, dstDb IDataBaseHandler, dstname string, rmSrcCf bool) (uint64, error)
	GetDbName() string
	DumpGlobalState()
}

// base class of db handler and txdb handler
type baseHandler struct {
	OpenOpt *gorocksdb.Options
	cfMap   map[string]*gorocksdb.ColumnFamilyHandle
	db      *gorocksdb.DB
}

// // factory method to get db handler
// func GetDataBaseHandler() IDataBaseHandler {

// 	var dbhandler IDataBaseHandler
// 	if protos.CurrentDbVersion == 0 {
// 		dbhandler = GetDBHandle()
// 	} else {
// 		dbhandler = GetGlobalDBHandle()
// 	}
// 	return dbhandler
// }

func GetDBHandle() *OpenchainDB {
	return originalDB
}

func GetGlobalDBHandle() *GlobalDataDB {
	return globalDataDB
}

func DefaultOption() (opts *gorocksdb.Options) {
	opts = gorocksdb.NewDefaultOptions()

	dbLogger.Info("Create new default option")

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

	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	return
}

// Start the db, init the openchainDB instance and open the db. Note this method has no guarantee correct behavior concurrent invocation.
func Start() error {

	dbVersion := GlobalDataBaseVersion

	if viper.IsSet("peer.db.version") {
		//forced db version
		dbVersion = viper.GetInt("peer.db.version")
		if dbVersion > GlobalDataBaseVersion {
			return fmt.Errorf("Specified wrong version for database :d", dbVersion)
		}
	}

	dbLogger.Infof("Current db version=<%d>", dbVersion)

	opts := DefaultOption()
	clearOpt := func() {
		globalDataDB.OpenOpt = nil
		originalDB.OpenOpt = nil
	}
	defer opts.Destroy()
	defer clearOpt()

	globalDataDB.OpenOpt = opts
	err := globalDataDB.open(getDBPath("txdb"))
	if err != nil {
		return err
	}

	originalDB.OpenOpt = opts
	err = originalDB.open(getDBPath("db"))
	if err != nil {
		return err
	}

	return err
}

// Stop the db. Note this method has no guarantee correct behavior concurrent invocation.
func Stop() {
	originalDB.close()
	globalDataDB.close()
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// method exposed by IDataBaseHandler interface

// func (bashHandler *BaseHandler) PutTransactions(txs []*protos.Transaction,
// 	cfname string, wb *gorocksdb.WriteBatch) error {

// 	var opt *gorocksdb.WriteOptions
// 	opt = nil
// 	if wb == nil {
// 		wb = gorocksdb.NewWriteBatch()
// 		defer wb.Destroy()

// 		opt = gorocksdb.NewDefaultWriteOptions()
// 		defer opt.Destroy()
// 	}

// 	for _, tx := range txs {
// 		data, _ := tx.Bytes()
// 		dbLogger.Debugf("[%s] <%s><%x>", printGID, tx.Txid, data)
// 		bashHandler.PutValue(cfname, []byte(tx.Txid), data, wb)
// 	}
// 	var dbErr error
// 	if opt != nil {
// 		dbErr = bashHandler.BatchCommit(opt, wb)
// 		dbLogger.Errorf("[%s] Error: %s", printGID, dbErr)
// 	}
// 	return dbErr
// }

func (h *baseHandler) get(cf *gorocksdb.ColumnFamilyHandle, key []byte) ([]byte, error) {
	opt := gorocksdb.NewDefaultReadOptions()
	defer opt.Destroy()

	slice, err := h.db.GetCF(opt, cf, key)

	if err != nil {
		dbLogger.Errorf("[%s] Error while trying to retrieve key: %s", printGID, key)
		return nil, err
	}

	defer slice.Free()
	if slice.Data() == nil {
		// nil value is not error
		// dbLogger.Errorf("No such value for column family<%s.%s>, key<%s>[%x].",
		// 	baseHandler.dbName, cfName, string(key), key)
		return nil, nil
	}

	data := makeCopy(slice.Data())
	return data, nil
}

func (h *baseHandler) put(cf *gorocksdb.ColumnFamilyHandle, key []byte, value []byte) error {
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()

	err := h.db.PutCF(opt, cf, key, value)
	if err != nil {
		dbLogger.Errorf("[%s] Error while trying to write key: %s", printGID, key)
	}

	return err
}

func (h *baseHandler) delete(cf *gorocksdb.ColumnFamilyHandle, key []byte) error {
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()

	err := h.db.DeleteCF(opt, cf, key)
	if err != nil {
		dbLogger.Errorf("[%s] Error while trying to delete key: %s", printGID, key)
	}

	return err
}

func (h *baseHandler) GetValue(cfName string, key []byte) ([]byte, error) {
	cf, ok := h.cfMap[cfName]
	if !ok {
		return nil, fmt.Errorf("No cf for [%s]", cfName)
	}

	return h.get(cf, key)
}

func (h *baseHandler) PutValue(cfName string, key []byte, value []byte) error {
	cf, ok := h.cfMap[cfName]
	if !ok {
		return fmt.Errorf("No cf for [%s]", cfName)
	}

	return h.put(cf, key, value)
}

func (h *baseHandler) BatchCommit(writeBatch *gorocksdb.WriteBatch) error {

	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()

	return h.db.Write(opt, writeBatch)
}

func (h *baseHandler) DeleteKey(cfName string, key []byte) error {

	cf, ok := h.cfMap[cfName]
	if !ok {
		return fmt.Errorf("No cf for [%s]", cfName)
	}

	return h.delete(cf, key)
}

type baseExtHandler struct {
	*baseHandler //we basically copy the whole handler
}

// GetSnapshot create a point-in-time view of the DB.
func (h baseExtHandler) GetSnapshot() *gorocksdb.Snapshot {

	return h.db.NewSnapshot()
}

func (h baseExtHandler) GetIterator(cfName string) *gorocksdb.Iterator {
	cf := h.cfMap[cfName]

	if cf == nil {
		return nil
	}

	opt := gorocksdb.NewDefaultReadOptions()
	opt.SetFillCache(true)
	defer opt.Destroy()
	return h.db.NewIteratorCF(opt, cf)
}

func (h baseExtHandler) GetFromSnapshot(snapshot *gorocksdb.Snapshot, cfName string, key []byte) ([]byte, error) {
	cf, ok := h.cfMap[cfName]
	if !ok {
		return nil, fmt.Errorf("No cf for [%s]", cfName)
	}

	return h.getFromSnapshot(snapshot, cf, key)
}

// GetStateCFSnapshotIterator get iterator for column family - stateCF. This iterator
// is based on a snapshot and should be used for long running scans, such as
// reading the entire state. Remember to call iterator.Close() when you are done.
func (h *baseExtHandler) GetStateCFSnapshotIterator(snapshot *gorocksdb.Snapshot, cfName string) *gorocksdb.Iterator {
	cf, ok := h.cfMap[cfName]
	if !ok {
		return nil
	}

	return h.getSnapshotIterator(snapshot, cf)
}

// Open open underlying rocksdb
func (openchainDB *baseHandler) opendb(dbPath string, cf []string, cfOpts []*gorocksdb.Options) []*gorocksdb.ColumnFamilyHandle {

	opts := openchainDB.OpenOpt
	if opts == nil {
		//use some default options
		opts = gorocksdb.NewDefaultOptions()
		opts.SetCreateIfMissing(true)
		opts.SetCreateIfMissingColumnFamilies(true)
		defer opts.Destroy()
	}

	if cfOpts == nil {
		cfOpts = make([]*gorocksdb.Options, len(cf))
	}

	for i, op := range cfOpts {

		if op == nil {
			cfOpts[i] = opts
		}
	}

	db, cfHandlers, err := gorocksdb.OpenDbColumnFamilies(opts, dbPath, cf, cfOpts)

	if err != nil {
		dbLogger.Error("Error opening DB:", err)
		return nil
	}

	dbLogger.Infof("gorocksdb.OpenDbColumnFamilies<%s>, len cfHandlers<%d>", dbPath, len(cfHandlers))

	openchainDB.db = db
	return cfHandlers
}

func (h *baseHandler) close() {

	if h.cfMap != nil {
		for _, cf := range h.cfMap {
			cf.Destroy()
		}
	}

	if h.db != nil {
		h.db.Close()
	}

}

func (openchainDB *baseHandler) getSnapshotIterator(snapshot *gorocksdb.Snapshot,
	cfHandler *gorocksdb.ColumnFamilyHandle) *gorocksdb.Iterator {
	opt := gorocksdb.NewDefaultReadOptions()
	defer opt.Destroy()
	opt.SetSnapshot(snapshot)
	iter := openchainDB.db.NewIteratorCF(opt, cfHandler)
	return iter
}

func (openchainDB *baseHandler) getFromSnapshot(snapshot *gorocksdb.Snapshot,
	cfHandler *gorocksdb.ColumnFamilyHandle, key []byte) ([]byte, error) {
	opt := gorocksdb.NewDefaultReadOptions()
	defer opt.Destroy()
	opt.SetSnapshot(snapshot)
	slice, err := openchainDB.db.GetCF(opt, cfHandler, key)
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
	// even null string is OK, just create it on the work directory
	// if dbPath == "" {
	// 	panic("DB path not specified in configuration file. Please check that property 'peer.fileSystemPath' is set")
	// }
	return filepath.Join(util.CanonicalizePath(dbPath), dbname)
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
