package db

import (
	"github.com/tecbot/gorocksdb"
	"sync"
)

var defaultOption baseOpt
var getDefaultOption sync.Once

// basically it was just a fixed-length prefix extractor
type indexCFPrefixGen struct{}

func (*indexCFPrefixGen) Transform(src []byte) []byte {
	return src[:1]
}
func (*indexCFPrefixGen) InDomain(src []byte) bool {
	//skip "lastIndexedBlockKey"
	return len(src) > 0 && int(src[0]) > 0
}
func (s *indexCFPrefixGen) Name() string {
	return "indexPrefixGen\x00"
}

// method has been deprecated
func (*indexCFPrefixGen) InRange(src []byte) bool {
	return false
}

//define options for db
func (openchainDB *OpenchainDB) buildOpenDBOptions() []*gorocksdb.Options {
	opt := openchainDB.db.OpenOpt

	//opts for indexesCF
	if openchainDB.indexesCFOpt == nil {
	iopt := opt.Options()
	if globalDataDB.GetDBVersion() > 1 {
		//can support prefix search
		iopt.SetPrefixExtractor(&indexCFPrefixGen{})
		openchainDB.indexesCFOpt = iopt
	}

	var dbOpts = make([]*gorocksdb.Options, len(columnfamilies))
	for i, cf := range columnfamilies {
		switch cf {
		case IndexesCF:
			dbOpts[i] = openchainDB.indexesCFOpt
		}
	}

	return dbOpts
}

func (openchainDB *OpenchainDB) cleanDBOptions() {

	if openchainDB.indexesCFOpt != nil {
		/* problem and hacking:
		   rocksdb crashs if the option with prefix extractor is destroy
		   the possible cause may be:
			   https://github.com/facebook/rocksdb/issues/1095
		   whatever we have no way to resolve it but just reset the handle to nil
		   before destory the option
		   (it was weired that cmo (merge operator) is not release in gorocksdb)
		*/
		//openchainDB.indexesCFOpt.SetPrefixExtractor(gorocksdb.NewNativeSliceTransform(nil))
		openchainDB.indexesCFOpt.Destroy()
	}

}

//open of txdb is ensured to be Once
func (txdb *GlobalDataDB) buildOpenDBOptions() []*gorocksdb.Options {

	sOpt := txdb.OpenOpt.Options()
	sOpt.SetMergeOperator(&globalstatusMO{txdb.useCycleGraph})
	txdb.globalStateOpt = sOpt

	txOpts := make([]*gorocksdb.Options, len(txDbColumnfamilies))
	for i, cf := range txDbColumnfamilies {
		switch cf {
		case GlobalCF:
			txOpts[i] = txdb.globalStateOpt
		}
	}

	return txOpts
}

func (txdb *GlobalDataDB) cleanDBOptions() {

	if txdb.globalStateOpt != nil {
		txdb.globalStateOpt.Destroy()
		txdb.globalStateOpt = nil
	}

}
