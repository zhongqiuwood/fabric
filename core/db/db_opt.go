package db

import (
	"errors"
	"fmt"
	"github.com/tecbot/gorocksdb"
	"sync"
)

var defaultOption baseOpt
var getDefaultOption sync.Once

//define options for db
func (openchainDB *OpenchainDB) buildOpenDBOptions(opt baseOpt) []*gorocksdb.Options {

	//opts for indexesCF
	if openchainDB.indexesCFOpt == nil {
		iopt := opt.Options()
		openchainDB.indexesCFOpt = iopt
	}

	var dbOPts = make([]*gorocksdb.Options, len(columnfamilies))
	for i, cf := range columnfamilies {
		switch cf {
		case IndexesCF:
			dbOPts[i] = openchainDB.indexesCFOpt
		}
	}

	return dbOpts
}

func (openchainDB *OpenchainDB) cleanDBOptions() {

	if openchainDB.indexesCFOpt != nil {
		openchainDB.indexesCFOpt.Destroy()
	}

}

//open of txdb is ensured to be Once
func (txdb *GlobalDataDB) buildOpenDBOptions(opt baseOpt) []*gorocksdb.Options {

	sOpt := opt.Options()
	sOpt.SetMergeOperator(&globalstatusMO{"globalstateMO", txdb.useCycleGraph})
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
	}

}
