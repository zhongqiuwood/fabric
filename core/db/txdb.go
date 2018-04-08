/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package db

import (
	"github.com/op/go-logging"
	"github.com/tecbot/gorocksdb"
	"github.com/abchain/fabric/protos"
	"github.com/abchain/fabric/dbg"
)

var txdbLogger = logging.MustGetLogger("txdb")
var txDbColumnfamilies = []string{
	TxCF,
	GlobalCF,
	ConsensusCF,
	PersistCF,
}

type GlobalDataDB struct {
	BaseHandler
}

var globalDataDB = createTxDB()

func createTxDB() *GlobalDataDB {
	txdb := &GlobalDataDB{}
	txdb.cfMap = make(map[string]*gorocksdb.ColumnFamilyHandle)
	return txdb
}

func (txdb *GlobalDataDB) feedCfHandlers(cfHandlers []*gorocksdb.ColumnFamilyHandle) {
	txdb.cfMap[TxCF] = cfHandlers[1]
	txdb.cfMap[GlobalCF] = cfHandlers[2]
	txdb.cfMap[ConsensusCF] = cfHandlers[3]
	txdb.cfMap[PersistCF] = cfHandlers[4]
}

func (txdb *GlobalDataDB) open(dbname string, cf []string) {
	dbPath := getDBPath(dbname)
	txdb.dbName = dbname
	cfhandlers := txdb.opendb(dbPath, cf)
	txdb.feedCfHandlers(cfhandlers)
}

func (txdb *GlobalDataDB) PutGlobalState(gs *protos.GlobalState,
	statehash []byte, writeBatch *gorocksdb.WriteBatch) error {
	data, _ := gs.Bytes()
	dbg.Infof("statehash<%x>: gs<%+v>, gs.Bytes<%x>", statehash, gs, data)
	txdb.BatchPut(GlobalCF, writeBatch, statehash, data)
	return  nil
}

func (txdb *GlobalDataDB) getTransactionFromDB(txids []string) []*protos.Transaction  {

	if txids == nil {
		return nil
	}

	length := len(txids)
	txs := make([]*protos.Transaction, length)

	idx := 0
	for _, id := range txids {
		txInByte, err := txdb.GetValue(TxCF, []byte(id))

		if err != nil {
			dbg.ChkErr(err)
			txs = nil
			break
		}

		if txInByte == nil {
			dbg.DumpStack()
			break
		}

		var tx *protos.Transaction

		tx, err = protos.UnmarshallTransaction(txInByte)

		if err != nil {
			dbg.ChkErr(err)
			txs = nil
			break
		}

		txs[idx] = tx
		idx++
	}

	return txs
}
