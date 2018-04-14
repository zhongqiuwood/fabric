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
	"bytes"
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

func (txdb *GlobalDataDB) GetGlobalState(statehash []byte) *protos.GlobalState{

	var gs *protos.GlobalState
	gs = nil
	data, _ := txdb.GetValue(GlobalCF, statehash)
	if data != nil {
		gs, _ = protos.UnmarshallGS(data)
	}

	return gs
}


func (txdb *GlobalDataDB) AddChildNode4GlobalState(parentStateHash []byte, statehash []byte, wb *gorocksdb.WriteBatch) [][]byte {

	var res [][]byte
	data, _ := txdb.GetValue(GlobalCF, parentStateHash)
	parentGS, _ := protos.UnmarshallGS(data)

	nextNodeStateHashList := make([][]byte, len(parentGS.NextNodeStateHash)+1)

	idx := 0
	if len(parentGS.NextNodeStateHash) > 0 {

		for _, next := range parentGS.NextNodeStateHash {
			if bytes.Equal(next, statehash) {
				// do not update parentGS.NextNodeStateHash
				// if it has one that is exactly same as statehash
				nextNodeStateHashList = nil
				break
			}
			nextNodeStateHashList[idx] = next
			idx++
		}
	}

	if nextNodeStateHashList != nil {
		nextNodeStateHashList[idx] = statehash
		parentGS.NextNodeStateHash = nextNodeStateHashList
	}

	if parentGS.Branched == false && len(parentGS.NextNodeStateHash) > 1 {
		parentGS.Branched = true
		res = parentGS.NextNodeStateHash
	}

	dbg.Infof("gsInDB: len(parentGS.NextNodeStateHash)<%d>, <%+v>",
		len(parentGS.NextNodeStateHash), parentGS)

	err := txdb.PutGlobalState(parentGS, parentStateHash, wb)
	if err != nil {
		dbg.Errorf("Error: %s", err)
	}

	return res
}

func (txdb *GlobalDataDB) PutGlobalState(gs *protos.GlobalState, statehash []byte,
	wb *gorocksdb.WriteBatch) error {

	existingGs := txdb.GetGlobalState(statehash)

	if existingGs != nil {
		gs.Count = existingGs.Count
	}

	data, _ := gs.Bytes()
	dbg.Infof("statehash<%x>: gs<%+v>, gs.Bytes<%x>", statehash, gs, data)
	txdb.PutValue(GlobalCF, statehash, data, wb)
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

func (txdb *GlobalDataDB) DumpGlobalState() {
	itr := txdb.GetIterator(GlobalCF)
	defer itr.Close()

	idx := 0
	itr.SeekToFirst()

	for ; itr.Valid(); itr.Next() {
		k := itr.Key()
		v := itr.Value()
		keyBytes := k.Data()
		idx++

		gs, _:= protos.UnmarshallGS(v.Data())

		dbg.Infof("%d: statehash<%x>", gs.Count, keyBytes)
		dbg.Infof("	  branched<%t>", gs.Branched)
		dbg.Infof("	  parent<%x>", gs.ParentNodeStateHash)
		dbg.Infof("	  lastBranch<%x>", gs.LastBranchNodeStateHash)
		dbg.Infof("	  childNum<%d>:", len(gs.NextNodeStateHash))
		for _, c := range gs.NextNodeStateHash {
			dbg.Infof("        <%x>", c)
		}
		k.Free()
		v.Free()
	}
}

