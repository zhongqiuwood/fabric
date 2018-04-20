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
	"bytes"
	"errors"
	"github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"github.com/tecbot/gorocksdb"
)

var txDbColumnfamilies = []string{
	TxCF,
	GlobalCF,
	ConsensusCF,
	PersistCF,
}

type txCFs struct {
	txCF        *gorocksdb.ColumnFamilyHandle
	globalCF    *gorocksdb.ColumnFamilyHandle
	consensusCF *gorocksdb.ColumnFamilyHandle
	persistCF   *gorocksdb.ColumnFamilyHandle
}

func (c *txCFs) feed(cfmap map[string]*gorocksdb.ColumnFamilyHandle) {
	c.txCF = cfmap[TxCF]
	c.globalCF = cfmap[GlobalCF]
	c.consensusCF = cfmap[ConsensusCF]
	c.persistCF = cfmap[PersistCF]
}

type GlobalDataDB struct {
	baseHandler
	baseExtHandler
	txCFs
}

var globalDataDB = &GlobalDataDB{}

func (txdb *GlobalDataDB) open(dbpath string) error {

	cfhandlers := txdb.opendb(dbpath, txDbColumnfamilies)

	if len(cfhandlers) != 4 {
		return errors.New("rocksdb may ruin or not work as expected")
	}

	//feed cfs
	txdb.cfMap = map[string]*gorocksdb.ColumnFamilyHandle{
		TxCF:        cfHandlers[0],
		GlobalCF:    cfHandlers[1],
		ConsensusCF: cfHandlers[2],
		PersistCF:   cfHandlers[3],
	}

	txdb.feed(txdb.cfMap)

	return nil
}

func (txdb *GlobalDataDB) GetGlobalState(statehash []byte) *protos.GlobalState {

	var gs *protos.GlobalState
	data, _ := txdb.GetValue(GlobalCF, statehash)
	if data != nil {
		gs, err = protos.UnmarshallGS(data)
		if err != nil {
			dbLogger.Errorf("Decode global state of [%x] fail: %s", statehash, err)
			return nil
		}
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

	dbLogger.Infof("[%s] gsInDB: len(parentGS.NextNodeStateHash)<%d>, <%+v>",
		printGID, len(parentGS.NextNodeStateHash), parentGS)

	err := txdb.PutGlobalState(parentGS, parentStateHash, wb)
	if err != nil {
		dbLogger.Errorf("[%s] Error: %s", printGID, err)
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
	dbLogger.Infof("[%s] statehash<%x>: gs<%+v>, gs.Bytes<%x>", printGID, statehash, gs, data)
	txdb.PutValue(GlobalCF, statehash, data, wb)
	return nil
}

func (txdb *GlobalDataDB) getTransactionFromDB(txids []string) []*protos.Transaction {

	if txids == nil {
		return nil
	}

	length := len(txids)
	txs := make([]*protos.Transaction, length)

	idx := 0
	for _, id := range txids {
		txInByte, err := txdb.GetValue(TxCF, []byte(id))

		if err != nil {
			dbLogger.Errorf("[%s] Error: %s", printGID, err)
			txs = nil
			break
		}

		if txInByte == nil {
			dbLogger.Errorf("[%s] Empty txInByte", printGID)
			break
		}
		var tx *protos.Transaction
		tx, err = protos.UnmarshallTransaction(txInByte)

		if err != nil {
			dbLogger.Errorf("[%s] Error: %s", printGID, err)
			txs = nil
			break
		}

		txs[idx] = tx
		idx++
	}

	return txs
}
