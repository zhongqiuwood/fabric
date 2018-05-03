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
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/abchain/fabric/protos"
	"github.com/tecbot/gorocksdb"
	"sync"
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
	txCFs
	globalStateLock sync.Mutex
	//caution: destroy option before cf/db is WRONG but just ok only if it was just "default"
	//we must keep object alive if we have custom some options (i.e.: merge operators)
	globalOpt *gorocksdb.Options
}

const (
	update_nextnode = iota
	update_lastbranch
	update_nextbranch
)

type globalstatusMO struct {
}

func decodeMergeValue(b []byte) (uint64, []byte, error) {

	u, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, fmt.Errorf("Wrong uvarint data: %d", n)
	}

	return u, b[n:], nil
}

func encodeMergeValue(op byte, count uint64, hash []byte) []byte {

	buf := make([]byte, 8)

	return bytes.Join([][]byte{[]byte{op}, buf[:binary.PutUvarint(buf, count)], hash}, nil)
}

func (mo globalstatusMO) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	//to tell the true: we are not DARE TO use dblogger in these subroutines so we may just
	//keep all the error silent ...
	//REMEMBER: return false may ruin the whole db so we must try our best to keep the data robust
	var gs *protos.GlobalState
	var err error
	if existingValue != nil {
		gs, err = protos.UnmarshallGS(existingValue)
		if err != nil {
			return existingValue, true
		}
	} else {
		//this also indicate something wrong for we add an key without valid globalstate ...
		return nil, true
	}

	//the "last/next branchnode" mush be updated when we have walked through the operands
	lbtarget := gs.LastBranchNodeStateHash
	nxtarget := gs.NextBranchNodeStateHash
	var nxtargetN, lbtargetN uint64

	for _, op := range operands {

		if len(op) < 2 {
			continue //wrong op and omit it
		}

		opcode := int(op[0])
		n, h, e := decodeMergeValue(op[1:])
		if e != nil {
			continue //skip this op
		}

		switch opcode {
		case update_nextnode:
			gs.NextNodeStateHash = append(gs.NextNodeStateHash, h)
		case update_lastbranch:
			if n >= gs.Count {
				continue //skip this op
			}
			if n > lbtargetN {
				lbtargetN = n
				lbtarget = h
			}
		case update_nextbranch:
			if n <= gs.Count {
				continue //skip this op
			}
			if nxtargetN == 0 || n > nxtargetN {
				nxtargetN = n
				nxtarget = h
			}
		}
	}

	//finally we update the branch node hashs
	gs.LastBranchNodeStateHash = lbtarget
	gs.NextBranchNodeStateHash = nxtarget

	//also update branch status
	if len(gs.NextNodeStateHash) > 1 {
		gs.Branched = true
	}

	ret, err := gs.Bytes()
	if err != nil {
		return existingValue, true
	}

	return ret, true
}
func (mo globalstatusMO) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {

	//clear the mal-formed operand ...
	if len(leftOperand) < 2 {
		return rightOperand, true
	} else if len(rightOperand) < 2 {
		return leftOperand, true
	}

	opcodeL := int(leftOperand[0])
	opcodeR := int(rightOperand[0])

	if opcodeL != opcodeR || opcodeL == update_nextnode {
		return nil, false
	}

	//only operand with same op code can be merged
	nL, _, e := decodeMergeValue(leftOperand[1:])
	if e != nil {
		return rightOperand, true
	}

	nR, _, e := decodeMergeValue(rightOperand[1:])

	if e != nil {
		return leftOperand, true
	}

	switch opcodeL {
	case update_lastbranch:
		//return the later one
		if nL < nR {
			return rightOperand, true
		} else {
			return leftOperand, true
		}
	case update_nextbranch:
		//return the early7 one
		if nL < nR {
			return leftOperand, true
		} else {
			return rightOperand, true
		}
	default:
		return nil, false
	}

}
func (mo globalstatusMO) Name() string { return "GolbalStateMO" }

var globalDataDB = &GlobalDataDB{}

func (txdb *GlobalDataDB) open(dbpath string) error {

	cfhandlers := txdb.opendb(dbpath, txDbColumnfamilies, nil)

	if len(cfhandlers) != len(txDbColumnfamilies) {
		return errors.New("rocksdb may ruin or not work as expected")
	}

	//feed cfs
	txdb.cfMap = make(map[string]*gorocksdb.ColumnFamilyHandle)
	for i, cfName := range txDbColumnfamilies {
		txdb.cfMap[cfName] = cfhandlers[i]
	}

	txdb.feed(txdb.cfMap)

	return nil
}

func (txdb *GlobalDataDB) GetGlobalState(statehash []byte) *protos.GlobalState {

	data, _ := txdb.get(txdb.globalCF, statehash)
	if data != nil {
		gs, err := protos.UnmarshallGS(data)
		if err != nil {
			dbLogger.Errorf("Decode global state of [%x] fail: %s", statehash, err)
			return nil
		}

		return gs
	}

	return nil
}

type gscommiter struct {
	*gorocksdb.WriteBatch
	refGS *protos.GlobalState
}

type StateDuplicatedError struct {
	error
}

func (txdb *GlobalDataDB) addGSCritical(parentStateHash []byte,
	statehash []byte) (*gscommiter, error) {

	//this read and write is critical and must be serialized
	txdb.globalStateLock.Lock()
	defer txdb.globalStateLock.Unlock()

	data, _ := txdb.get(txdb.globalCF, parentStateHash)
	if data == nil {
		return nil, fmt.Errorf("No corresponding parent [%x]", parentStateHash)
	}

	data, _ = txdb.get(txdb.globalCF, statehash)
	if data != nil {
		return nil, StateDuplicatedError{fmt.Errorf("state [%x] exist", statehash)}
	}

	parentgs, err := protos.UnmarshallGS(data)
	if err != nil {
		return nil, fmt.Errorf("state of parent [%x] was ruined: %s", parentStateHash, err)
	}

	newgs := protos.NewGlobalState()

	newgs.Count = parentgs.Count + 1
	newgs.ParentNodeStateHash = parentStateHash
	newgs.LastBranchNodeStateHash = parentgs.LastBranchNodeStateHash

	vbytes, err := newgs.Bytes()
	if err != nil {
		return nil, fmt.Errorf("could not encode state [%x]: %s", statehash, err)
	}

	wb := gorocksdb.NewWriteBatch()

	wb.PutCF(txdb.globalCF, statehash, vbytes)
	wb.MergeCF(txdb.globalCF, parentStateHash,
		encodeMergeValue(update_nextnode, 0, statehash))

	// the "critical point": that is, node turn into a branch point and its parents and childs
	// have to be updated, we exit here so multiple updating may happen concurrently, but
	// this is safe
	if len(parentgs.NextNodeStateHash) == 1 {
		return &gscommiter{wb, parentgs}, nil
	}

	err = txdb.BatchCommit(wb)
	if err != nil {
		return nil, fmt.Errorf("commit on node [%x] was ruined: %s", parentStateHash, err)
	}

	return nil, nil

}

func (txdb *GlobalDataDB) AddGlobalState(parentStateHash []byte, statehash []byte) error {

	cm, err := txdb.addGSCritical(parentStateHash, statehash)

	if err != nil {
		return err
	}

	if cm == nil { //done
		return nil
	}

	//create snapshot, start updating on gs-tree
	sn := txdb.NewSnapshot()
	defer txdb.ReleaseSnapshot(sn)

	//first we update nextbranch, trace parents ...
	target := cm.refGS.ParentNodeStateHash
	for target != nil {
		gsbyte, err := txdb.getFromSnapshot(sn, txdb.globalCF, target)
		if gsbyte == nil {
			dbLogger.Errorf("Could not found state [%x]", target)
			break
		}

		gs, err := protos.UnmarshallGS(gsbyte)
		if err != nil {
			//TODO: should be break the whole commit process?
			dbLogger.Errorf("Decode state [%x] fail: %s", target, err)
			break
		}

		if bytes.Compare(gs.LastBranchNodeStateHash, cm.refGS.LastBranchNodeStateHash) != 0 {
			//end
			break
		}

		cm.MergeCF(txdb.globalCF, target, encodeMergeValue(update_nextbranch,
			cm.refGS.Count, parentStateHash))

		target = gs.ParentNodeStateHash
	}

	//then we update lastbranch, trace childs ...
	panic(len(cm.refGS.NextNodeStateHash) != 1) //how do you return the commiter??
	target = cm.refGS.NextNodeStateHash[0]

	for target != nil {
		gsbyte, err := txdb.getFromSnapshot(sn, txdb.globalCF, target)
		if gsbyte == nil {
			dbLogger.Errorf("Could not found state [%x]", target)
			break
		}

		gs, err := protos.UnmarshallGS(gsbyte)
		if err != nil {
			dbLogger.Errorf("Decode state [%x] fail: %s", target, err)
			break
		}

		if bytes.Compare(gs.LastBranchNodeStateHash, cm.refGS.LastBranchNodeStateHash) != 0 {
			//end
			break
		}

		cm.MergeCF(txdb.globalCF, target, encodeMergeValue(update_lastbranch,
			cm.refGS.Count, parentStateHash))

		if len(gs.NextNodeStateHash) != 1 {
			break
		}
		target = gs.NextNodeStateHash[0]
	}

	err = txdb.BatchCommit(cm.WriteBatch)
	if err != nil {
		return fmt.Errorf("commit on node [%x] was ruined: %s", parentStateHash, err)
	}

	return nil
}

func (txdb *GlobalDataDB) PutTransactions(txs []*protos.Transaction) error {

	//according to rocksdb's wiki, use writebach can do faster writting
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	for _, tx := range txs {
		data, _ := tx.Bytes()
		dbLogger.Debugf("[%s] Write transaction <%s>", printGID, tx.Txid)
		wb.PutCF(txdb.txCF, []byte(tx.Txid), data)
	}

	return txdb.DB.Write(opt, wb)
}

func (txdb *GlobalDataDB) GetTransaction(txid string) (*protos.Transaction, error) {

	txInByte, err := txdb.get(txdb.txCF, []byte(txid))

	if err != nil {
		return nil, err
	}

	if txInByte == nil {
		dbLogger.Debugf("[%s] Transaction %s not found", printGID, txid)
		return nil, nil
	} else {
		tx, err := protos.UnmarshallTransaction(txInByte)
		if err != nil {
			//treat as not found
			dbLogger.Errorf("[%s] Transaction data error: %s", printGID, err)
			return nil, nil
		}
		return tx, nil
	}

}

func (txdb *GlobalDataDB) GetTransactions(txids []string) []*protos.Transaction {

	if txids == nil {
		return nil
	}

	length := len(txids)
	txs := make([]*protos.Transaction, 0, length)

	for _, id := range txids {

		var tx *protos.Transaction
		tx, err := txdb.GetTransaction(id)

		//not try more, just quit
		if err != nil {
			return nil
		} else if tx != nil {
			txs = append(txs, tx)
		}

	}

	return txs
}


func (openchainDB *GlobalDataDB) GetIterator(cfName string) *gorocksdb.Iterator {

	cf := openchainDB.cfMap[cfName]

	if cf == nil {
		panic(fmt.Sprintf("Wrong CF Name %s", cfName))
	}

	opt := gorocksdb.NewDefaultReadOptions()
	opt.SetFillCache(true)
	defer opt.Destroy()

	return openchainDB.NewIteratorCF(opt, cf)
}

func (txdb *GlobalDataDB) PutGenesisGlobalState(statehash []byte) error {

	newgs := protos.NewGlobalState()
	v, err := newgs.Bytes()

	if err == nil {
		err = txdb.PutValue(GlobalCF, []byte(statehash), v)
	}
	return err
}
