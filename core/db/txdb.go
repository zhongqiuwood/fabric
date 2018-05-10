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
	globalOpt     *gorocksdb.Options
	useCycleGraph bool
}

const (
	update_nextnode = iota
	update_lastbranch
	update_nextbranch
	update_lastnode
)

type globalstatusMO struct {
	MOName       string
	IsCycleGraph bool
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

func (mo *globalstatusMO) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
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

	//this work for both cyclic/acyclic case, because
	//in acyclic case, the count is just distance from root node
	//instead of the nearest node
	lbtargetN = gs.Count
	nxtargetN = 1<<64 - 1 //just uint64 max

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
			if n < lbtargetN { //smaller counts
				lbtargetN = n
				lbtarget = h
			} else if n == lbtargetN && lbtarget == nil {
				//this is a special case when gensis state become a node
				//else n == lbtargetN should indicate no change to lastbranch
				lbtarget = h
			}
		case update_nextbranch:
			if n < gs.Count {
				//equal is OK, means we just set node itself as nextbranch node
				continue //maybe something wrong, skip this op
			}
			if n < nxtargetN {
				nxtargetN = n
				nxtarget = h
			}
		case update_lastnode:
			//should only apply in cycle-graph case
			if mo.IsCycleGraph {
				gs.ParentNodeStateHash = append(gs.ParentNodeStateHash, h)
			}

		}
	}

	//finally we update the branch node hashs
	gs.LastBranchNodeStateHash = lbtarget
	gs.NextBranchNodeStateHash = nxtarget

	//for cycle graph, we update count
	if mo.IsCycleGraph {
		if gs.Branched() {
			gs.Count = 0
		} else {
			gs.Count = lbtargetN
		}
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

	if opcodeL != opcodeR {
		return nil, false
	}

	switch opcodeL {
	case update_lastbranch:

	case update_nextbranch:

	default:
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

	//smaller n counts
	if nL < nR {
		return leftOperand, true
	} else {
		return rightOperand, true
	}
}
func (mo *globalstatusMO) Name() string { return mo.MOName }

var globalDataDB = &GlobalDataDB{}

func (txdb *GlobalDataDB) open(dbpath string) error {

	//caution: keep it in txdb.globalOpt, DO NOT destroy!
	globalOpt := DefaultOption()
	globalOpt.SetMergeOperator(&globalstatusMO{"globalstateMO", txdb.useCycleGraph})
	txdb.globalOpt = globalOpt

	opts := make([]*gorocksdb.Options, len(txDbColumnfamilies))
	for i, cf := range txDbColumnfamilies {
		if cf == GlobalCF {
			opts[i] = txdb.globalOpt
		}
	}

	cfhandlers := txdb.opendb(dbpath, txDbColumnfamilies, opts)

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
	//the gs to be updated use its target hash, only in cycle-graph
	tailCase bool
	//"backward gs" used for update, only work in cycle-graph
	refGSAdded *protos.GlobalState
}

func (c *gscommiter) clear() {
	if c != nil && c.WriteBatch != nil {
		c.WriteBatch.Destroy()
	}
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

	parentgs, err := protos.UnmarshallGS(data)
	if err != nil {
		return nil, fmt.Errorf("state of parent [%x] was ruined: %s", parentStateHash, err)
	}

	wb := gorocksdb.NewWriteBatch()
	cmt := &gscommiter{WriteBatch: wb}
	defer cmt.clear()

	// the "critical point": that is, node turn into a branch point and its parents and childs
	// have to be updated, we exit here so multiple updating may happen concurrently, but
	// this is safe
	if len(parentgs.NextNodeStateHash) == 1 {
		cmt.refGS = parentgs
	}

	data, _ = txdb.get(txdb.globalCF, statehash)

	if data != nil {
		if !txdb.useCycleGraph {
			return nil, StateDuplicatedError{fmt.Errorf("state [%x] exist", statehash)}
		}

		gs, err := protos.UnmarshallGS(data)
		if err != nil {
			return nil, fmt.Errorf("state of [%x] was ruined: %s", statehash, err)
		}

		wb.MergeCF(txdb.globalCF, statehash,
			encodeMergeValue(update_lastnode, 0, parentStateHash))

		if len(gs.ParentNodeStateHash) == 1 {
			cmt.refGSAdded = gs
		}

		//consider this case:
		// ------- grandparent ---- parent --->---> <current>
		// when parent state (not a node before) connected to current (it definitiely a node now)
		// the "backward" path of parent (include parent itself) must be updated
		// because their "lastbranch" should change from nil to <current>
		if len(parentgs.NextNodeStateHash) == 0 {
			cmt.refGS = parentgs
			cmt.tailCase = true
		}

	} else {

		newgs := protos.NewGlobalState()

		newgs.Count = parentgs.Count + 1
		newgs.ParentNodeStateHash = [][]byte{parentStateHash}
		if !parentgs.Branched() && cmt.refGS == nil {
			newgs.LastBranchNodeStateHash = parentgs.LastBranchNodeStateHash
		} else {
			newgs.LastBranchNodeStateHash = parentStateHash
		}

		vbytes, err := newgs.Bytes()
		if err != nil {
			return nil, fmt.Errorf("could not encode state [%x]: %s", statehash, err)
		}

		wb.PutCF(txdb.globalCF, statehash, vbytes)

	}

	wb.MergeCF(txdb.globalCF, parentStateHash,
		encodeMergeValue(update_nextnode, 0, statehash))

	if cmt.refGS != nil || cmt.refGSAdded != nil {
		//we clear the wb in cmt, so cmt.clear never release the write batch
		ret := &gscommiter{}
		*ret = *cmt
		cmt.WriteBatch = nil
		return ret, nil
	}

	err = txdb.BatchCommit(wb)
	if err != nil {
		return nil, fmt.Errorf("commit on node [%x] was ruined: %s", parentStateHash, err)
	}

	return nil, nil

}

func (txdb *GlobalDataDB) walkOnGraph(sn *gorocksdb.Snapshot, target []byte,
	di func(*protos.GlobalState) []byte, c chan *protos.GlobalState) {

	var gs *protos.GlobalState
	for ; target != nil; target = di(gs) {

		gsbyte, err := txdb.getFromSnapshot(sn, txdb.globalCF, target)
		if gsbyte == nil {
			dbLogger.Errorf("Could not found state [%x]", target)
			break
		}

		gs, err = protos.UnmarshallGS(gsbyte)
		if err != nil {
			dbLogger.Errorf("Decode state [%x] fail: %s", target, err)
			break
		}

		c <- gs
	}

	close(c)
}

//start from a gs which will change into a node in graph, we update the path which it was in
func (txdb *GlobalDataDB) updatePath(sn *gorocksdb.Snapshot, wb *gorocksdb.WriteBatch,
	refGS *protos.GlobalState, refGSKey []byte) {

	//update backward for their "nextbranch"
	cfront := make(chan *protos.GlobalState, 8)

	gskey := refGS.ParentNode()
	go txdb.walkOnGraph(sn, gskey, (*protos.GlobalState).ParentNode, cfront)

	for gs := range cfront {
		wb.MergeCF(txdb.globalCF, gskey, encodeMergeValue(update_nextbranch,
			refGS.Count, refGSKey))
		gskey = gs.ParentNode()
	}

	//update forward for their "lastbranch"
	cback := make(chan *protos.GlobalState, 8)

	gskey = refGS.NextNode()
	go txdb.walkOnGraph(sn, gskey, (*protos.GlobalState).NextNode, cback)

	for gs := range cback {
		wb.MergeCF(txdb.globalCF, gskey, encodeMergeValue(update_lastbranch,
			gs.Count-refGS.Count, refGSKey))
		gskey = gs.NextNode()
	}
}

func (txdb *GlobalDataDB) AddGlobalState(parentStateHash []byte, statehash []byte) error {

	cm, err := txdb.addGSCritical(parentStateHash, statehash)
	defer cm.clear()

	if err != nil {
		return err
	}

	if cm == nil { //done
		return nil
	}

	//create snapshot, start updating on gs-tree
	sn := txdb.NewSnapshot()
	defer txdb.ReleaseSnapshot(sn)

	if cm.refGS != nil {
		var targetHash []byte
		if cm.tailCase {
			targetHash = statehash
		} else {
			targetHash = parentStateHash
		}

		//parent itself should be update for "nextbrach"
		cm.MergeCF(txdb.globalCF, parentStateHash, encodeMergeValue(update_nextbranch,
			cm.refGS.Count, targetHash))
		txdb.updatePath(sn, cm.WriteBatch, cm.refGS, targetHash)
	}

	if cm.refGSAdded != nil {
		//(overlapped) gs itself should be update for "last brach"
		cm.MergeCF(txdb.globalCF, statehash, encodeMergeValue(update_lastbranch,
			0, statehash))
		txdb.updatePath(sn, cm.WriteBatch, cm.refGSAdded, statehash)
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

func (openchainDB *GlobalDataDB) ListCheckpoints() (ret [][]byte) {

	ret = make([][]byte, 0)

	it := openchainDB.GetIterator(PersistCF)
	defer it.Close()

	prefix := []byte(checkpointNamePrefix)

	for it.Seek([]byte(prefix)); it.ValidForPrefix(prefix); it.Next() {
		//Value/Key() in iterator need not to be Free() but its Data()
		//must be copied
		//ret = append(ret, it.Value().Data()) --- THIS IS WRONG
		ret = append(ret, makeCopy(it.Value().Data()))
	}

	return
}

func (openchainDB *GlobalDataDB) GetDBVersion() int {
	v, _ := openchainDB.GetValue(PersistCF, []byte(currentVersionKey))
	if len(v) == 0 {
		return 0
	}

	return int(v[0])
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

	//if you connect root gs from tail, you got some wired situtation:
	//there is a cycle without any node so walkPath method will not stop
	//we avoid this case by setting a "dummy" gs which is never accessed
	if txdb.useCycleGraph {
		dummygs := protos.NewGlobalState()
		dummygs.NextNodeStateHash = [][]byte{statehash}
		v, err := dummygs.Bytes()
		if err != nil {
			return err
		}
		err = txdb.PutValue(GlobalCF, []byte("DUMMYGS#"), v)
		if err != nil {
			return err
		}
		newgs.ParentNodeStateHash = [][]byte{[]byte("DUMMYGS#")}
	}

	v, err := newgs.Bytes()

	if err == nil {
		err = txdb.PutValue(GlobalCF, []byte(statehash), v)
	}
	return err
}
