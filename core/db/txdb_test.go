package db

import (
	"bytes"
	"github.com/abchain/fabric/core/util"
	pb "github.com/abchain/fabric/protos"
	"math/rand"
	"testing"
	"time"
)

var populatedTxCnt = 32

func compareTx(a *pb.Transaction, b *pb.Transaction) bool {
	if a == nil || b == nil {
		return false
	}
	return a.Txid == b.Txid && bytes.Compare(a.Payload, b.Payload) == 0
}

func TestTxDBRW(t *testing.T) {

	Start()
	defer deleteTestDBPath()
	defer Stop()

	//populate txs
	txs := make([]*pb.Transaction, populatedTxCnt)
	txids := make([]string, populatedTxCnt)
	for i, _ := range txs {
		txs[i] = new(pb.Transaction)
		tx := txs[i]
		tx.Txid = util.GenerateUUID()
		tx.Payload = util.GenerateBytesUUID()
		txids[i] = tx.Txid
	}

	txmap := make(map[string]*pb.Transaction)
	for _, tx := range txs {
		txmap[tx.Txid] = tx
	}

	err := globalDataDB.PutTransactions(txs)
	if err != nil {
		t.Fatal("put tx fail:", err)
	}

	//shulffle txids (our go version have not shuffle yet)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i, tx := range txids[:populatedTxCnt-1] {
		mv := populatedTxCnt - r.Intn(populatedTxCnt-i) - 1
		txids[i] = txids[mv]
		txids[mv] = tx
	}

	tx, err := globalDataDB.GetTransaction(txids[0])
	if err != nil || tx == nil {
		t.Fatal("get singele tx fail:", err)
	}

	if !compareTx(tx, txmap[txids[0]]) {
		t.Fatal("Wrong tx:", tx, "expected:", txmap[txids[0]])
	}

	tx, err = globalDataDB.GetTransaction("unexistTxid")
	if err != nil || tx != nil {
		t.Fatal("get unexisted singele tx fail:", err)
	}

	gtxs := globalDataDB.GetTransactions(txids[:10])
	if len(gtxs) < 10 {
		t.Fatal("get mutiple tx fail:", err)
	}

	for i, gtx := range gtxs {
		if !compareTx(gtx, txmap[txids[i]]) {
			t.Fatal("Wrong tx:", gtx, "expected:", txmap[txids[i]])
		}
	}

	gtxs = globalDataDB.GetTransactions(txids)
	if len(gtxs) < len(txids) {
		t.Fatal("get all txs fail:", err)
	}

	for i, gtx := range gtxs {
		if !compareTx(gtx, txmap[txids[i]]) {
			t.Fatal("Wrong tx:", gtx, "expected:", txmap[txids[i]])
		}
	}

	unexistTxid := []string{"unexisted1", "unexisted2", "unexisted3"}

	gtxs = globalDataDB.GetTransactions(unexistTxid)
	if len(gtxs) != 0 {
		t.Fatal("get unexisted txs fail:", err)
	}

	gtxs = globalDataDB.GetTransactions(append(txids, unexistTxid...))
	if len(gtxs) != len(txids) {
		t.Fatal("get all txs fail:", err)
	}

	for i, gtx := range gtxs {
		if !compareTx(gtx, txmap[txids[i]]) {
			t.Fatal("Wrong tx:", gtx, "expected:", txmap[txids[i]])
		}
	}
}

func dumpCF(t *testing.T) {
	t.Log("------Current CF-----")
	itr := globalDataDB.GetIterator(GlobalCF)
	if itr == nil {
		t.Fatal("No iterator!")
	}
	defer itr.Close()

	itr.SeekToFirst()
	for ; itr.Valid(); itr.Next() {

		k := itr.Key()
		v := itr.Value()
		defer k.Free()
		defer v.Free()

		gs, err := pb.UnmarshallGS(v.Data())

		if err != nil {
			t.Fatal("Wrong data in gs")
		}

		t.Log(string(k.Data()), gs)
	}

	t.Log("---------------------")
}

func TestTxDBGlobalStateRW(t *testing.T) {

	Start()
	defer deleteTestDBPath()
	defer Stop()

	//populated global states
	//we have such a graphic:
	//                                       |--<root7>
	//                                       |
	// <stateroot> ---- <root1> ---- <root2> --- <root3>
	//               |
	//               |--<root4> ---- <root5>
	//               |
	//               |-<root6>
	//
	// The adjacent matrix:
	adjm := map[string][]string{
		"stateroot": []string{"root1", "root4", "root6"},
		"root1":     []string{"root2"},
		"root2":     []string{"root3", "root7"},
		"root3":     nil,
		"root4":     []string{"root5"},
		"root5":     nil,
		"root6":     nil,
		"root7":     nil,
	}

	addTask := func(curtasks [][2]string, state string) [][2]string {
		nodes, ok := adjm[state]
		if !ok {
			t.Fatal("Not valid state", state)
		}

		for _, newnode := range nodes {
			curtasks = append(curtasks, [2]string{state, newnode})
		}

		return curtasks
	}

	//random populating ...
	err := globalDataDB.PutGenesisGlobalState([]byte("stateroot"))
	if err != nil {
		t.Fatal("Add state fail", err)
	}

	r := rand.New(rand.NewSource(time.Now().Unix()))
	curTasks := addTask(nil, "stateroot")

	dumpCF(t)
	for len(curTasks) != 0 {

		rpos := r.Intn(len(curTasks))
		tsk := curTasks[rpos]
		newstate := tsk[1]

		t.Log("Add state", tsk)

		err = globalDataDB.AddGlobalState([]byte(tsk[0]), []byte(newstate))
		if err != nil {
			t.Fatal("Add state fail", err)
		}

		dumpCF(t)
		//switch with tail ...
		if rpos != len(curTasks)-1 {
			curTasks[rpos][0] = curTasks[len(curTasks)-1][0]
			curTasks[rpos][1] = curTasks[len(curTasks)-1][1]
		}

		curTasks = addTask(curTasks[:len(curTasks)-1], newstate)
	}

	//test
	gsr := globalDataDB.GetGlobalState([]byte())
}
