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
	"github.com/abchain/fabric/core/util"
	"github.com/spf13/viper"
	"github.com/tecbot/gorocksdb"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	setupTestConfig()
	os.Exit(m.Run())
}

func TestGetDBPathEmptyPath(t *testing.T) {
	dbPathSetting = ""
	originalSetting := viper.GetString("peer.fileSystemPath")
	viper.Set("peer.fileSystemPath", "")
	defer func() {
		x := recover()
		if x == nil {
			t.Fatal("A panic should have been caused here.")
		}
	}()
	defer viper.Set("peer.fileSystemPath", originalSetting)
	Start()
	GetDBHandle()
}

func TestStartDB_DirDoesNotExist(t *testing.T) {
	deleteTestDBPath()

	defer deleteTestDBPath()
	defer Stop()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Failed to open DB: %s", r)
		}
	}()
	Start()
}

func TestStartDB_NonEmptyDirExists(t *testing.T) {

	t.Skip("gorocksdb may have different behavior now, skip this test")
	deleteTestDBPath()
	createNonEmptyTestDBPath()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("dbPath is already exists. DB open should throw error")
		}
	}()
	Start()
}

func TestWriteAndRead(t *testing.T) {
	deleteTestDBPath()
	Start()
	defer deleteTestDBPath()
	defer Stop()
	performBasicReadWrite(originalDB, t)
}

func TestStandaloneDB(t *testing.T) {
	deleteTestDBPath()
	db, err := StartDB("test", viper.Sub("peer.db"))
	if err != nil {
		t.Fatal("start db fail", err)
	}
	defer deleteTestDBPath()
	defer StopDB(db)
	defer Stop()
	performBasicReadWrite(db, t)
}

// This test verifies that when a new column family is added to the DB
// users at an older level of the DB will still be able to open it with new code
func TestDBColumnUpgrade(t *testing.T) {
	deleteTestDBPath()
	Start()
	Stop()

	oldcfs := columnfamilies
	columnfamilies = append([]string{"Testing"}, columnfamilies...)
	defer func() {
		columnfamilies = oldcfs
	}()

	defer deleteTestDBPath()
	defer Stop()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Error re-opening DB with upgraded columnFamilies")
		}
	}()
	Start()
}

func TestDeleteState(t *testing.T) {
	testDBWrapper := NewTestDBWrapper()
	testDBWrapper.CleanDB(t)
	openchainDB := GetDBHandle()
	defer testDBWrapper.cleanup()
	openchainDB.PutValue(StateCF, []byte("key1"), []byte("value1"))
	openchainDB.PutValue(StateDeltaCF, []byte("key2"), []byte("value2"))
	openchainDB.DeleteState()
	value1, err := openchainDB.GetValue(StateCF, []byte("key1"))
	if err != nil {
		t.Fatalf("Error getting in value: %s", err)
	}
	if value1 != nil {
		t.Fatalf("A nil value expected. Found [%s]", value1)
	}

	value2, err := openchainDB.GetValue(StateCF, []byte("key2"))
	if err != nil {
		t.Fatalf("Error getting in value: %s", err)
	}
	if value2 != nil {
		t.Fatalf("A nil value expected. Found [%s]", value2)
	}
}

func TestDBSnapshot(t *testing.T) {
	testDBWrapper := NewTestDBWrapper()
	testDBWrapper.CleanDB(t)
	openchainDB := GetDBHandle()
	defer testDBWrapper.cleanup()

	// write key-values
	openchainDB.PutValue(BlockchainCF, []byte("key1"), []byte("value1"))
	openchainDB.PutValue(BlockchainCF, []byte("key2"), []byte("value2"))

	// create a snapshot
	snapshot := openchainDB.GetSnapshot()

	defer snapshot.Release()

	// add/delete/modify key-values
	openchainDB.DeleteKey(BlockchainCF, []byte("key1"))
	openchainDB.PutValue(BlockchainCF, []byte("key2"), []byte("value2_new"))
	openchainDB.PutValue(BlockchainCF, []byte("key3"), []byte("value3"))

	// test key-values from latest data in db
	v1, _ := openchainDB.GetValue(BlockchainCF, []byte("key1"))
	v2, _ := openchainDB.GetValue(BlockchainCF, []byte("key2"))
	v3, _ := openchainDB.GetValue(BlockchainCF, []byte("key3"))
	if !bytes.Equal(v1, nil) {
		t.Fatalf("Expected value from db is 'nil', found [%s]", v1)
	}
	if !bytes.Equal(v2, []byte("value2_new")) {
		t.Fatalf("Expected value from db [%s], found [%s]", "value2_new", v2)
	}
	if !bytes.Equal(v3, []byte("value3")) {
		t.Fatalf("Expected value from db [%s], found [%s]", "value3", v3)
	}

	// test key-values from snapshot
	v1, _ = snapshot.GetFromBlockchainCFSnapshot([]byte("key1"))
	v2, _ = snapshot.GetFromBlockchainCFSnapshot([]byte("key2"))
	v3, err := snapshot.GetFromBlockchainCFSnapshot([]byte("key3"))
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	if !bytes.Equal(v1, []byte("value1")) {
		t.Fatalf("Expected value from db snapshot [%s], found [%s]", "value1", v1)
	}

	if !bytes.Equal(v2, []byte("value2")) {
		t.Fatalf("Expected value from db snapshot [%s], found [%s]", "value1", v2)
	}

	if !bytes.Equal(v3, nil) {
		t.Fatalf("Expected value from db snapshot is 'nil', found [%s]", v3)
	}
}

func TestDBIteratorAndSnapshotIterator(t *testing.T) {
	testDBWrapper := NewTestDBWrapper()
	testDBWrapper.CleanDB(t)
	openchainDB := GetDBHandle()
	defer testDBWrapper.cleanup()

	// write key-values
	openchainDB.PutValue(StateCF, []byte("key1"), []byte("value1"))
	openchainDB.PutValue(StateCF, []byte("key2"), []byte("value2"))

	// create a snapshot
	snapshot := openchainDB.GetSnapshot()

	// add/delete/modify key-values
	openchainDB.DeleteKey(StateCF, []byte("key1"))
	openchainDB.PutValue(StateCF, []byte("key2"), []byte("value2_new"))
	openchainDB.PutValue(StateCF, []byte("key3"), []byte("value3"))

	// test snapshot iterator
	itr := snapshot.GetStateCFSnapshotIterator()
	defer itr.Close()
	testIterator(t, itr, map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2")})

	// test iterator over latest data in stateCF
	dbitr := openchainDB.GetIterator(StateCF)
	defer dbitr.Close()
	testIterator(t, dbitr.Iterator, map[string][]byte{"key2": []byte("value2_new"), "key3": []byte("value3")})

	openchainDB.PutValue(StateDeltaCF, []byte("key4"), []byte("value4"))
	openchainDB.PutValue(StateDeltaCF, []byte("key5"), []byte("value5"))
	dbitr = openchainDB.GetIterator(StateDeltaCF)
	defer dbitr.Close()
	testIterator(t, dbitr.Iterator, map[string][]byte{"key4": []byte("value4"), "key5": []byte("value5")})

	openchainDB.PutValue(BlockchainCF, []byte("key6"), []byte("value6"))
	openchainDB.PutValue(BlockchainCF, []byte("key7"), []byte("value7"))
	dbitr = openchainDB.GetIterator(BlockchainCF)
	defer dbitr.Close()
	testIterator(t, dbitr.Iterator, map[string][]byte{"key6": []byte("value6"), "key7": []byte("value7")})
}

// db helper functions
func testIterator(t *testing.T, itr *gorocksdb.Iterator, expectedValues map[string][]byte) {
	itrResults := make(map[string][]byte)
	itr.SeekToFirst()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		k := makeCopy(key.Data())
		v := makeCopy(value.Data())
		itrResults[string(k)] = v
	}
	if len(itrResults) != len(expectedValues) {
		t.Fatalf("Expected [%d] results from iterator, found [%d]", len(expectedValues), len(itrResults))
	}
	for k, v := range expectedValues {
		if !bytes.Equal(itrResults[k], v) {
			t.Fatalf("Wrong value for key [%s]. Expected [%s], found [%s]", k, itrResults[k], v)
		}
	}
}

func createNonEmptyTestDBPath() {
	dbPath := util.CanonicalizePath(viper.GetString("peer.fileSystemPath"))
	os.MkdirAll(filepath.Join(dbPath, "db", "tmpFile"), 0775)
	os.MkdirAll(filepath.Join(dbPath, "txdb"), 0775)
}

func deleteTestDBPath() {
	dbPath := viper.GetString("peer.fileSystemPath")
	os.RemoveAll(dbPath)
}

func setupTestConfig() {
	tempDir, err := ioutil.TempDir("", "fabric-db-test")
	if err != nil {
		panic(err)
	}
	viper.Set("peer.fileSystemPath", tempDir)
	deleteTestDBPath()
}

func performBasicReadWrite(openchainDB *OpenchainDB, t *testing.T) {

	writeBatch := openchainDB.NewWriteBatch()
	defer writeBatch.Destroy()
	dbh := writeBatch.GetDBHandle()
	writeBatch.PutCF(dbh.BlockchainCF, []byte("dummyKey"), []byte("dummyValue"))
	writeBatch.PutCF(dbh.StateCF, []byte("dummyKey1"), []byte("dummyValue1"))
	writeBatch.PutCF(dbh.StateDeltaCF, []byte("dummyKey2"), []byte("dummyValue2"))
	writeBatch.PutCF(dbh.IndexesCF, []byte("dummyKey3"), []byte("dummyValue3"))
	err := writeBatch.BatchCommit()
	if err != nil {
		t.Fatalf("Error while writing to db: %s", err)
	}
	value, err := openchainDB.GetValue(BlockchainCF, []byte("dummyKey"))
	if err != nil {
		t.Fatalf("read error = [%s]", err)
	}
	if !bytes.Equal(value, []byte("dummyValue")) {
		t.Fatalf("read error. Bytes not equal. Expected [%s], found [%s]", "dummyValue", value)
	}

	value, err = openchainDB.GetValue(StateCF, []byte("dummyKey1"))
	if err != nil {
		t.Fatalf("read error = [%s]", err)
	}
	if !bytes.Equal(value, []byte("dummyValue1")) {
		t.Fatalf("read error. Bytes not equal. Expected [%s], found [%s]", "dummyValue1", value)
	}

	value, err = openchainDB.GetValue(StateDeltaCF, []byte("dummyKey2"))
	if err != nil {
		t.Fatalf("read error = [%s]", err)
	}
	if !bytes.Equal(value, []byte("dummyValue2")) {
		t.Fatalf("read error. Bytes not equal. Expected [%s], found [%s]", "dummyValue2", value)
	}

	value, err = openchainDB.GetValue(IndexesCF, []byte("dummyKey3"))
	if err != nil {
		t.Fatalf("read error = [%s]", err)
	}
	if !bytes.Equal(value, []byte("dummyValue3")) {
		t.Fatalf("read error. Bytes not equal. Expected [%s], found [%s]", "dummyValue3", value)
	}
}

//
//func (txdb *GlobalDataDB) DumpGlobalState() {
//	itr := txdb.GetIterator(GlobalCF)
//	defer itr.Close()
//
//	idx := 0
//	itr.SeekToFirst()
//
//	hashGSMap := make(map[string]*protos.GlobalState)
//	var genesis *protos.GlobalState
//
//	for ; itr.Valid(); itr.Next() {
//		k := itr.Key()
//		v := itr.Value()
//		keyBytes := k.Data()
//		idx++
//
//		gs, _:= protos.UnmarshallGS(v.Data())
//		hashGSMap[dbg.Byte2string(keyBytes)] = gs
//
//		if gs.Count == 0 {
//			genesis = gs
//		}
//		k.Free()
//		v.Free()
//	}
//
//	cur := genesis
//	sh := cur.ParentNodeStateHash
//	sh = nil
//	for {
//		dbg.Infof("%d, <%x>", cur.Count, xxx(sh, 3))
//		//dbg.Infof("%d, <%x>, <%x>-------|", cur.Count, xxx(sh, 3), xxx(sh, 3))
//
//		if cur.NextNodeStateHash != nil {
//			key := cur.NextNodeStateHash[0]
//			cur = hashGSMap[dbg.Byte2string(key)]
//			sh = key
//		} else {
//			break
//		}
//	}
//}

func xxx(bkey []byte, num int) []byte {

	if bkey == nil {
		return nil
	}
	return bkey[0:num]
}
