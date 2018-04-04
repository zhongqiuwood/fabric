package db

import (
	"bytes"
	"github.com/tecbot/gorocksdb"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func TestCheckPoint(t *testing.T) {

	orgdb, orgdir := newTestDB("orgdir", nil)
	t.Log("orgdir:", orgdir)

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	givenVal := []byte("valaa")
	wo := gorocksdb.NewDefaultWriteOptions()
	for _, k := range givenKeys {
		orgdb.Put(wo, k, givenVal)
	}

	suffix := "checkpoint"
	newdir, err := ioutil.TempDir("", "gorocksdb-"+suffix)
	newdir = filepath.Join(newdir, "1")
	t.Log("checkpoint dir:", newdir)

	var newdb *gorocksdb.DB
	var checkpoint *gorocksdb.Checkpoint

	checkpoint, _ = orgdb.NewCheckpoint()
	defer checkpoint.Destroy()

	err = checkpoint.CreateCheckpoint(newdir, 0)
	if err != nil {
		t.Fatal("create fail:", err)
	}
	orgdb.Close()

	opts := gorocksdb.NewDefaultOptions()
	newdb, err = gorocksdb.OpenDb(opts, newdir)
	defer newdb.Close()
	if err != nil {
		t.Fatal("open fail:", err)
	}

	// test keys
	var value *gorocksdb.Slice
	ro := gorocksdb.NewDefaultReadOptions()
	for _, k := range givenKeys {
		value, err = newdb.Get(ro, k)
		defer value.Free()
		if bytes.Compare(value.Data(), givenVal) != 0 {
			t.Fatalf("cp: <%s><%x>, givenVal:<%x>\n", k, value.Data(), givenVal)
		}
	}

}

func newTestDB(name string, applyOpts func(opts *gorocksdb.Options)) (*gorocksdb.DB, string) {
	dir, _ := ioutil.TempDir("", "gorocksdb-"+name)
	opts := gorocksdb.NewDefaultOptions()
	rateLimiter := gorocksdb.NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	db, _ := gorocksdb.OpenDb(opts, dir)
	return db, dir
}
