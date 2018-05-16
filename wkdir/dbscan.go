/*
Copyright BlackPai Corp. 2016 All Rights Reserved.

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

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/protos"
	"github.com/spf13/viper"
	"github.com/abchain/fabric/flogging"
	"github.com/op/go-logging"
	"github.com/tecbot/gorocksdb"
)

var logger = logging.MustGetLogger("dbscan")

type detailPrinter func(data []byte)

func test() {

	var start int = 0
	var end int = 21

	for {
		target := (start + end) / 2
		fmt.Printf("start<%d>, end<%d>,  %d\n", start, end, target)

		end = target

		newTarget := (start + end) / 2
		if target == newTarget {
			break
		}
	}

}

func main() {
	flagSetName := os.Args[0]
	flagSet := flag.NewFlagSet(flagSetName, flag.ExitOnError)
	dbDirPtr := flagSet.String("dbpath", "", "path to db dump")
	flagSet.Parse(os.Args[1:])

	dbDir := *dbDirPtr

	//test()
	//return


	fmt.Printf("dbDir = [%s]\n", dbDir)

	flogging.LoggingInit("client")

	if len(dbDir) == 0 {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", flagSetName)
		flagSet.PrintDefaults()
		os.Exit(3)
	}

	viper.Set("peer.fileSystemPath", dbDir)

	// ensure dbDir exists
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		fmt.Printf("<%s> does not exist\n", dbDir)
		os.Exit(4)
	}

	if _, err := os.Stat(dbDir + "/db"); os.IsNotExist(err) {
		fmt.Printf("<%s> does not contain a sub-dir named 'db'\n", dbDir)
		os.Exit(5)
	}

	if _, err := os.Stat(dbDir + "/txdb"); os.IsNotExist(err) {
		fmt.Printf("<%s> does not contain a sub-dir named 'txdb'\n", dbDir)
		os.Exit(5)
	}

	db.Start()

	orgdb := db.GetDBHandle()
	txdb := db.GetGlobalDBHandle()

	defer db.Stop()

	scan(orgdb.GetIterator(db.IndexesCF).Iterator, db.IndexesCF, nil)
	scan(orgdb.GetIterator(db.BlockchainCF).Iterator, db.BlockchainCF, blockDetailPrinter)
	scan(orgdb.GetIterator(db.PersistCF).Iterator, db.PersistCF, nil)
	scan(orgdb.GetIterator(db.StateDeltaCF).Iterator, db.StateDeltaCF, nil)
	scan(orgdb.GetIterator(db.StateCF).Iterator, db.StateCF, nil)

	scan(txdb.GetIterator(db.TxCF), db.TxCF, txDetailPrinter)
	scan(txdb.GetIterator(db.GlobalCF), db.GlobalCF, gsDetailPrinter)
	scan(txdb.GetIterator(db.PersistCF), db.PersistCF, nil)
}

func scan(itr *gorocksdb.Iterator, cfName string, printer detailPrinter) {

	if itr == nil {
		return
	}
	fmt.Printf("\n================================================================\n")
	fmt.Printf("====== Dump %s: ====== \n", cfName)
	fmt.Printf("================================================================\n")

	totalKVs := 0
	itr.SeekToFirst()
	for ; itr.Valid(); itr.Next() {
		k := itr.Key()
		v := itr.Value()
		keyBytes := k.Data()

		var keyName string
		if cfName == db.TxCF || cfName == db.PersistCF {
			keyName = string(keyBytes)
		} else {
			keyName = fmt.Sprintf("%x", keyBytes)
		}

		fmt.Printf("Index<%d>: key=[%s], value=[%x]\n", totalKVs, keyName, v.Data())
		//fmt.Printf("Index<%d>: «key=[%s], value=[%x]\n", totalKVs, keyName, v.Data())
		if printer != nil {
			fmt.Println("    Value Details:")
			printer(v.Data())
			fmt.Println("")
		}

		k.Free()
		v.Free()
		totalKVs++

	}
	itr.Close()

	return
}

func txDetailPrinter(valueBytes []byte) {

	v, err := protos.UnmarshallTransaction(valueBytes)

	if err != nil {
		return
	}

	fmt.Printf("	Txid = [%s]\n", v.Txid)
	fmt.Printf("	Payload = [%x]\n", v.Payload)
}

func blockDetailPrinter(blockBytes []byte) {

	block, err := protos.UnmarshallBlock(blockBytes)

	if err != nil {
		return
	}

	fmt.Printf("	Number of transactions = [%d]\n", len(block.Transactions))
	fmt.Printf("	Number of txid = [%d]\n", len(block.Txids))
	fmt.Printf("	block version = [%d]\n", block.Version)
	fmt.Printf("	StateHash = [%x]\n", block.StateHash)
}

func gsDetailPrinter(inputBytes []byte) {

	gs, err := protos.UnmarshallGS(inputBytes)

	if err != nil {
		return
	}

	fmt.Printf("	Count = [%d]\n", gs.Count)
	fmt.Printf("	LastBranchNodeStateHash = [%x]\n", gs.LastBranchNodeStateHash)
	fmt.Printf("	ParentNodeStateHash = [%+x]\n", gs.ParentNodeStateHash)
	fmt.Printf("	Branched = [%t]\n", gs.Branched())
	fmt.Printf("	NextNodeStateHash count = [%d]\n", len(gs.NextNodeStateHash))
}
