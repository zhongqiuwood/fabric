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

	"github.com/golang/protobuf/proto"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/protos"
	"github.com/spf13/viper"
	//"github.com/tecbot/gorocksdb"
	"github.com/abchain/fabric/dbg"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/flogging"
)

const (
	//MaxValueSize is used to compare the size of the key-value in db.
	//If a key-value is more than this size, it's details are printed for further analysis.
	MaxValueSize = 1024 * 1024
)

type detailPrinter func(data []byte)

func main() {
	flagSetName := os.Args[0]
	flagSet := flag.NewFlagSet(flagSetName, flag.ExitOnError)
	dbDirPtr := flagSet.String("dbpath", "/var/hyperledger/production0", "path to db dump")
	modePtr := flagSet.String("mode", "q", "q | r. query or reorganize database, query by default")
	flagSet.Parse(os.Args[1:])

	dbDir := *dbDirPtr
	mode := *modePtr


	fmt.Printf("dbDir = [%s], mode=%s\n", dbDir, mode)

	dbg.Init()
	flogging.LoggingInit("scandb")

	if dbDir == "" || (mode != "q" && mode != "r") {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", flagSetName)
		flagSet.PrintDefaults()
		os.Exit(3)
	}

	//viper.Set("	peer.db.version", "1")
	viper.Set("peer.fileSystemPath", dbDir)

	// check that dbDir exists
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		fmt.Printf("<%s> does not exist\n",  dbDir)
		os.Exit(4)
	}

	if _, err := os.Stat(dbDir + "/db"); os.IsNotExist(err) {
		fmt.Printf("<%s> does not contain a sub-dir named 'db'\n", dbDir)
		os.Exit(5)
	}


	protos.CurrentDbVersion = 1
	db.Start(protos.CurrentDbVersion)

	orgdb := db.GetDataBaseHandler(0)
	txdb := db.GetDataBaseHandler(1)

	defer db.Stop(protos.CurrentDbVersion)
	if mode == "r" {
		fmt.Printf("------------------------------Before InitializeDataBase-----------------------------\n")
	}

	scandb(orgdb, txdb)

	if mode == "r" {
		fmt.Printf("------------------------------Start InitializeDataBase-----------------------------\n")

		res := ledger.InitializeDataBase(orgdb, txdb)

		if !res {
			return
		}

		fmt.Printf("------------------------------InitializeDataBase done!-----------------------------\n")
		fmt.Printf("---------------------------Start rescan db...------------------------------\n")
		scandb(orgdb, txdb)
	}

	fmt.Println()
}

func scandb(orgdb db.IDataBaseHandler, txdb db.IDataBaseHandler) {

	fmt.Printf("-------scandb in--------\n")

	fmt.Printf("---------------------------------------------------------------------------------------------------------------------------------------------------------\n")
	fmt.Printf("--------------------------------------------------------------------------------------scan db------------------------------------------------------------\n")

	scan(orgdb, db.IndexesCF, nil)
	scan(orgdb, db.BlockchainCF, blockDetailPrinter)
	scan(orgdb, db.PersistCF, nil)

	fmt.Printf("---------------------------------------------------------------------------------------------------------------------------------------------------------\n")
	fmt.Printf("--------------------------------------------------------------------------------------scan txdb------------------------------------------------------------\n")

	scan(txdb, db.PersistCF, nil)
	scan(txdb, db.TxCF, nil)
	scan(txdb, db.GlobalCF, nil)
	fmt.Printf("-------scandb out--------\n")
}

func scan(openchainDB db.IDataBaseHandler, cfName string, printer detailPrinter) (int, int) {
	fmt.Printf("\n===================================================================================\n")
	fmt.Printf("|============================ dump %s.%s=======================\n", openchainDB.GetDbName(), cfName)

	itr := openchainDB.GetIterator(cfName)
	totalKVs := 0
	itr.SeekToFirst()
	for ; itr.Valid(); itr.Next() {
		k := itr.Key()
		v := itr.Value()
		keyBytes := k.Data()
		totalKVs++

		var keyName string
		if cfName == db.TxCF {
			keyName = string(keyBytes)
		} else if cfName == db.PersistCF {
			keyName = string(keyBytes)
		} else if cfName == db.GlobalCF {
			keyName = dbg.Byte2string(keyBytes)
			//keyName = string(keyBytes)
		} else {
			keyName = dbg.Byte2string(keyBytes)
		}

		fmt.Printf("<%d>: key=[%s], value=[%x]\n", totalKVs, keyName, v.Data())
		if printer != nil {
			fmt.Println("=== KV Details === ")
			printer(v.Data())
			fmt.Println("")
		}

		k.Free()
		v.Free()
	}
	itr.Close()


	fmt.Printf("|======================== dump %s.%s done. %d totalKVs=======================\n", openchainDB.GetDbName(), cfName, totalKVs)
	fmt.Printf("===================================================================================\n\n")
	return totalKVs, 0
}

func blockDetailPrinter(blockBytes []byte) {

	block, err := protos.UnmarshallBlock(blockBytes)

	if err != nil {
		return
	}
	fmt.Printf("%+v", block)

	txs := block.GetTransactions()
	fmt.Printf("\nNumber of transactions = [%d]\n", len(txs))
	fmt.Printf("Number of txid = [%d]\n", len(block.Txids))
	fmt.Printf("block version = [%d]\n", block.Version)
	for _, tx := range txs {
		cIDBytes := tx.ChaincodeID
		cID := &protos.ChaincodeID{}
		proto.Unmarshal(cIDBytes, cID)
		fmt.Printf("TxDetails: payloadSize=[%d], tx.Type=[%s], cID.Name=[%s], cID.Path=[%s]\n",
			len(tx.Payload), tx.Type, cID.Name, cID.Path)
	}
}
