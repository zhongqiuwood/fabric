package main

import (
	"github.com/abchain/fabric/core/db"
	"fmt"
	"strconv"
	"os"
	"flag"
	"github.com/spf13/viper"
	"github.com/tecbot/gorocksdb"
	"github.com/op/go-logging"
	"github.com/abchain/fabric/protos"
	"github.com/abchain/fabric/core/util"
	_ "github.com/abchain/fabric/core/ledger/statemgmt/buckettree"
	"github.com/abchain/fabric/core/ledger/statemgmt/buckettree"
	node "github.com/abchain/fabric/node/start"

)

var logger = logging.MustGetLogger("dbscan")

type detailPrinter func(data []byte)


func main() {
	flagSetName := os.Args[0]
	flagSet := flag.NewFlagSet(flagSetName, flag.ExitOnError)
	dbDirPtr := flagSet.String("dbpath", "", "path to db dump")
	switchPtr := flagSet.String("switch", "", "statehash for db to switch")
	dumpBlockPtr := flagSet.String("block", "", "dump db")
	dumpStatePtr := flagSet.String("state", "", "dump db")
	flagSet.Parse(os.Args[1:])

	dbDir := *dbDirPtr
	switchTarget := *switchPtr

	fmt.Printf("dbDir = [%s]\n", dbDir)
	fmt.Printf("switch to = [%s]\n", switchTarget)

	switchTargetNum, _ := strconv.Atoi(switchTarget)

	_ = switchTargetNum

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

	if _, err := os.Stat(dbDir + "/txdb"); os.IsNotExist(err) {
		fmt.Printf("<%s> does not contain a sub-dir named 'txdb'\n", dbDir)
		os.Exit(5)
	}

	reg := func() error {

		orgdb := db.GetDBHandle()
		txdb := db.GetGlobalDBHandle()

		if len(*dumpStatePtr) > 0 {
			dumpBreakPointHash(-1)
		}

		if len(*dumpBlockPtr) > 0 {
			scan(orgdb.GetIterator(db.IndexesCF).Iterator, db.IndexesCF, nil)
			scan(orgdb.GetIterator(db.BlockchainCF).Iterator, db.BlockchainCF, blockDetailPrinter)
			scan(orgdb.GetIterator(db.StateCF).Iterator, db.StateCF, nil)
			scan(orgdb.GetIterator(db.StateDeltaCF).Iterator, db.StateDeltaCF, nil)
			scan(orgdb.GetIterator(db.PersistCF).Iterator, db.PersistCF, nil)
			scan(txdb.GetIterator(db.TxCF), db.TxCF, txDetailPrinter)
			scan(txdb.GetIterator(db.GlobalCF), db.GlobalCF, gsDetailPrinter)
			scan(txdb.GetIterator(db.PersistCF), db.PersistCF, nil)
		}
		os.Exit(0)
		return nil
	}

	node.RunNode(&node.NodeConfig{PostRun: reg})
}

func dumpBucketNodes(expectedLevel int) {
	output("=========================================================")
	output("== Dump level[%d]:", expectedLevel)
	output("=========================================================")
	maxNum := buckettree.BucketTreeConfig().GetNumBuckets(expectedLevel)
	for i := 1; i <= maxNum; i++ {

		output("== Dump level[%d][%d]:", expectedLevel, i)
		buckettree.ComputeBreakPointHash(expectedLevel, i, nil)
	}
}


func dumpBreakPointHash(level int) {

	if level < 0 {
		// dump all levels
		for levelIdx := 0; levelIdx <= buckettree.BucketTreeConfig().GetLowestLevel(); levelIdx++{
			dumpBucketNodes(levelIdx)
		}
	} else {
		// dump specified level
		dumpBucketNodes(level)
	}
}

func output(format string, a ...interface{}) (n int, err error) {
	format += "\n"
	return fmt.Printf(format, a...)
}

func swtch(target uint64, orgdb *db.OpenchainDB) error {

	size, err := fetchBlockchainSizeFromDB()
	if err != nil {
		return nil
	}

	if target >= size {
		fmt.Printf("BlockchainSize: %d\n", size)
		return nil
	}

	orgdb.StateSwitch(getBlockStateHash(target))
	return nil
}

func getBlockStateHash(blockNumber uint64) []byte {

	size, err := fetchBlockchainSizeFromDB()
	if err != nil {
		return nil
	}

	if blockNumber >= size {

		fmt.Printf("BlockchainSize: %d\n", size)

		return nil
	}


	block, err := fetchRawBlockFromDB(blockNumber)

	if err != nil {
		return nil
	}

	return block.StateHash
}


var blockCountKey = []byte("blockCount")

func fetchBlockchainSizeFromDB() (uint64, error) {
	bytes, err := db.GetDBHandle().GetFromBlockchainCF(blockCountKey)
	if err != nil {
		return 0, err
	}
	if bytes == nil {
		return 0, nil
	}
	return util.DecodeToUint64(bytes), nil
}


func fetchRawBlockFromDB(blockNumber uint64) (*protos.Block, error) {

	blockBytes, err := db.GetDBHandle().GetFromBlockchainCF(util.EncodeUint64(blockNumber))
	if err != nil {
		return nil, err
	}
	if blockBytes == nil {
		return nil, nil
	}
	blk, err := protos.UnmarshallBlock(blockBytes)
	if err != nil {
		return nil, err
	}

	return blk, nil
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
	fmt.Printf("	ParentNodeStateHash = [%x]\n", gs.ParentNodeStateHash)
	fmt.Printf("	Branched = [%t]\n", gs.Branched())
	fmt.Printf("	NextNodeStateHash count = [%d]\n", len(gs.NextNodeStateHash))
}
