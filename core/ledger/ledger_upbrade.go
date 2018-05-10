package ledger

// import (
// 	"bytes"
// 	"fmt"
// 	"github.com/abchain/fabric/core/db"
// 	"github.com/abchain/fabric/core/util"
// 	"github.com/abchain/fabric/protos"
// 	"golang.org/x/net/context"
// 	"strconv"
// )

// func updateDBToV1() error {

// 	//rewrite blockchainCF
// 	size, err := fetchBlockchainSizeFromDB()
// 	if err != nil {
// 		return fmt.Errorf("Get blockchain size fail: %s", err)
// 	}

// 	var previousHash []byte
// 	var bytesBefore, bytesAfter uint64
// 	var writeBatch *db.DBWriteBatch
// 	defer func() {
// 		//notice the difference between "defer writeBatch.Destroy()""
// 		writeBatch.Destroy()
// 	}()

// 	for i := uint64(0); i < size; i++ {

// 		blockBytes, err := db.GetDBHandle().GetFromBlockchainCF(encodeBlockNumberDBKey(blockNumber))
// 		if err != nil {
// 			return err
// 		}

// 		bytesBefore = bytesBefore + uint64(len(blockBytes))

// 		block, err := protos.UnmarshallBlock(blockBytes)
// 		if err != nil {
// 			return err
// 		}

// 		blockBytes, err = block.GetBlockBytes()
// 		if err != nil {
// 			return err
// 		}

// 		if i%8 == 0 {
// 			if writeBatch != nil {
// 				err = writeBatch.BatchCommit()
// 				if err != nil {
// 					return err
// 				}
// 			}

// 			writeBatch := db.GetDBHandle().NewWriteBatch()
// 		}
// 		writeBatch := db.GetDBHandle().NewWriteBatch()
// 		defer writeBatch.Destroy()
// 		cf := writeBatch.GetDBHandle().BlockchainCF
// 		writeBatch.PutCF(cf, encodeBlockNumberDBKey(blockNumber), blockBytes)

// 		bytesAfter = bytesAfter + uint64(len(blockBytes))

// 		previousHash = block.GetHash()

// 	}

// 	writeBatch := db.GetDBHandle().NewWriteBatch()
// 	defer writeBatch.Destroy()
// 	cf := writeBatch.GetDBHandle().BlockchainCF
// 	writeBatch.PutCF(cf, encodeBlockNumberDBKey(blockNumber), blockBytes)

// 	//copying persistCF

// }
