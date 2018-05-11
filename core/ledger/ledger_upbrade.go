package ledger

import (
	"bytes"
	"fmt"
	"github.com/abchain/fabric/core/db"
	_ "github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/protos"
	protobuf "github.com/golang/protobuf/proto"
	_ "strconv"
)

type reconstructInfo struct {
	begin   uint64
	newhash [][]byte
}

func reconstructBlockChain() (err error, r *reconstructInfo) {

	var size uint64
	size, err = fetchBlockchainSizeFromDB()
	if err != nil {
		return
	}

	var previousHash []byte
	var bytesBefore, bytesAfter uint64
	var writeBatch *db.DBWriteBatch
	defer func() {
		if err == nil && writeBatch != nil {
			err = writeBatch.BatchCommit()
			//notice the difference between "defer writeBatch.Destroy()"
			writeBatch.Destroy()
		}
	}()

	opBatch := uint64(8)

	for i := uint64(0); i < size; i++ {

		if i%opBatch == 0 {
			if writeBatch != nil {
				err = writeBatch.BatchCommit()
				if err != nil {
					return
				}
				writeBatch.Destroy()
			}
			writeBatch = db.GetDBHandle().NewWriteBatch()
		}

		blkk := encodeBlockNumberDBKey(i)

		var blockBytes []byte
		blockBytes, err = db.GetDBHandle().GetFromBlockchainCF(blkk)
		if err != nil {
			return
		}

		bytesBefore = bytesBefore + uint64(len(blockBytes))

		var block *protos.Block
		block, err = protos.UnmarshallBlock(blockBytes)
		if err != nil {
			return
		}

		if block.Version == 0 && len(block.Transactions) != 0 {
			//it was the "most legacy" block so we need to normalize it
			block.Txids = make([]string, len(block.Transactions))
			for i, tx := range block.Transactions {
				block.Txids[i] = tx.Txid
			}

			err = db.GetGlobalDBHandle().PutTransactions(block.Transactions)
			if err != nil {
				return
			}
		}

		if bytes.Compare(previousHash, block.GetPreviousBlockHash()) != 0 && previousHash != nil {

			if r == nil {
				ledgerLogger.Warningf("Block from %d has changed its hash from %x to %x, blocks reconstructed",
					i-1, block.GetPreviousBlockHash(), previousHash)
				r = &reconstructInfo{i - 1, make([][]byte, 0, size-i+1)}
				r.newhash = append(r.newhash, previousHash)

				writeBatch.PutCF(writeBatch.GetDBHandle().IndexesCF,
					encodeBlockHashKey(previousHash), encodeBlockNumber(i-1))
			}
			block.PreviousBlockHash = previousHash
		}

		previousHash, err = block.GetHash()
		if err != nil {
			return
		}
		if r != nil {
			//we must also update index of block
			writeBatch.PutCF(writeBatch.GetDBHandle().IndexesCF,
				encodeBlockHashKey(previousHash), blkk)
			r.newhash = append(r.newhash, previousHash)
		}

		blockBytes, err = block.GetBlockBytes()
		if err != nil {
			return
		}

		writeBatch.PutCF(writeBatch.GetDBHandle().BlockchainCF, blkk, blockBytes)
		bytesAfter = bytesAfter + uint64(len(blockBytes))
	}

	ledgerLogger.Infof("Reconstruct block hash, from %d bytes to %d bytes",
		bytesBefore, bytesAfter)

	return
}

//This is a highly hacking for the legacy PBFT module and we should not use persistCF
//for purpose of consensus in the later version:
//Here we have a brief explanation according to the PBFT TOCS and pbft-persist.go:

/*
	The PBFT module persisted following items for preparing a view change:

P/QSet: the prepare messages it have for the current view, it have digest of a batch,
	that is, some txs waiting for committing and not involved in any block so they
	do not need to be change

reqbatch: a request batch it have got (if peer is primary) for handling, they have no
    matter with blocks, too

chkpt: the checkpoint it have known, which is just expressed by the blockchain info
	so they must be changed if we reconstruct the whole chain. Peer will use the id
	saved in chkpt for syncing so if old blockhash is used in viewchange message
	everything is ruined

*/

var rawLegacyPBFTPrefix = []byte("consensus.chkpt.")

func updateLegacyPBFTChkp(itr *db.DBIterator, r *reconstructInfo) error {

	wb := db.GetDBHandle().NewWriteBatch()
	cf := wb.GetDBHandle().PersistCF
	defer wb.Destroy()

	for ; itr.ValidForPrefix(rawLegacyPBFTPrefix); itr.Next() {

		k := itr.Key().Data()

		var seqNo uint64
		if _, err := fmt.Sscanf(string(k[len(rawLegacyPBFTPrefix):]), "%d", &seqNo); err != nil {
			ledgerLogger.Warningf("Could not restore checkpoint key %s", string(k))
		} else if seqNo > r.begin {
			//construct the checkpoint id, it was generated by helper.GetBlockchainInfoBlob
			//which just call GetBlockchainInfo in ledger, which was a harshal of protos.BlockchainInfo
			info := &protos.BlockchainInfo{seqNo, r.newhash[seqNo-r.begin], r.newhash[seqNo-r.begin-1]}
			newv, err := protobuf.Marshal(info)
			if err != nil {
				return fmt.Errorf("Encode chkpoint %d fail: %s", seqNo, err)
			}

			wb.PutCF(cf, k, newv)
			ledgerLogger.Debugf("Update PBFT checkpoint at %d", seqNo)

		} else if seqNo == r.begin {
			//a little special: the previous blockhash has to be obtained from old checkpoint
			info := &protos.BlockchainInfo{}
			err := protobuf.Unmarshal(itr.Value().Data(), info)
			if err != nil {
				return fmt.Errorf("Could not decode chkpoint %d: %s", seqNo, err)
			}

			if info.Height != seqNo {
				//may be we can just panic because we have a wrong db?
				return fmt.Errorf("Checkpoint data is not match in height: %d vs %d", info.Height, seqNo)
			}

			info.CurrentBlockHash = r.newhash[0]

			newv, err := protobuf.Marshal(info)
			if err != nil {
				return fmt.Errorf("Encode chkpoint %d fail: %s", seqNo, err)
			}

			wb.PutCF(cf, k, newv)
			ledgerLogger.Debugf("Update PBFT checkpoint at %d", seqNo)
		}
	}

	return wb.BatchCommit()
}

func updateDBToV1() error {

	//rewrite blockchainCF
	err, reconstruct := reconstructBlockChain()
	if err != nil {
		return fmt.Errorf("Reconstruct blockchain fail: %s", err)
	}

	//copying keys in persistCF for required content
	//currently it just the peer-persisted content in 0.6, which have no
	//any prefix and we need to add PeerStoreKeyPrefix on it
	itr := db.GetDBHandle().GetIterator(db.PersistCF)
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {

		k := itr.Key().Data()

		//if hash is update, the persisted checkpoint for PBFT must be also updated
		if reconstruct != nil {
			if bytes.HasPrefix(k, rawLegacyPBFTPrefix) {
				err = updateLegacyPBFTChkp(itr, reconstruct)
				if err != nil {
					return fmt.Errorf("Reconstruct PBFT consensus fail: %s", err)
				}
			}

			//after run in updateLegacyPBFTChkp the itr may become invalid
			if !itr.Valid() {
				break
			}
		}

		//check prefix, handling consensus or other
		if bytes.HasPrefix(k, []byte("consensus.")) {
			//we do not need to move consensus kv
		} else {
			//treat it as peer persisted data and move
		}

	}

	return nil
}
