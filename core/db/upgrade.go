package db

import (
	_ "github.com/tecbot/gorocksdb"
)

// func (srcDb *baseHandler) MoveColumnFamily(srcname string, dstDb IDataBaseHandler,
// 	dstname string, rmSrcCf bool) (uint64, error) {

// 	var err error
// 	itr := srcDb.GetIterator(srcname)
// 	var totalKVs uint64
// 	totalKVs = 0
// 	itr.SeekToFirst()

// 	for ; itr.Valid(); itr.Next() {
// 		k := itr.Key()
// 		v := itr.Value()
// 		err = dstDb.PutValue(dstname, k.Data(), v.Data(), nil)

// 		if err != nil {
// 			dbLogger.Error("Put value fail", err)
// 		}

// 		if rmSrcCf {
// 			srcDb.DeleteKey(srcname, k.Data(), nil)
// 		}
// 		k.Free()
// 		v.Free()
// 		if err != nil {
// 			break
// 		}
// 		totalKVs++
// 	}

// 	itr.Close()

// 	dbLogger.Infof("Moved %d KVs from %s.%s to %s.%s",
// 		totalKVs, srcDb.dbName, srcname, dstDb.GetDbName(), dstname)

// 	if err != nil {
// 		dbLogger.Errorf("An error happened during moving: %s", err)
// 	}

// 	return totalKVs, err
// }

// func (txdb *GlobalDataDB) DumpGlobalState() {
// 	itr := txdb.GetIterator(GlobalCF)
// 	defer itr.Close()

// 	idx := 0
// 	itr.SeekToFirst()

// 	for ; itr.Valid(); itr.Next() {
// 		k := itr.Key()
// 		v := itr.Value()
// 		keyBytes := k.Data()
// 		idx++

// 		gs, _ := protos.UnmarshallGS(v.Data())

// 		dbLogger.Infof("%d: statehash<%x>", gs.Count, keyBytes)
// 		dbLogger.Infof("	  branched<%t>", gs.Branched)
// 		dbLogger.Infof("	  parent<%x>", gs.ParentNodeStateHash)
// 		dbLogger.Infof("	  lastBranch<%x>", gs.LastBranchNodeStateHash)
// 		dbLogger.Infof("	  childNum<%d>:", len(gs.NextNodeStateHash))
// 		for _, c := range gs.NextNodeStateHash {
// 			dbLogger.Infof("        <%x>", c)
// 		}
// 		k.Free()
// 		v.Free()
// 	}
// }

// move txs to txdb
// func (blockchain *blockchain) reorganize() error {

// 	size := blockchain.getSize()
// 	if size == 0 {
// 		return nil
// 	}

// 	gs := protos.NewGlobalState()

// 	for i := uint64(0); i < size; i++ {
// 		block, blockErr := blockchain.getBlockByOldMode(i)
// 		if blockErr != nil {
// 			return blockErr
// 		}

// 		if i > 0 {
// 			blockHashV0, err := block.GetHashV0()
// 			if err != nil {
// 				return err
// 			}

// 			err = db.GetDBHandle().DeleteKey(db.IndexesCF, encodeBlockHashKey(blockHashV0), nil)
// 			ledgerLogger.Infof("%d: DeleteKey in <%s>: <%x>. err: %s", i, db.IndexesCF, encodeBlockHashKey(blockHashV0), err)
// 		}

// 		block.FeedTranscationIds()

// 		state.CommitGlobalState(block.Transactions, block.StateHash, i, gs)

// 		if block.Transactions == nil {
// 			ledgerLogger.Infof("%d: block.Transactions == nil", i)
// 			continue
// 		}

// 		blockchain.persistUpdatedRawBlock(block, i)
// 	}

// 	//dbg.Infof("===========================after shrink==============================")
// 	//for i := uint64(0); i < size; i++ {
// 	//	block, blockErr := blockchain.getBlock(i)
// 	//	if blockErr != nil {
// 	//		return blockErr
// 	//	}
// 	//	//block.Dump()
// 	//}
// 	return nil
// }

/////////////////// DB-reorganize  methods ///////////////////////////////
//////////////////////////////////////////////////////////////////////////

// func InitializeDataBase(orgdb db.IDataBaseHandler, txdb db.IDataBaseHandler) error {

// 	orgdbVersion, errVer := orgdb.GetValue(db.PersistCF, []byte(db.VersionKey))
// 	if errVer == nil {
// 		if orgdbVersion != nil {
// 			ver := db.DecodeToUint64(orgdbVersion)
// 			return fmt.Errorf("Original DB version: %d. No action required.", ver)
// 		} else {
// 			// ok, this is the v0 db that expected to be reorganized
// 			ledgerLogger.Infof("No original DB version detected, start to reorganize original DB and produce txdb.")
// 		}
// 	} else {
// 		ledgerLogger.Error("Init DB fail", errVer)
// 		return errVer
// 	}

// 	_, err := orgdb.MoveColumnFamily(db.PersistCF, txdb, db.PersistCF, true)

// 	if err != nil {
// 		return err
// 	}

// 	bc, errBlockchain := newBlockchain(0)
// 	if errBlockchain != nil {
// 		return errBlockchain
// 	}

// 	err = bc.reorganize()

// 	if err != nil {
// 		return err
// 	}

// 	err = txdb.PutValue(db.PersistCF, []byte(db.VersionKey), db.EncodeUint64(db.GlobalDataBaseVersion), nil)
// 	if err != nil {
// 		return err
// 	}
// 	err = orgdb.PutValue(db.PersistCF, []byte(db.VersionKey), db.EncodeUint64(db.OriginalDataBaseVersion), nil)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }
