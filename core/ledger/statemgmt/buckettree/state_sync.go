package buckettree

import (
	"fmt"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/protos"
)

type syncProcess struct {
	*StateImpl
	targetStateHash []byte
	current         *protos.BucketTreeOffset
	metaDelta       int
	syncLevels      []int
}

const partialStatusKeyPrefixByte = byte(16)

func checkSyncProcess(parent *StateImpl) *syncProcess {
	dbItr := parent.GetIterator(db.StateCF)
	defer dbItr.Close()

	for ; dbItr.Valid() && dbItr.ValidForPrefix([]byte{partialStatusKeyPrefixByte}); dbItr.Next() {

		targetStateHash := statemgmt.Copy(dbItr.Key().Data())[1:]
		data := &protos.SyncOffset{Data: statemgmt.Copy(dbItr.Value().Data())}
		offset, err := data.Unmarshal2BucketTree()

		if err == nil {

			logger.Info("Restore sync task to target [%x]", targetStateHash)
			sp := &syncProcess{
				StateImpl:       parent,
				targetStateHash: targetStateHash,
			}
			sp.calcSyncLevels(parent.currentConfig)

			if offset.GetLevel() == parent.currentConfig.getLowestLevel() {
				sp.syncLevels = []int{}
				sp.current = offset
			} else {
				//we check current offset against the calculated sync plan ...
				for i, lvl := range sp.syncLevels {
					if lvl == int(offset.Level) {
						sp.current = offset
					} else if lvl < int(offset.Level) {
						logger.Infof("We restored different level (%d, nearest is %d)", offset.Level, lvl)
						sp.syncLevels = sp.syncLevels[:l]
					}
				}
			}
		}

		logger.Infof("Recorded sync state [%x] is invalid: %s", targetStateHash, err)
		parent.DeleteKey(db.StateCF, dbItr.Key().Data())
	}

	return nil
}

func newSyncProcess(parent *StateImpl, stateHash []byte) *syncProcess {

	sp := &syncProcess{
		StateImpl:       parent,
		targetStateHash: stateHash,
	}

	sp.calcSyncLevels(parent.currentConfig)
	sp.resetCurrentOffset()

	logger.Infof("newSyncProcess: sync start with offset %v", sp.current)
	return sp
}

var metaReferenceDelta = 125

func (proc *syncProcess) resetCurrentOffset() {
	if l := len(proc.syncLevels); l == 0 {
		sp.current = &protos.BucketTreeOffset{
			Level:     uint64(proc.StateImpl.currentConfig.getLowestLevel()),
			BucketNum: 1,
			Delta:     uint64(proc.StateImpl.currentConfig.syncDelta),
		}

	} else {
		sp.current = &protos.BucketTreeOffset{
			Level:     uint64(proc.syncLevels[l-1]),
			BucketNum: 1,
			Delta:     uint64(proc.metaDelta),
		}
	}

}

// compute the levels by which metadata will be sent for sanity check
func (proc *syncProcess) calcSyncLevels(conf *config) {

	syncLevels := []int{}
	//estimate a suitable delta for metadata: one bucketnode is 32-bytes hash
	//and ~4k bytes in total (i.e.: 125 hashes) is acceptable
	//(but we must calculate a number which is just an exponent of maxgroup)
	metaDelta := conf.getMaxGroupingAtEachLevel()
	lvldistance := 1
	for ; metaDelta < metaReferenceDelta; metaDelta = metaDelta * conf.getMaxGroupingAtEachLevel() {
		lvldistance++
	}

	//the syncdelta may be small and different with metaDelta, so we should test the
	//suitable level for last meta-syncing ...
	testSyncDelta := conf.syncDelta
	curlvl := conf.getLowestLevel()
	for ; curlvl > 0; curlvl-- {
		if testSyncDelta >= conf.getMaxGroupingAtEachLevel() {
			testSyncDelta = testSyncDelta / conf.getMaxGroupingAtEachLevel()
		} else {
			break
		}
	}
	for ; curlvl > 0; curlvl = curlvl - lvldistance {
		//twe shrink the lvls by metaDelta
		syncLevels = append(syncLevels, curlvl)
	}

	proc.metaDelta = metaDelta
	proc.syncLevels = syncLevels
	logger.Infof("Calculate sync plan as: %v", syncLevels)
}

func (underSync *syncProcess) currentLevel() int {
	return underSync.syncLevels[underSync.curLevelIndex]
}

//implement for syncinprogress interface
func (proc *syncProcess) IsSyncInProgress() {}
func (proc *syncProcess) Error() string     { return "buckettree: state syncing is in progress" }

func (proc *syncProcess) PersistProgress(writeBatch *db.DBWriteBatch) error {

	key := append([]byte{partialStatusKeyPrefixByte}, proc.targetStateHash...)
	if value, err := proc.current.Byte(); err == nil {
		logger.Infof("Persisting current sync process = %+v", proc.current)
		writeBatch.PutCF(writeBatch.GetDBHandle().StateCF, key, value)
	} else {
		return err
	}

	return nil
}

func (proc *syncProcess) RequiredParts() ([]*protos.SyncOffset, error) {

	if proc.current == nil {
		return nil, nil
	}

	if data, err := proc.current.Byte(); err != nil {
		return nil, err
	} else {
		return []*protos.SyncOffset{&protos.SyncOffset{Data: data}}, err
	}

}

func (proc *syncProcess) CompletePart(part *protos.BucketTreeOffset) error {

	if proc.current == nil {
		return fmt.Errorf("No task left")
	} else if proc.current.Level != part.GetLevel() || proc.current.BucketNum != part.GetBucketNum() {
		return fmt.Errorf("Not current task (expect <%v> but has <%v>", proc.current, part)
	}

	conf := proc.currentConfig

	// err := proc.verifyMetadata()
	// if err != nil {
	// 	return err
	// }

	maxNum := uint64(conf.getNumBuckets(int(proc.current.Level)))
	nextNum := proc.current.BucketNum + proc.current.Delta

	if maxNum <= nextNum-1 {
		//current level is done
		if l := len(proc.syncLevels); l > 0 {
			proc.syncLevels = proc.syncLevels[:l-1]
		} else {
			logger.Infof("Finally hit maxBucketNum<%d> @ target Level<%d>",
				maxNum, proc.current.Level)
			proc.current = nil
			return nil
		}

		proc.resetCurrentOffset()
		logger.Infof("Compute Next level[%d], delta<%d>", proc.current.Level, proc.current.Delta)
	} else {
		delta := min(uint64(proc.current.Delta), maxNum-nextNum+1)

		proc.current.BucketNum = nextNum
		proc.current.Delta = delta
		logger.Infof("Next state offset <%+v>", proc.current)
	}

	return nil
}

// func (underSync *syncProcess) verifyMetadata() error {

// 	curLevelIndex := underSync.curLevelIndex
// 	tempTreeDelta := underSync.StateImpl.bucketTreeDelta
// 	if underSync.metadataTreeDelta != nil {
// 		tempTreeDelta = underSync.metadataTreeDelta
// 	}

// 	var err error
// 	if curLevelIndex < len(underSync.syncLevels)-1 {

// 		lastSyncLevel := underSync.syncLevels[curLevelIndex+1]
// 		bucketNodes := tempTreeDelta.getBucketNodesAt(lastSyncLevel)

// 		var localBucketNode *bucketNode
// 		for _, bkNode := range bucketNodes {

// 			localBucketNode, err = fetchBucketNodeFromDB(underSync.StateImpl.OpenchainDB,
// 				bkNode.bucketKey.getBucketKey(underSync.currentConfig))

// 			if err == nil {
// 				if bytes.Equal(localBucketNode.computeCryptoHash(), bkNode.computeCryptoHash()) {
// 					logger.Infof("Pass: verify metadata: bucketKey[%+v] cryptoHash[%x]",
// 						bkNode.bucketKey,
// 						bkNode.computeCryptoHash())
// 				} else {
// 					err = fmt.Errorf("Failed to verify metadata: error: mismatch, "+
// 						"bucketKey[%+v] cryptoHash[%x] localCryptoHash[%x]",
// 						bkNode.bucketKey,
// 						bkNode.computeCryptoHash(),
// 						localBucketNode.computeCryptoHash())
// 					break
// 				}
// 			} else {
// 				err = fmt.Errorf("Failed to verify metadata: error: %s, bucketKey[%+v] cryptoHash[%x]",
// 					err,
// 					bkNode.bucketKey,
// 					bkNode.computeCryptoHash())
// 				break
// 			}

// 		}
// 	}

// 	underSync.metadataTreeDelta = nil
// 	return err
// }

func sqrt(x float64) float64 {
	z := 1.0

	if x < 0 {
		return 0
	} else if x == 0 {
		return 0
	} else {

		getabs := func(x float64) float64 {
			if x < 0 {
				return -x
			}
			if x == 0 {
				return 0
			}
			return x
		}

		for getabs(z*z-x) > 1e-6 {
			z = (z + x/z) / 2
		}
		return z
	}
}
