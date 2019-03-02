package buckettree

import (
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/protos"
)

type syncProcess struct {
	*StateImpl
	targetStateHash []byte
	current         *protos.BucketTreeOffset
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

			//TODO: should verify the partial data ...

			return &syncProcess{
				StateImpl:       parent,
				targetStateHash: targetStateHash,
				current:         offset,
			}
		}

		logger.Errorf("Recorded sync state [%x] is invalid: %s", targetStateHash, err)
		parent.DeleteKey(db.StateCF, dbItr.Key().Data())
	}

	return nil
}

func newSyncProcess(parent *StateImpl, stateHash []byte) *syncProcess {

	return &syncProcess{
		StateImpl:       parent,
		targetStateHash: stateHash,
		current: &protos.BucketTreeOffset{
			Level:     uint64(parent.currentConfig.getLowestLevel()),
			BucketNum: 1,
			Delta:     min(uint64(parent.currentConfig.syncDelta), uint64(parent.currentConfig.getNumBucketsAtLowestLevel())),
		},
	}
}

func (proc *syncProcess) PersistProgress(writeBatch *db.DBWriteBatch) error {

	key := append([]byte{partialStatusKeyPrefixByte}, proc.targetStateHash...)
	if value, err := proc.current.Byte(); err == nil {
		logger.Debugf("Persisting current sync process = %#v", proc.current)
		writeBatch.PutCF(writeBatch.GetDBHandle().StateCF, key, value)
	} else {
		return err
	}

	return nil
}

func (proc *syncProcess) RequiredParts() ([]*protos.SyncOffset, error) {

	conf := proc.currentConfig

	maxNum := uint64(conf.getNumBuckets(int(proc.current.Level)))
	nextNum := proc.current.BucketNum + proc.current.Delta

	if maxNum <= nextNum-1 {
		logger.Infof("Hit maxBucketNum<%d>, target BucketNum<%d>", maxNum, nextNum)
		return nil, nil
	}
	delta := min(uint64(conf.syncDelta), maxNum-nextNum+1)

	proc.current.BucketNum = nextNum
	proc.current.Delta = delta

	logger.Debugf("Next state offset <%+v>", proc.current)
	if data, err := proc.current.Byte(); err != nil {
		return nil, err
	} else {
		return []*protos.SyncOffset{&protos.SyncOffset{Data: data}}, err
	}

}
