package buckettree

import (
	"fmt"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/protos"
)

type PartialSnapshotIterator struct {
	StateSnapshotIterator
	*config
	keyCache, valueCache []byte
	lastBucketNum        int
}

func newPartialSnapshotIterator(snapshot *db.DBSnapshot, cfg *config) (*PartialSnapshotIterator, error) {
	iit, err := newStateSnapshotIterator(snapshot)
	if err != nil {
		return nil, err
	}

	return &PartialSnapshotIterator{
		StateSnapshotIterator: *iit,
		config:                cfg,
		lastBucketNum:         cfg.getNumBuckets(cfg.getSyncLevel()),
	}, nil
}

//overwrite the original GetRawKeyValue and Next
func (partialItr *PartialSnapshotIterator) Next() bool {

	partialItr.keyCache = nil
	partialItr.valueCache = nil

	if !partialItr.StateSnapshotIterator.Next() {
		return false
	}

	keyBytes := statemgmt.Copy(partialItr.dbItr.Key().Data())
	valueBytes := statemgmt.Copy(partialItr.dbItr.Value().Data())
	dataNode := unmarshalDataNodeFromBytes(keyBytes, valueBytes)

	if dataNode.dataKey.bucketNumber > partialItr.lastBucketNum {
		return false
	}

	partialItr.keyCache = dataNode.getCompositeKey()
	partialItr.valueCache = dataNode.getValue()

	return true
}

func (partialItr *PartialSnapshotIterator) NextBucketNode() bool {

	//sanity check
	if partialItr.keyCache == nil {
		panic("Called after Next return false")
	}
	return partialItr.keyCache, partialItr.valueCache
}


func (partialItr *PartialSnapshotIterator) Seek(offset *protos.SyncOffset) error {

	bucketTreeOffset, err := offset.Unmarshal2BucketTree()
	if err != nil {
		return err
	}

	logger.Infof("-----------Required bucketTreeOffset: [%+v]", bucketTreeOffset)

	level := int(bucketTreeOffset.Level)
	if level > partialItr.getLowestLevel() {
		return fmt.Errorf("level %d outbound: [%d]", level, partialItr.getLowestLevel())
	}

	startNum := int(bucketTreeOffset.BucketNum)
	if startNum > partialItr.getNumBuckets(level) {
		return fmt.Errorf("Start numbucket %d outbound: [%d]", startNum, partialItr.getNumBuckets(level))
	}

	endNum := int(bucketTreeOffset.Delta + bucketTreeOffset.BucketNum - 1)

	if level == partialItr.getLowestLevel() {
		//transfer datanode
		//seek to target datanode
		partialItr.dbItr.Seek(minimumPossibleDataKeyBytesFor(newBucketKey(partialItr.config, level, startNum)))
		partialItr.dbItr.Prev()
		partialItr.lastBucketNum = endNum
	} else {
		//TODO: transfer bucketnode in metadata


		partialItr.dbItr.Seek(newBucketKey(partialItr.config, level, startNum).getEncodedBytes())
		partialItr.dbItr.Prev()

		partialItr.GetMetaData()
		//return fmt.Errorf("No implement")
	}

	return nil

}

func (partialItr *PartialSnapshotIterator) GetMetaData() []byte {

	md := &protos.SyncMetadata{}

	for partialItr.NextBucketNode() {
		k, v := partialItr.GetRawKeyValue()

		bucketKey := decodeBucketKey(partialItr.config, k)
		logger.Infof("bucketKey[%+v], CryptoHash[%x]", bucketKey, v)

		md.BucketNodeHashList = append(md.BucketNodeHashList, v)
	}


	return nil
}

func (partialItr *PartialSnapshotIterator) GetRawKeyValue() ([]byte, []byte) {

	//sanity check
	if partialItr.keyCache == nil {
		panic(partialItr.keyCache == nil)
	}

	return partialItr.keyCache, partialItr.valueCache
}
