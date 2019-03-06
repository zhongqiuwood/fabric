package buckettree

import (
	"fmt"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/proto"
)

type PartialSnapshotIterator struct {
	StateSnapshotIterator
	*config
	keyCache, valueCache []byte
	lastBucketNum        int
	curLevel        int
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

	if partialItr.curLevel != partialItr.getLowestLevel() {
		return false
	}

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
	partialItr.keyCache = nil
	partialItr.valueCache = nil

	if !partialItr.StateSnapshotIterator.Next() {
		return false
	}

	keyBytes := statemgmt.Copy(partialItr.dbItr.Key().Data())
	valueBytes := statemgmt.Copy(partialItr.dbItr.Value().Data())

	bucketKey := decodeBucketKey(partialItr.config, keyBytes)

	if bucketKey.bucketNumber > partialItr.lastBucketNum {
		return false
	}
	if bucketKey.level > partialItr.curLevel {
		return false
	}

	bucketNode := unmarshalBucketNode(bucketKey, valueBytes)

	partialItr.keyCache = bucketKey.getEncodedBytes()
	partialItr.valueCache = bucketNode.marshal()

	logger.Infof("Sent metadata: bucketNode: [%+v], computeCryptoHash[%x]",
		bucketNode.bucketKey,
		bucketNode.computeCryptoHash())

	return true
}

func (partialItr *PartialSnapshotIterator) Seek(offset *protos.SyncOffset) error {

	bucketTreeOffset, err := offset.Unmarshal2BucketTree()
	if err != nil {
		return err
	}

	level := int(bucketTreeOffset.Level)
	if level > partialItr.getLowestLevel() {
		return fmt.Errorf("level %d outbound: [%d]", level, partialItr.getLowestLevel())
	}
	partialItr.curLevel = level

	startNum := int(bucketTreeOffset.BucketNum)
	if startNum > partialItr.getNumBuckets(level) {
		return fmt.Errorf("Start numbucket %d outbound: [%d]", startNum, partialItr.getNumBuckets(level))
	}

	endNum := int(bucketTreeOffset.Delta + bucketTreeOffset.BucketNum - 1)
	if endNum > partialItr.getNumBuckets(level) {
		endNum = partialItr.getNumBuckets(level)
	}

	logger.Infof("-----------Required bucketTreeOffset: [%+v] start-end [%d-%d]",
		bucketTreeOffset, startNum, endNum)

	if level == partialItr.getLowestLevel() {
		partialItr.dbItr.Seek(minimumPossibleDataKeyBytesFor(newBucketKey(partialItr.config, level, startNum)))
	} else {
		partialItr.dbItr.Seek(newBucketKey(partialItr.config, level, startNum).getEncodedBytes())
	}
	partialItr.dbItr.Prev()
	partialItr.lastBucketNum = endNum

	return nil

}

func (partialItr *PartialSnapshotIterator) GetMetaData() []byte {

	if partialItr.curLevel == partialItr.getLowestLevel() {
		return nil
	}

	md := &protos.SyncMetadata{}
	for partialItr.NextBucketNode() {
		_, v := partialItr.GetRawKeyValue()
		md.BucketNodeHashList = append(md.BucketNodeHashList, v)
	}

	metadata, _ := proto.Marshal(md)
	//logger.Infof("-----------send metadata [%x]", metadata)
	return metadata
}

func (partialItr *PartialSnapshotIterator) GetRawKeyValue() ([]byte, []byte) {
	//sanity check
	if partialItr.keyCache == nil {
		panic("Called after Next return false")
	}
	return partialItr.keyCache, partialItr.valueCache
}
