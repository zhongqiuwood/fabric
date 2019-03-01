package protos

import (
	"github.com/golang/protobuf/proto"
)

////////////////////////////////////////////
// BlockOffset
////////////////////////////////////////////
func (m *BlockOffset) Byte() ([]byte, error) {
	return proto.Marshal(m)
}

func (m *SyncOffset) Unmarshal2BlockOffset() (*BlockOffset, error) {
	offsetImpl := &BlockOffset{}
	err := proto.Unmarshal(m.Data, offsetImpl)
	return offsetImpl, err
}

func (m *SyncOffset) Unmarshal2BucketTree() (*BucketTreeOffset, error) {
	return byte2BucketTreeOffset(m.Data)
}

func NewBlockOffset(start, end uint64) *SyncOffset {
	syncOffset := &SyncOffset{}
	offset := &BlockOffset{start, end}
	data, err := offset.Byte()
	if err == nil {
		syncOffset.Data = data
	}

	return syncOffset
}

////////////////////////////////////////////
// BucketTreeOffset
////////////////////////////////////////////
func (m *BucketTreeOffset) Byte() ([]byte, error) {
	return proto.Marshal(m)
}

func byte2BucketTreeOffset(data []byte) (*BucketTreeOffset, error) {
	bucketTreeOffset := &BucketTreeOffset{}
	err := proto.Unmarshal(data, bucketTreeOffset)
	return bucketTreeOffset, err
}

func NewBucketTreeOffset(level, bucketNum uint64) *SyncOffset {
	stateOffset := &SyncOffset{}

	btOffset := &BucketTreeOffset{level, bucketNum, 1}
	data, err := btOffset.Byte()
	if err == nil {
		stateOffset.Data = data
	}

	return stateOffset
}
