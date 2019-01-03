/*
Copyright IBM Corp. 2016 All Rights Reserved.

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

package protos

import (
	"github.com/golang/proto"
)

func (m *BucketTreeOffset) Byte() ([]byte, error) {
	return proto.Marshal(m)
}

func byte2BucketTreeOffset(data []byte) (*BucketTreeOffset, error) {
	bucketTreeOffset := &BucketTreeOffset{}
	err := proto.Unmarshal(data, bucketTreeOffset)
	return bucketTreeOffset, err
}


func (m *StateOffset) Unmarshal() (*BucketTreeOffset, error) {
	return byte2BucketTreeOffset(m.Data)
}


func NewStateOffset(level, bucketNum uint64) *StateOffset {
	stateOffset := &StateOffset{}

	btOffset := &BucketTreeOffset{level, bucketNum, 1}
	data, err := btOffset.Byte()
	if err == nil {
		stateOffset.Data = data
		stateOffset.Unmarshal()
	}

	return stateOffset
}
