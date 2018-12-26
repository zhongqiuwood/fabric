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
	"sort"
	"github.com/golang/proto"
)


type ChaincodeStateDeltaMap map[string]*ChaincodeStateDelta

func (m *BucketTreeOffset) Byte() ([]byte, error) {
	return proto.Marshal(m)
}

func NewChaincodeStateDelta() *ChaincodeStateDelta {
	return &ChaincodeStateDelta{make(map[string]*UpdatedValue)}
}

func (chaincodeStateDelta *ChaincodeStateDelta) Get(key string) *UpdatedValue {
	// TODO Cache?
	return chaincodeStateDelta.UpdatedKVs[key]
}

func (chaincodeStateDelta *ChaincodeStateDelta) Set(key string, updatedValue, previousValue []byte) {
	updatedKV, ok := chaincodeStateDelta.UpdatedKVs[key]
	if ok {
		// Key already exists, just set the updated value
		updatedKV.Value = updatedValue
	} else {
		// New key. Create a new entry in the map
		chaincodeStateDelta.UpdatedKVs[key] = &UpdatedValue{updatedValue, previousValue}
	}
}

func (chaincodeStateDelta *ChaincodeStateDelta) Remove(key string, previousValue []byte) {
	updatedKV, ok := chaincodeStateDelta.UpdatedKVs[key]
	if ok {
		// Key already exists, just set the value
		updatedKV.Value = nil
	} else {
		// New key. Create a new entry in the map
		chaincodeStateDelta.UpdatedKVs[key] = &UpdatedValue{nil, previousValue}
	}
}

func (chaincodeStateDelta *ChaincodeStateDelta) HasChanges() bool {
	return len(chaincodeStateDelta.UpdatedKVs) > 0
}

func (chaincodeStateDelta *ChaincodeStateDelta) GetSortedKeys() []string {
	updatedKeys := []string{}
	for k := range chaincodeStateDelta.UpdatedKVs {
		updatedKeys = append(updatedKeys, k)
	}
	sort.Strings(updatedKeys)
	logger.Debugf("Sorted keys = %#v", updatedKeys)
	return updatedKeys
}


// IsDeleted checks whether the key was deleted
func (updatedValue *UpdatedValue) IsDeleted() bool {
	return updatedValue.Value == nil
}
