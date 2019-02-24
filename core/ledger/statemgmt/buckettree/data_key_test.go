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

package buckettree

import (
	"testing"

	"github.com/abchain/fabric/core/ledger/testutil"
)

func TestDataKey(t *testing.T) {
	conf = newConfig(26, 3, fnvHash)
	dataKey := newDataKey(conf, "chaincodeID", "key")
	encodedBytes := dataKey.getEncodedBytes()
	dataKeyFromEncodedBytes := newDataKeyFromEncodedBytes(conf, encodedBytes)
	testutil.AssertEquals(t, dataKey, dataKeyFromEncodedBytes)
}

func TestDataKeyGetBucketKey(t *testing.T) {
	conf = newConfig(26, 3, fnvHash)
	newDataKey(conf, "chaincodeID1", "key1").getBucketKey()
	newDataKey(conf, "chaincodeID1", "key2").getBucketKey()
	newDataKey(conf, "chaincodeID2", "key1").getBucketKey()
	newDataKey(conf, "chaincodeID2", "key2").getBucketKey()
}
