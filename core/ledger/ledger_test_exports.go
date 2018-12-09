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

package ledger

import (
	"testing"

	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/testutil"
)

var testDBWrapper = db.NewTestDBWrapper()

//InitTestLedger provides a ledger for testing. This method creates a fresh db and constructs a ledger instance on that.
func InitTestLedger(t *testing.T) *Ledger {

	//"exhaust" the once var in ledger so we can replace the singleton later
	once.Do(func() {})
	ledger_gOnce.Do(func() {})
	testDBWrapper.CleanDB(t)
	txpool, err := newTxPool()
	testutil.AssertNoError(t, err, "Error while constructing txpool")
	//replace global ledger singleton
	ledger_g = &LedgerGlobal{txpool}
	newLedger, err := GetNewLedger(testDBWrapper.GetDB(), nil)
	testutil.AssertNoError(t, err, "Error while constructing ledger")
	gensisstate, err := newLedger.GetCurrentStateHash()
	testutil.AssertNoError(t, err, "Error while get gensis state")
	err = testDBWrapper.PutGenesisGlobalState(gensisstate)
	testutil.AssertNoError(t, err, "Error while add gensis state")
	//replace ledger singleton
	ledger = newLedger
	poolTxBeforeCommit = true
	return newLedger
}
