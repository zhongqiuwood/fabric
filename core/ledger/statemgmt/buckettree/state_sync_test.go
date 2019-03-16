package buckettree

import (
	"fmt"
	"testing"

	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger/statemgmt"
	"github.com/abchain/fabric/core/ledger/testutil"
)

func TestBaseSync(t *testing.T) {

	secondaryDB, _ := db.StartDB("secondary", nil)
	defer db.StopDB(secondaryDB)

	srcImpl := newStateImplTestWrapperWithCustomConfig(t, 100, 2)
	targetImpl := newStateImplTestWrapperOnDBWithCustomConfig(t, secondaryDB, 100, 2)

	
}

func TestSync(t *testing.T) {
	testDBWrapper.CleanDB(t)
	stateImplTestWrapper := newStateImplTestWrapperWithCustomConfig(t, 100, 2)
	stateImpl := stateImplTestWrapper.stateImpl
	stateDelta := statemgmt.NewStateDelta()

	i := 1
	for i <= 100 {
		chaincode := fmt.Sprintf("chaincode%d", i)
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("value%d", i)
		stateDelta.Set(chaincode, k, []byte(v), nil)
		i++
	}

	stateImpl.PrepareWorkingSet(stateDelta)
	targetHash := stateImplTestWrapper.computeCryptoHash()
	stateImplTestWrapper.persistChangesAndResetInMemoryChanges()

	err := stateImplTestWrapper.syncState(targetHash)
	testutil.AssertNil(t, err)

	localHash := stateImplTestWrapper.computeCryptoHash()
	fmt.Printf("Local hash: %x\n", localHash)
	fmt.Printf("Target hash: %x\n", targetHash)

	testutil.AssertEquals(t, localHash, targetHash)
}
