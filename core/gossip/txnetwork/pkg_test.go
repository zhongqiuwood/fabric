package txnetwork

import (
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/ledger/testutil"
	"github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"os"
	"testing"
)

var testParams []string
var testDBWrapper = db.NewTestDBWrapper()

func TestMain(m *testing.M) {
	testParams = testutil.ParseTestParams()
	testutil.SetupTestConfig()
	testutil.SetLogLevel(logging.DEBUG, "")
	os.Exit(m.Run())
}

func initTestLedgerWrapper(tb testing.TB) *ledger.Ledger {
	testDBWrapper.CleanDB(tb)
	ledger, err := ledger.GetNewLedger(testDBWrapper.GetDB())
	testutil.AssertNoError(tb, err, "Error while constructing ledger")
	gensisstate, err := ledger.GetCurrentStateHash()
	testutil.AssertNoError(tb, err, "Error while get gensis state")
	err = testDBWrapper.PutGenesisGlobalState(gensisstate)
	testutil.AssertNoError(tb, err, "Error while add gensis state")
	return ledger
}

func buildTestTx(t *testing.T) (*protos.Transaction, string) {
	uuid := util.GenerateUUID()
	tx, err := protos.NewTransaction(protos.ChaincodeID{Path: "testUrl"}, uuid, "anyfunction", []string{"param1", "param2", uuid})
	testutil.AssertNil(t, err)
	return tx, uuid
}

func TestBaseOp(t *testing.T) {

	ledger := initTestLedgerWrapper(t)
	ledger.BeginTxBatch(1)
	ledger.TxBegin("txUuid")
	ledger.SetState("chaincode1", "key1", []byte("value1"))
	ledger.TxFinished("txUuid", true)
	t1, _ := buildTestTx(t)
	t2, _ := buildTestTx(t)
	ledger.CommitTxBatch(1, []*protos.Transaction{t1, t2}, nil, []byte("proof"))

	h, err := ledger.GetCurrentStateHash()
	testutil.AssertNil(t, err)
	testutil.AssertNotNil(t, h)

	b, err := ledger.GetBlockByNumber(0)
	testutil.AssertNil(t, err)
	testutil.AssertNotNil(t, b)

	testutil.AssertEquals(t, len(b.Transactions), 2)

	t1r, err := ledger.GetTransactionByID(t1.GetTxid())
	testutil.AssertNil(t, err)
	testutil.AssertNotNil(t, t1r)
	testutil.AssertEquals(t, t1.ChaincodeID, t1r.ChaincodeID)
	testutil.AssertEquals(t, t1.Timestamp, t1r.Timestamp)
}
