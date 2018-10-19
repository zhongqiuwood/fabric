package txnetwork

import (
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/ledger/testutil"
	"github.com/abchain/fabric/core/util"
	"github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
	"os"
	"testing"

	//this import is required to init the GossipFactory func
	_ "github.com/abchain/fabric/core/gossip/stub"
)

var testParams []string

func TestMain(m *testing.M) {
	testParams = testutil.ParseTestParams()
	testutil.SetupTestConfig()
	testutil.SetLogLevel(logging.DEBUG, "")

	//create a default peer
	id, s := CreateSimplePeer()
	DefaultInitPeer.Id = id
	DefaultInitPeer.State = s
	os.Exit(m.Run())
}

func initGlobalStatus() *txNetworkGlobal {

	return CreateTxNetworkGlobal()
}

func initTestLedgerWrapper(t *testing.T) *ledger.Ledger {
	return ledger.InitTestLedger(t)
}

func buildTestTx(t *testing.T) (*protos.Transaction, string) {
	uuid := util.GenerateUUID()
	tx, err := protos.NewTransaction(protos.ChaincodeID{Path: "testUrl"}, uuid, "anyfunction", []string{"param1", "param2", uuid})
	testutil.AssertNil(t, err)
	return tx, uuid
}

var genesisDigest = util.GenerateBytesUUID()

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
