package gossip

import (
	"github.com/abchain/fabric/core/ledger"
)

// TxQuota struct
type HotTxCat struct {
	ledger *ledger.Ledger
	//	txMarkupStates map[string]*TxMarkupState

	// security state
	totalTxCount   int64
	invalidTxCount int64
	invalidTxTime  int64
}
