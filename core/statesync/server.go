package statesync

import (
	"github.com/abchain/fabric/core/ledger"
	_ "github.com/abchain/fabric/core/ledger/statemgmt"
)

type stateServer struct {
	ledger *ledger.Ledger
}

func newStateServer() (s *stateServer) {
	s = new(stateServer)
	s.ledger, _ = ledger.GetLedger()

	return
}
