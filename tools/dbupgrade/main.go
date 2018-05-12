package main

import (
	"fmt"
	"github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/peerex"
)

func main() {

	cfg := peerex.GlobalConfig{LogRole: "client"}

	err := cfg.InitGlobal(false)
	if err != nil {
		panic(err)
	}

	db.Start()
	defer db.Stop()
	err = ledger.UpgradeLedger(false)

	if err != nil {
		fmt.Print("Upgrade fail:", err)
	}

	fmt.Print("Upgrade Finished")
}
