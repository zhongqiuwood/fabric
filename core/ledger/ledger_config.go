package ledger

import (
	"github.com/spf13/viper"
)

type ledgerConfig struct {
	*viper.Viper
}

func NewLedgerConfig(vp *viper.Viper) *ledgerConfig {

	return &ledgerConfig{vp}
}
