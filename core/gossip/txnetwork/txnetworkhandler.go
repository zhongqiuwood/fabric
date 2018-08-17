package txnetwork

import (
	_ "fmt"
	crypto "github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	_ "github.com/abchain/fabric/core/ledger"
	_ "github.com/abchain/fabric/events/litekfk"
	pb "github.com/abchain/fabric/protos"
)

type txNetworkHandlerImpl struct {
	*gossip.GossipStub
	defaultEndorser crypto.Client
	lastDigest      []byte
	lastSeries      uint64
	epochDigest     []byte
	epochSeries     uint64
}

func buildPrecededTx(digest []byte, tx *pb.Transaction) *pb.Transaction {
	//TODO: now we just put something in the nonce ...
	tx.Nonce = []byte{2, 3, 3}
	return tx
}

func (t *txNetworkHandlerImpl) updateEpoch() {

}

func (t *txNetworkHandlerImpl) HandleTxs(txs []PendingTransaction) error {

	selfUpdate := model.NewscuttlebuttUpdate(nil)
	txUpdate := txPeerUpdate{new(pb.HotTransactionBlock)}

	lastDigest := t.lastDigest
	lastSeries := t.lastSeries

	for _, tx := range txs {

		tx.Transaction = buildPrecededTx(t.lastDigest, tx.Transaction)

		if tx.endorser != "" {
			if sec, err := crypto.InitClient(tx.endorser, nil); err == nil {
				sec.EndorseExecuteTransaction(tx.Transaction, tx.attrs...)
				defer crypto.CloseClient(sec)
			} else {
				logger.Errorf("create new crypto client for %d fail: %s", tx.endorser, err)
				continue
			}
		} else {
			t.defaultEndorser.EndorseExecuteTransaction(tx.Transaction, tx.attrs...)
		}

		lastDigest = getTxDigest(tx.Transaction)
		lastSeries = lastSeries + 1

		txUpdate.Transactions = append(txUpdate.Transactions, tx.Transaction)
	}

	selfUpdate.UpdateLocal(txUpdate)

	hotcat := t.GossipStub.GetCatalogHandler(hotTxCatName)
	if hotcat == nil {
		panic("Can't not found corresponding catalogHandler")
	}
	if err := hotcat.Model().Update(selfUpdate); err != nil {
		logger.Errorf("Update hot transaction to self fail!")
	} else {
		t.lastDigest = lastDigest
		t.lastSeries = lastSeries

		//notify our peer is updated
		hotcat.SelfUpdate()
	}

	return nil
}
