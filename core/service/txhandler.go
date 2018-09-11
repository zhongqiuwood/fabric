package service

import (
	"fmt"
	crypto "github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/core/gossip"
	"github.com/abchain/fabric/core/gossip/txnetwork"
	_ "github.com/abchain/fabric/core/ledger"
	_ "github.com/abchain/fabric/events/litekfk"
	pb "github.com/abchain/fabric/protos"
)

var (
	epochInterval = uint64(512)
	logger        = clisrvLogger
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

	if len(digest) < txnetwork.TxDigestVerifyLen {
		panic("Wrong length of digest")
	}

	tx.Nonce = digest
	return tx
}

func NewTxNetworkHandlerNoSec(stub *gossip.GossipStub) (*txNetworkHandlerImpl, error) {

	self := txnetwork.GetNetworkStatus().GetNetwork(stub)
	if self == nil {
		return nil, fmt.Errorf("No global network created yet")
	}

	selfstatus := self.QuerySelf()

	ret := new(txNetworkHandlerImpl)

	ret.GossipStub = stub
	ret.lastDigest = selfstatus.GetDigest()
	ret.lastSeries = selfstatus.GetNum()

	logger.Infof("Start a txnetwork handler for peer at %d[%x]", ret.lastSeries, ret.lastDigest)

	return ret, nil
}

func NewTxNetworkHandler(stub *gossip.GossipStub, clientName string) (*txNetworkHandlerImpl, error) {

	if sec, err := crypto.InitClient(clientName, nil); err != nil {
		return nil, err
	} else {
		ret, err := NewTxNetworkHandlerNoSec(stub)

		if err != nil {
			return nil, err
		}

		ret.defaultEndorser = sec

		return ret, nil
	}

}

func (t *txNetworkHandlerImpl) updateHotTx(txs *pb.HotTransactionBlock, lastDigest []byte, lastSeries uint64) {

	if err := txnetwork.UpdateLocalHotTx(t.GossipStub, txs); err != nil {
		logger.Error("Update hot transaction fail", err)
	} else {
		t.lastDigest = lastDigest
		t.lastSeries = lastSeries
	}
}

func (t *txNetworkHandlerImpl) updateEpoch() {

	//we do not need to update catalogy for the first time
	if t.epochSeries == 0 {
		t.epochDigest = t.lastDigest
		t.epochSeries = t.lastSeries
		return
	}

	if err := txnetwork.UpdateLocalEpoch(t.GossipStub, t.epochSeries, t.epochDigest); err != nil {
		logger.Error("Update global fail", err)
	} else {
		t.epochDigest = t.lastDigest
		t.epochSeries = t.lastSeries
	}
}

func (t *txNetworkHandlerImpl) HandleTxs(txs []*txnetwork.PendingTransaction) {

	outtxs := new(pb.HotTransactionBlock)

	lastDigest := t.lastDigest
	lastSeries := t.lastSeries
	outtxs.BeginSeries = t.lastSeries + 1

	for _, tx := range txs {

		tx.Transaction = buildPrecededTx(t.lastDigest, tx.Transaction)

		//allow non-sec usage
		if t.defaultEndorser != nil {
			if tx.GetEndorser() != "" {
				if sec, err := crypto.InitClient(tx.GetEndorser(), nil); err == nil {
					sec.EndorseExecuteTransaction(tx.Transaction, tx.GetAttrs()...)
					defer crypto.CloseClient(sec)
				} else {
					logger.Errorf("create new crypto client for %d fail: %s", tx.GetEndorser(), err)
					continue
				}
			} else {
				t.defaultEndorser.EndorseExecuteTransaction(tx.Transaction, tx.GetAttrs()...)
			}
		}

		txdig, err := tx.Digest()
		if err != nil {
			logger.Errorf("Can not get digest for tx %v: %s", tx, err)
			continue
		} else if len(txdig) < txnetwork.TxDigestVerifyLen {
			panic("Wrong code generate tx digest less than 16 bytes")
		}

		lastDigest = txdig[:txnetwork.TxDigestVerifyLen]
		lastSeries = lastSeries + 1

		outtxs.Transactions = append(outtxs.Transactions, tx.Transaction)
		tx.Respond(&pb.Response{pb.Response_SUCCESS, []byte(tx.GetTxid())})

	}

	t.updateHotTx(outtxs, lastDigest, lastSeries)

	if t.epochSeries+epochInterval < t.lastSeries {
		t.updateEpoch()
	}

}

func (t *txNetworkHandlerImpl) Release() {
	if t == nil || t.defaultEndorser == nil {
		return
	}

	crypto.CloseClient(t.defaultEndorser)
}
