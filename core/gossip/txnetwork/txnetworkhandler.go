package txnetwork

import (
	"fmt"
	crypto "github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	_ "github.com/abchain/fabric/core/ledger"
	_ "github.com/abchain/fabric/events/litekfk"
	pb "github.com/abchain/fabric/protos"
)

var (
	epochInterval = uint64(512)
)

type txNetworkHandlerImpl struct {
	*gossip.GossipStub
	defaultEndorser crypto.Client
	lastDigest      []byte
	lastSeries      uint64
	epochDigest     []byte
	epochSeries     uint64
}

func NewTxNetworkHandlerNoSec(stub *gossip.GossipStub) (*txNetworkHandlerImpl, error) {

	self := global.GetNetwork(stub)
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

func buildPrecededTx(digest []byte, tx *pb.Transaction) *pb.Transaction {
	//TODO: now we just put something in the nonce ...
	tx.Nonce = []byte{2, 3, 3}
	return tx
}

func (t *txNetworkHandlerImpl) updateHotTx(txs *pb.HotTransactionBlock, lastDigest []byte, lastSeries uint64) {

	hotcat := t.GossipStub.GetCatalogHandler(hotTxCatName)
	if hotcat == nil {
		panic("Can't not found corresponding catalogHandler")
	}

	selfUpdate := model.NewscuttlebuttUpdate(nil)
	selfUpdate.UpdateLocal(txPeerUpdate{txs})

	if err := hotcat.Model().Update(selfUpdate); err != nil {
		logger.Errorf("Update hot transaction to self fail!")
	} else {
		t.lastDigest = lastDigest
		t.lastSeries = lastSeries

		//notify our peer is updated
		hotcat.SelfUpdate()
	}
}

func (t *txNetworkHandlerImpl) updateEpoch() {

	//we do not need to update catalogy for the first time
	if t.epochSeries == 0 {
		t.epochDigest = t.lastDigest
		t.epochSeries = t.lastSeries
		return
	}

	globalcat := t.GossipStub.GetCatalogHandler(globalCatName)
	if globalcat == nil {
		panic("Can't not found corresponding catalogHandler")
	}

	newstate := &pb.PeerTxState{Digest: t.epochDigest, Num: t.epochSeries}
	//TODO: make signature

	selfUpdate := model.NewscuttlebuttUpdate(nil)
	selfUpdate.UpdateLocal(peerStatus{newstate})

	if err := globalcat.Model().Update(selfUpdate); err != nil {
		logger.Errorf("Update hot transaction to self fail!")
	} else {
		t.epochDigest = t.lastDigest
		t.epochSeries = t.lastSeries

		//notify our peer is updated
		globalcat.SelfUpdate()
	}
}

func (t *txNetworkHandlerImpl) HandleTxs(txs []PendingTransaction) error {

	outtxs := new(pb.HotTransactionBlock)

	lastDigest := t.lastDigest
	lastSeries := t.lastSeries
	outtxs.BeginSeries = t.lastSeries + 1

	for _, tx := range txs {

		tx.Transaction = buildPrecededTx(t.lastDigest, tx.Transaction)

		//allow non-sec usage
		if t.defaultEndorser != nil {
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
		}

		lastDigest = getTxDigest(tx.Transaction)
		lastSeries = lastSeries + 1

		outtxs.Transactions = append(outtxs.Transactions, tx.Transaction)
	}

	t.updateHotTx(outtxs, lastDigest, lastSeries)

	if t.epochSeries+epochInterval < t.lastSeries {
		t.updateEpoch()
	}

	return nil
}

func (t *txNetworkHandlerImpl) Release() {
	if t == nil || t.defaultEndorser == nil {
		return
	}

	crypto.CloseClient(t.defaultEndorser)
}
