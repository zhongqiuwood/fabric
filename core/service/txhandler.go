package service

import (
	"fmt"
	crypto "github.com/abchain/fabric/core/crypto"
	"github.com/abchain/fabric/core/gossip/txnetwork"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
)

var (
	epochInterval = uint64(512)
	logger        = logging.MustGetLogger("txhandler")
)

type txPoint struct {
	Digest []byte
	Series uint64
}

type txNetworkHandlerImpl struct {
	txnetwork.TxNetworkUpdate
	defaultEndorser crypto.Client
	last            txPoint
	epoch           txPoint
	chk             map[uint]txPoint
	nextChkPos      uint
}

func buildPrecededTx(digest []byte, tx *pb.Transaction) *pb.Transaction {

	if len(digest) < txnetwork.TxDigestVerifyLen {
		panic("Wrong length of digest")
	}

	tx.Nonce = digest
	return tx
}

func NewTxNetworkHandlerNoSec(entry txnetwork.TxNetworkEntry) (*txNetworkHandlerImpl, error) {

	self := entry.GetNetwork()
	if self == nil {
		return nil, fmt.Errorf("No global network created yet")
	}

	selfstatus := self.QuerySelf()

	ret := new(txNetworkHandlerImpl)

	ret.TxNetworkUpdate = entry
	ret.last = txPoint{selfstatus.GetDigest(), selfstatus.GetNum()}
	ret.epoch = ret.last
	ret.nextChkPos = uint(selfstatus.GetNum()/uint64(txnetwork.PeerTxQueueLen())) + 1
	ret.chk = make(map[uint]txPoint)

	logger.Infof("Start a txnetwork handler for peer at %d[%x]", ret.last.Series, ret.last.Digest)

	return ret, nil
}

func NewTxNetworkHandler(entry txnetwork.TxNetworkEntry, clientName string) (*txNetworkHandlerImpl, error) {

	if sec, err := crypto.InitClient(clientName, nil); err != nil {
		return nil, err
	} else {
		ret, err := NewTxNetworkHandlerNoSec(entry)

		if err != nil {
			return nil, err
		}

		ret.defaultEndorser = sec

		return ret, nil
	}

}

func (t *txNetworkHandlerImpl) updateHotTx(txs *pb.HotTransactionBlock, lastDigest []byte, lastSeries uint64) {

	if err := t.UpdateLocalHotTx(txs); err != nil {
		logger.Error("Update hot transaction fail", err)
	} else {
		t.last = txPoint{lastDigest, lastSeries}
		chk := uint(lastSeries/uint64(txnetwork.PeerTxQueueLen())) + 1
		if chk > t.nextChkPos {
			//checkpoint current, and increase chkpoint pos
			logger.Infof("Chkpoint %d reach, record [%d:%x]", t.nextChkPos, lastSeries, lastDigest)
			t.chk[t.nextChkPos] = t.last
			t.nextChkPos = chk
		}
	}
}

func (t *txNetworkHandlerImpl) updateEpoch() {

	//first we search for a eariest checkpoint
	start := uint(t.epoch.Series/uint64(txnetwork.PeerTxQueueLen())) + 1
	end := uint(t.last.Series/uint64(txnetwork.PeerTxQueueLen())) + 1

	doUd := func(updated txPoint) {
		if err := t.UpdateLocalEpoch(updated.Series, updated.Digest); err != nil {
			logger.Error("Update global fail:", err)
		}
	}

	for i := start; i < end; i++ {
		if chk, ok := t.chk[i]; ok {
			delete(t.chk, i)
			//always update epoch, even we do not update succefully
			logger.Infof("Update epoch to chkpoint %d [%d:%x]", i, chk.Series, chk.Digest)
			t.epoch = chk
			doUd(chk)
			return
		}
	}

	logger.Warning("Can't not find available checkpoint, something may get wrong")
	//reset epoch but not prune
	t.epoch = t.last
	t.chk = make(map[uint]txPoint)
}

func (t *txNetworkHandlerImpl) HandleTxs(txs []*txnetwork.PendingTransaction) {

	outtxs := new(pb.HotTransactionBlock)

	lastDigest := t.last.Digest
	lastSeries := t.last.Series
	outtxs.BeginSeries = lastSeries + 1

	logger.Debugf("start handling %d txs", len(txs))

	var err error
	for _, tx := range txs {

		tx.Transaction = buildPrecededTx(lastDigest, tx.Transaction)

		var endorser crypto.Client
		if tx.GetEndorser() != "" {
			if sec, err := crypto.InitClient(tx.GetEndorser(), nil); err == nil {
				endorser = sec
				//may stack a bunch of closeClient but should be ok (not more than txnetwork.maxOutputBatch)
				defer crypto.CloseClient(sec)
			} else {
				logger.Errorf("create new crypto client for %d fail: %s, corresponding tx skipped", tx.GetEndorser(), err)
				continue
			}
		} else {
			endorser = t.defaultEndorser
		}

		//allow non-sec usage
		if endorser != nil {
			tx.Transaction, err = endorser.EndorseExecuteTransaction(tx.Transaction, tx.GetAttrs()...)
			if err != nil {
				logger.Errorf("endorse tx fail: %s, corresponding tx skipped", err)
				continue
			}
		}

		//build (complete) tx
		txdig, err := tx.Digest()
		if err != nil {
			logger.Errorf("Can not get digest for tx %v: %s", tx, err)
			continue
		}
		tx.Txid = pb.TxidFromDigest(txdig)

		lastDigest = txnetwork.GetTxDigest(tx.Transaction)
		lastSeries = lastSeries + 1

		outtxs.Transactions = append(outtxs.Transactions, tx.Transaction)
		tx.Respond(&pb.Response{pb.Response_SUCCESS, []byte(tx.GetTxid())})

	}

	t.updateHotTx(outtxs, lastDigest, lastSeries)

	if t.epoch.Series+epochInterval < lastSeries {
		t.updateEpoch()
	}

}

func (t *txNetworkHandlerImpl) Release() {
	if t == nil || t.defaultEndorser == nil {
		return
	}

	crypto.CloseClient(t.defaultEndorser)
}
