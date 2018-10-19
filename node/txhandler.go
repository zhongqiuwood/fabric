package node

import (
	"fmt"
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/gossip/txnetwork"
	pb "github.com/abchain/fabric/protos"
	"github.com/op/go-logging"
)

var (
	epochInterval = uint64(512)
	txlogger      = logging.MustGetLogger("txhandler")
)

type txTask struct {
	txs        []*pb.Transaction
	lastDigest []byte
	lastSeries uint64
}

type txNetworkHandlerImpl struct {
	output          chan txTask
	defaultEndorser cred.TxEndorser
	lastSeries      uint64
	lastDigest      []byte
	finishedTxs     []*pb.Transaction
}

func NewTxNetworkHandler(entry *txnetwork.TxNetworkEntry, endorser cred.TxEndorser) (*txNetworkHandlerImpl, error) {

	selfstatus, _ := entry.GetPeerStatus()
	if selfstatus == nil {
		return nil, fmt.Errorf("Self peer is not inited yet")
	}

	ret := new(txNetworkHandlerImpl)
	ret.output = make(chan txTask)
	ret.defaultEndorser = endorser
	ret.lastSeries = selfstatus.GetNum()
	ret.lastDigest = selfstatus.GetDigest()

	txlogger.Infof("Start a txnetwork handler for peer at %d[%x]", ret.lastSeries, ret.lastDigest)

	return ret, nil
}

func (t *txNetworkHandlerImpl) updateHotTx(txs *pb.HotTransactionBlock, lastDigest []byte, lastSeries uint64) {

	if err := t.UpdateLocalHotTx(txs); err != nil {
		txlogger.Error("Update hot transaction fail", err)
	} else {
		t.last = txPoint{lastDigest, lastSeries}
		chk := uint(lastSeries/uint64(txnetwork.PeerTxQueueLen())) + 1
		if chk > t.nextChkPos {
			//checkpoint current, and increase chkpoint pos
			txlogger.Infof("Chkpoint %d reach, record [%d:%x]", t.nextChkPos, lastSeries, lastDigest)
			t.chk[t.nextChkPos] = t.last
			t.nextChkPos = chk
		}
	}
}

func (t *txNetworkHandlerImpl) updateEpoch() {

	//first we search for a eariest checkpoint
	start := uint(t.epoch.Series/uint64(txnetwork.PeerTxQueueLen())) + 1
	end := uint(t.last.Series/uint64(txnetwork.PeerTxQueueLen())) + 1

	for i := start; i < end; i++ {
		if chk, ok := t.chk[i]; ok {
			delete(t.chk, i)

			state := &pb.PeerTxState{Digest: chk.Digest, Num: chk.Series}
			var err error

			if t.defaultEndorser != nil {
				state, err = t.defaultEndorser.EndorsePeerState(state)
				if err != nil {
					txlogger.Errorf("Endorse new state [%d] fail: %s", chk.Series, err)
					continue
				}
			}

			err = t.UpdateLocalPeer(sate)
			if err != nil {
				txlogger.Errorf("Update new state [%d] fail: %s", chk.Series, err)
			}

			txlogger.Infof("Update epoch to chkpoint %d [%d:%x]", i, chk.Series, chk.Digest)
			t.epoch = chk
			return
		}
	}

	txlogger.Warning("Can't not find available checkpoint, something may get wrong")
	//reset epoch but not prune
	t.epoch = t.last
	t.chk = make(map[uint]txPoint)
}

//build (complete) tx
func handleTx(lastDigest []byte, tx *pb.Transaction, endorser cred.TxEndorser) (*pb.Transaction, error) {
	tx = txnetwork.GetPrecededTx(lastDigest, tx)

	//allow non-sec usage
	if endorser != nil {
		tx, err = endorser.EndorseTransaction(tx)
		if err != nil {
			return nil, err
		}
	}

	txdig, err := tx.Digest()
	if err != nil {
		return nil, fmt.Errorf("Can not get digest for tx [%v]: %s", tx, err)
	}
	tx.Txid = pb.TxidFromDigest(txdig)

	return tx
}

func (t *txNetworkHandlerImpl) HandleTxs(txs []*txnetwork.PendingTransaction) {

	txlogger.Debugf("start handling %d txs", len(txs))

	var err error
	for _, tx := range txs {

		endorser := tx.GetEndorser()
		if endorser == nil {
			endorser = t.defaultEndorser.GetEndorser()
		} else {
			//count of txs is limited so defer should not overflow
			defer endorser.Release()
		}

		if htx, err := handleTx(lastDigest, tx.Transaction, endorser); err == nil {
			t.lastDigest = txnetwork.GetTxDigest(htx)
			t.lastSeries = t.lastSeries + 1
			t.finishedTxs = append(t.finishedTxs, htx)
			tx.Respond(&pb.Response{pb.Response_SUCCESS, []byte(htx.GetTxid())})
		} else {
			txlogger.Errorf("building complete tx fail: %s, corresponding tx skipped", err)
			tx.Respond(&pb.Response{pb.Response_FAILURE, []byte(err.Error())})
		}
	}

	select {
	case t.output <- txTask{t.finishedTxs, t.lastDigest, t.lastSeries}:
		t.finishedTxs = nil
	default:
		//recv side is not ready ...
	}

	// t.updateHotTx(outtxs, lastDigest, lastSeries)

	// if t.epoch.Series+epochInterval < lastSeries {
	// 	t.updateEpoch()
	// }

}

func (t *txNetworkHandlerImpl) Release() {
	if t == nil {
		return
	} else if t.defaultEndorser != nil {
		t.defaultEndorser.Release()
	}

	close(t.output)

}
