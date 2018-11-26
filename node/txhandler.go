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

const chkpcnt = 32

type txNetworkHandlerImpl struct {
	*txnetwork.TxNetworkEntry
	OnExit chan struct{}

	defaultEndorser cred.TxEndorser
	lastSeries      uint64
	lastDigest      []byte
	chkpoints       [chkpcnt]*txPoint
}

func NewTxNetworkHandler(txnet *txnetwork.TxNetworkEntry, last uint64, digest []byte, endorser cred.TxEndorser) (*txNetworkHandlerImpl, error) {

	ret := new(txNetworkHandlerImpl)
	ret.TxNetworkEntry = txnet
	ret.defaultEndorser = endorser
	ret.lastSeries = last
	ret.lastDigest = digest
	ret.OnExit = make(chan struct{})

	txlogger.Infof("Start a txnetwork handler for txnetwork at %d[%x]", ret.lastSeries, ret.lastDigest)

	return ret, nil
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

func (t *txNetworkHandlerImpl) HandleTxs(txs []*txnetwork.PendingTransaction) error {

	txlogger.Debugf("start handling %d txs", len(txs))
	lastDigest := t.lastDigest
	var htxs []*pb.Transaction

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
			htxs = append(htxs, htx)
			lastDigest = txnetwork.GetTxDigest(htx)
			tx.Respond(&pb.Response{pb.Response_SUCCESS, []byte(htx.GetTxid())})
		} else {
			txlogger.Errorf("building complete tx fail: %s, corresponding tx skipped", err)
			tx.Respond(&pb.Response{pb.Response_FAILURE, []byte(err.Error())})
		}
	}

	if len(htxs) == 0 {
		txlogger.Warningf("can't build any tx from %d incoming pending txs", len(txs))
		return nil
	}

	lastSeries := t.lastSeries + len(htxs)
	nextChkPos := uint(t.lastSeries/uint64(txnetwork.PeerTxQueueLen())) + 1

	if err = t.UpdateLocalHotTx(&pb.HotTransactionBlock{htxs, lastSeries}); err == nil {
		t.lastSeries = lastSeries
		t.lastDigest = lastDigest
	} else {
		txlogger.Errorf("updating completed txs into network fail: %s", err)
		//TODO: though rare, should we drop the running thread? or tracking it by some ways?
		return nil
	}

	//update checkpoint and epoch
	chk := uint(t.lastSeries/uint64(txnetwork.PeerTxQueueLen()))
	if chk >= nextChkPos {
		//checkpoint current, and increase chkpoint pos
		txlogger.Infof("Chkpoint %d reach, record [%d:%x]", nextChkPos, t.lastSeries, t.lastDigest)
		t.chkpoints[chk%chkpcnt] = &txPoint{t.lastDigest, t.lastSeries}

		//when we have passed an epoch border, try to update the epoch
		if t.lastSeries > epochInterval{
			epochPoint := uint((t.lastSeries - epochInterval)/uint64(txnetwork.PeerTxQueueLen()))
			if t.chkpoints[epochPoint%chkpcnt] != nil
		}
	}

	pe.lastCache = txPoint{out.lastDigest, out.lastSeries}
	if epoch+epochInterval < out.lastSeries {
		//first we search for a eariest checkpoint
		start := uint(epoch/uint64(txnetwork.PeerTxQueueLen())) + 1
		end := uint(out.lastSeries/uint64(txnetwork.PeerTxQueueLen())) + 1

		for i := start; i < end; i++ {
			if chk, ok := chkps[i]; ok {
				delete(chkps, i)
				txlogger.Infof("Update epoch to chkpoint %d [%d:%x]", i, chk.Series, chk.Digest)
				if err := pe.updateEpoch(chk); err != nil {
					//we can torlence this problem, though ...
					txlogger.Errorf("Update new epoch state [%d] fail: %s", chk.Series, err)
				} else {
					break
				}
				//epoch is forwarded, even we do not update succefully
				epoch = chk.Series
			}
		}
	}

	return nil
}

func (t *txNetworkHandlerImpl) Release() {
	if t == nil {
		return
	} else if t.defaultEndorser != nil {
		t.defaultEndorser.Release()
	}

	close(t.OnExit)

}
