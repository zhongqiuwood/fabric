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

func NewTxNetworkHandler(last uint64, digest []byte, endorser cred.TxEndorser) (*txNetworkHandlerImpl, error) {

	ret := new(txNetworkHandlerImpl)
	ret.output = make(chan txTask)
	ret.defaultEndorser = endorser
	ret.lastSeries = last
	ret.lastDigest = digest

	txlogger.Infof("Start a txnetwork handler for peer at %d[%x]", ret.lastSeries, ret.lastDigest)

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
