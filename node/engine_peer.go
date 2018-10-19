package node

import (
	"fmt"
	cred "github.com/abchain/fabric/core/cred"
	"github.com/abchain/fabric/core/gossip/txnetwork"
	"github.com/abchain/fabric/core/ledger"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
)

type txPoint struct {
	Digest []byte
	Series uint64
}

func (pe *PeerEngine) updateEpoch(chk txPoint) error{
	
	epoch = chk.Series
	state := &pb.PeerTxState{Digest: chk.Digest, Num: chk.Series}
	var err error

	if pe.TxEndorserDef != nil {
		state, err = pe.TxEndorserDef.EndorsePeerState(state)
		if err != nil {
			return err
		}
	}
	
	return pe.TxNetworkEntry.UpdateLocalPeer(state)
}

func (pe *PeerEngine) Run() error {

	if pe.runStatus != nil {
		return fmt.Errorf("Engine is still running")
	}

	pe.runStatus = make(chan error)

	var endorser cred.TxEndorser
	var err error
	if pe.TxEndorserDef != nil {
		//in most case we generate a default endorser with empty attributes,
		//but it was also ok to create it with some attr.
		endorser, err = pe.TxEndorserDef.GetEndorser(pe.defaultAttr...)
		if err != nil {
			return err
		}
	}

	h, err := NewTxNetworkHandler(pe.TxNetworkEntry, endorser)
	if err != nil {
		return err
	}

	//take out the start position from txhandler, small hack ...
	startSeries := h.lastSeries

	go func(entry *txnetwork.TxNetworkEntry) {

		var err error
		defer func(){

			if err != nil{
				//maybe it should be fatal ...
				logger.Errorf(" *** Peer Engine exit unexpectly: %s ***", err)
			}

			pe.runStatus <-err
		}

		var epoch = startSeries
		var chkps = make(map[uint]txPoint)
		var nextChkPos = uint(startSeries/uint64(txnetwork.PeerTxQueueLen())) + 1

		for out := range h.output {
			err = entry.UpdateLocalHotTx(&pb.HotTransactionBlock{out.txs, out.lastSeries + 1 - len(out.txs)})
			if err != nil{
				return
			}

			chk := uint(out.lastSeries/uint64(txnetwork.PeerTxQueueLen())) + 1
			if chk > nextChkPos {
				//checkpoint current, and increase chkpoint pos
				txlogger.Infof("Chkpoint %d reach, record [%d:%x]", nextChkPos, out.lastSeries, out.lastDigest)
				chkps[nextChkPos] = txPoint{out.lastDigest, out.lastSeries}
				nextChkPos = chk
			}
			
			if epoch+epochInterval < out.lastSeries {
				//first we search for a eariest checkpoint
				start := uint(epoch/uint64(txnetwork.PeerTxQueueLen())) + 1
				end := uint(out.lastSeries/uint64(txnetwork.PeerTxQueueLen())) + 1

				for i := start; i < end; i++ {
					if chk, ok := chkps[i]; ok {
						delete(chkps, i)
						txlogger.Infof("Update epoch to chkpoint %d [%d:%x]", i, chk.Series, chk.Digest)
						if err := pe.updateEpoch(chk); err != nil{
							//we can torlence this problem, though ...
							txlogger.Errorf("Update new epoch state [%d] fail: %s", chk.Series, err)
						}else{
							break
						}
						//epoch is forwarded, even we do not update succefully
						epoch = chk.Series
					}
				}				
			}			
		}
	}()

}
