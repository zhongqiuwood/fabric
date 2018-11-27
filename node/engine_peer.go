package node

import (
	"fmt"
	_ "github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
)

type txPoint struct {
	Digest []byte
	Series uint64
}

func (pe *PeerEngine) updateEpoch(chk txPoint) error {

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

func (pe *PeerEngine) IsRunning() (bool, error) {

	if pe.exitNotify == nil {
		return false, fmt.Errorf("Engine is not inited")
	}

	select {
	case <-pe.exitNotify:
		return false, pe.runStatus
	default:
		return true, nil
	}

}

func (pe *PeerEngine) Stop() {

	if pe.stopFunc == nil {
		return
	}

	pe.stopFunc()
	<-pe.exitNotify
}

func (pe *PeerEngine) Run() error {

	if isrun, _ := pe.IsRunning(); isrun {
		return fmt.Errorf("Engine is still running")
	}

	lastState, id := pe.GetPeerStatus()
	if lastState == nil {
		return fmt.Errorf("Engine is not set a self peer yet")
	}

	if id != pe.lastID {
		//ok, we start a new handler
		pe.lastCache = txPoint{lastState.GetDigest(), lastState.GetNum()}
		//this is malformed: the laststate from txnetwork IS NOT the actual series
		//which current peer has achieved to and a branched tx-chain may be encountered
		if series := lastState.GetNum(); series != 0 {
			txlogger.Warningf("We try to start a new peer with a running-state (series %d)", series)
			pe.lastCache.Series = series
		}
		pe.lastID = id
	}

	h, err := NewTxNetworkHandler(pe.TxNetworkEntry, pe.lastCache, pe.TxEndorserDef, pe.defaultAttr)
	if err != nil {
		return err
	}

	pe.exitNotify = make(chan interface{})
	//run guard thread
	go func() {
		select {
		case <-h.OnExit:
			pe.runStatus = fmt.Errorf("txnetwork stopped")
			pe.lastCache.Series = h.lastSeries
			pe.lastCache.Digest = h.lastDigest
		case <-pe.GetPeerCtx().Done():
			pe.runStatus = fmt.Errorf("Global exit: %s", pe.GetPeerCtx().Err())
		}

		close(pe.exitNotify)

	}()

	var wctx context.Context
	wctx, pe.stopFunc = context.WithCancel(pe.GetPeerCtx())
	pe.Start(wctx, h)

	return nil
}
