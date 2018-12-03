package node

import (
	"fmt"
	cred "github.com/abchain/fabric/core/cred"
	_ "github.com/abchain/fabric/core/peer"
	_ "github.com/abchain/fabric/protos"
	"golang.org/x/net/context"
)

type txPoint struct {
	Digest []byte
	Series uint64
}

func (pe *PeerEngine) GetServerPoint() ServicePoint {
	return ServicePoint{pe.srvPoint}
}

func (pe *PeerEngine) GenTxEndorser() cred.TxEndorser {
	if pe.defaultEndorser == nil {
		return nil
	}

	ret, err := pe.defaultEndorser.GetEndorser(pe.defaultAttr...)
	if err != nil {
		txlogger.Error("Can not obtain tx endorser:", err)
		return nil
	}

	return ret

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

	h := NewTxNetworkHandler(pe)

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
