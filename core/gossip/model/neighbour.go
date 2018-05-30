package gossip_model

type NeighbourHelper interface {
	//If we still allow send update to this peer (return NON-error)
	//This method also track the push-status of local
	AllowPushUpdate() (Update, error)
}

type NeighbourPeer struct {
	workPuller *puller
	global     *Model
	helper     NeighbourHelper
}

func NewNeighbourPeer(model *Model, helper NeighbourHelper) *NeighbourPeer {

	nP := &NeighbourPeer{
		helper: helper,
		global: model,
	}
	return nP

}

//peer should reply update (that is, also "push" its status to far-end) if
//possible, ignoring any other conditions
func (nP *NeighbourPeer) AcceptDigest(dg map[string]Digest) Update {
	ud, err := nP.helper.AllowPushUpdate()
	if err != nil {
		//simply omit this message
		return nil
	}

	ud = nP.global.RecvPullDigest(dg, ud)
	return ud
}

func (nP *NeighbourPeer) IsPulling() bool {
	return nP.workPuller != nil
}

//accept update, normally it should only accept when we are under a "pull" task
//so we just send it to the puller
func (nP *NeighbourPeer) AcceptUpdate(ud Update) {
	nP.workPuller.NotifyUpdate(ud)
	//**any puller (if exist) finish its task and neighbour never track it any longer**
	nP.workPuller = nil

}
