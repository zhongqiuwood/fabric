package txnetwork

import (
	pb "github.com/abchain/fabric/protos"
)

func DumpNetwork() map[string]func() map[string]*pb.PeerTxState {

	ret := make(map[string]func() map[string]*pb.PeerTxState)
	global.Lock()
	defer global.Unlock()

	for stub, net := range global.ind {

		ret[stub.GetSelf().GetName()] = func() map[string]*pb.PeerTxState {

			out := make(map[string]*pb.PeerTxState)

			net.RLock()
			defer net.RUnlock()
			//copy index

			for id, item := range net.lruIndex {
				out[id] = item.Value.(*peerStatusItem).PeerTxState
			}

			return out
		}
	}

	return ret
}
