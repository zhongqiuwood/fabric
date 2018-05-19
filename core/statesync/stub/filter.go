package stub

import (
	pb "github.com/abchain/fabric/protos"
)

type StreamFilter struct {
	*pb.PeerEndpoint
}

func (self StreamFilter) QualitifiedPeer(ep *pb.PeerEndpoint) bool {

	//infact we should not need to check if endpoint is myself because it was impossible
	if self.ID.Name == ep.ID.Name {
		return false
	}

	//currently state transfer only work for validator
	return ep.Type == pb.PeerEndpoint_VALIDATOR
}
