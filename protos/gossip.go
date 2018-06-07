package protos

//the max size is limited by the msg size in grpc, so no overflow problem
//it was just an estimation of the whole size of package
func (m *Gossip) EstimateSize() (total int) {

	if m == nil {
		return
	}

	if m.Dig != nil {
		for _, i := range m.Dig.Data {
			total = total + len(i.Signature) + len(i.State) + 8 //the bytes of a num
		}
	}

	total = total + len(m.Payload)
	return
}
