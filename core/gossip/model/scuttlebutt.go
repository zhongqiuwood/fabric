package gossip_model

//VClock is a type of digest, indicate a partial order nature
type VClock interface {
	Less(VClock) bool
	OutOfRange() bool
}

//Now we can define a digest on scuttlebutt scheme
type ScuttlebuttDigest struct {
	VClock
}

func (d1 ScuttlebuttDigest) Merge(d Digest) Digest {

	d2, ok := d.(ScuttlebuttDigest)
	if !ok {
		panic("Wrong type, not another ScuttlebuttDigest")
	}

	if d2.OutOfRange() {
		return d1
	}

	if d1.Less(d2.VClock) {
		return d1
	} else {
		return d2
	}

}
