package litekfk

import (
	"fmt"
)

const (
	def_batchsize = 128
	def_maxkeep   = 16
	def_maxDelay  = 2
)

func NewDefaultConfig() *topicConfiguration {

	return &topicConfiguration{
		batchsize: def_batchsize,
		maxkeep:   def_maxkeep,
		maxDelay:  def_maxDelay,
	}
}

func (cf *topicConfiguration) BatchSize(b int) error {

	if b < 2 {
		return fmt.Errorf("Too small batchsize %d", b)
	}

	cf.batchsize = b
	return nil
}

func (cf *topicConfiguration) MaxKeep(k int) error {

	if cf.maxDelay*2 > cf.maxkeep {
		return fmt.Errorf("Too small keep %d, should more than 1/2 of maxdelay %d", k, cf.maxDelay)
	}

	if cf.maxbatch != 0 && cf.maxbatch < k {
		return fmt.Errorf("Too large keep %d, should less than maxbatch %d", k, cf.maxbatch)
	}

	cf.maxkeep = k
	return nil
}

func (cf *topicConfiguration) MaxDelay(d int) error {

	if d > cf.maxDelay*2 {
		return fmt.Errorf("Too large delay %d, should less than 1/2 of maxkeep %d", d, cf.maxkeep)
	}

	cf.maxDelay = d
	return nil
}

func (cf *topicConfiguration) MaxBatch(b int) error {

	if b != 0 && cf.maxkeep > b {
		return fmt.Errorf("Too small maxbatch %d, should larger than maxkeep %d", b, cf.maxkeep)
	}

	cf.maxbatch = b
	return nil
}
