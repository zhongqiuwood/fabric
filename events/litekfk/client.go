package litekfk

import (
	"fmt"
)

type client int

type reader struct {
	cli       client
	target    *topicUint
	current   *readerPos
	end       *readerPos
	autoReset bool
}

var ErrDropOut = fmt.Errorf("Client have been dropped out")
var ErrEOF = fmt.Errorf("EOF")

const (
	ReadPos_Default = iota
	ReadPos_Latest
)

func (c client) UnReg(t *topicUint) {
	t.clients.Lock()
	defer t.clients.Unlock()

	pos, ok := t.clients.readers[c]
	if !ok {
		//has been unreg, over
		return
	}

	delete(pos.batch().readers, c)
	delete(t.clients.readers, c)

	var oldest *readerPos

	//fix passed position
	for _, p := range t.clients.readers {
		if oldest == nil || oldest.batch().series > p.batch().series {
			oldest = p
		}
	}

	if oldest.batch().series > t.clients.passedSeries {
		t.clients.passedSeries = oldest.batch().series
		t.setPassed(oldest)
	}

	if len(t.clients.readers) == 0 {
		t.setDryrun(true)
	}
}

func (c client) Read(t *topicUint, beginPos int) (*reader, error) {
	t.clients.Lock()
	defer t.clients.Unlock()

	pos, ok := t.clients.readers[c]
	if !ok {
		if t.conf.notAutoResume {
			return nil, ErrDropOut
		}

		switch beginPos {
		case ReadPos_Latest:
			pos = t.getTail()
		default:
			pos = t.getStart()
		}

		if len(t.clients.readers) == 0 {

			t.clients.passedSeries = pos.batch().series
			t.setDryrun(false)

		} else if pos.batch().series < t.clients.passedSeries {
			t.clients.passedSeries = pos.batch().series
			t.setPassed(pos)
		}

		t.clients.readers[c] = pos

		pos.Value.(*batch).readers[c] = true
	}

	return &reader{
		current:   pos,
		end:       t.getStart(),
		cli:       c,
		target:    t,
		autoReset: true,
	}, nil
}

func (r *reader) endBatch() bool {
	return r.current.batch().series == r.end.batch().series
}

func (r *reader) _eof() bool {
	return r.current.batch().series == r.end.batch().series &&
		r.current.logpos == r.end.logpos
}

func (r *reader) eof() bool {

	if !r._eof() {
		return false
	}

	if r.autoReset {
		r.end = r.target.getTail()
		if !r._eof() {
			return false
		}
	}

	return true
}

func (r *reader) commit() {
	r.target.clients.Lock()
	defer r.target.clients.Unlock()

	if r.current.logpos == r.target.conf.batchsize {
		//commit to next batch
		curBatch := r.current.Value.(*batch)
		delete(curBatch.readers, r.cli)

		r.current = &readerPos{Element: r.current.Next()}
		r.current.batch().readers[r.cli] = true

		//update passed status
		if len(curBatch.readers) == 0 &&
			curBatch.series == r.target.clients.passedSeries {
			r.target.clients.passedSeries = r.current.batch().series
			r.target.setPassed(r.current)
		}
	}

	r.target.clients.readers[r.cli] = r.current

}

func (r *reader) AutoReset(br bool) {
	r.autoReset = br
}

func (r *reader) ReadOne() (interface{}, error) {

	if r.eof() {
		return nil, ErrEOF
	}

	v := r.current.batch().logs[r.current.logpos]
	r.current.logpos++
	r.commit()

	return v, nil
}

func (r *reader) ReadBatch() (ret []interface{}, e error) {
	if r.eof() {
		e = ErrEOF
		return
	}

	if r.endBatch() {
		ret = r.current.batch().logs[r.current.logpos:r.end.logpos]
		r.current.logpos = r.end.logpos
	} else {
		ret = r.current.batch().logs[r.current.logpos:]
		r.current.logpos = r.target.conf.batchsize
	}

	r.commit()
	return
}
