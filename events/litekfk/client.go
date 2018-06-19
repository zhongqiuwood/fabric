package litekfk

import (
	"fmt"
)

type client int

type readerPosCache struct {
	*batch
	logpos int
}

type reader struct {
	cli       client
	target    *topicUint
	current   *readerPosCache
	end       *readerPosCache
	autoReset bool
}

var ErrDropOut = fmt.Errorf("Client have been dropped out")
var ErrEOF = fmt.Errorf("EOF")

const (
	ReadPos_Empty = iota
	ReadPos_Default
	ReadPos_Latest
)

const (
	ReadPos_Resume = 256 + iota
	ReadPos_ResumeOrDefault
	ReadPos_ResumeOrLatest
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

	if len(t.clients.readers) == 0 {
		t.setDryrun(true)
	} else {
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
	}
}

func (c client) Read(t *topicUint, beginPos int) (*reader, error) {
	t.clients.Lock()
	defer t.clients.Unlock()

	pos, ok := t.clients.readers[c]
	if !ok {

		switch beginPos & (ReadPos_Resume - 1) {
		case ReadPos_Latest:
			pos = t.getTail()
		case ReadPos_Default:
			pos = t.getStart()
		default:
			return nil, ErrDropOut
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
	} else {
		if (beginPos & ReadPos_Resume) == 0 {
			return nil, fmt.Errorf("Read options not allow resume")
		}
	}

	return &reader{
		current:   pos.toCache(),
		end:       t.getTail().toCache(),
		cli:       c,
		target:    t,
		autoReset: true,
	}, nil
}

func (r *reader) endBatch() bool {
	return r.current.series == r.end.series
}

func (r *reader) _eof() bool {
	return r.current.series == r.end.series &&
		r.current.logpos == r.end.logpos
}

func (r *reader) eof() bool {

	if !r._eof() {
		return false
	}

	if r.autoReset {
		r.end = r.target.getTail().toCache()
		if !r._eof() {
			return false
		}
	}

	return true
}

func (r *reader) commit() error {
	r.target.clients.Lock()
	defer r.target.clients.Unlock()

	pos, ok := r.target.clients.readers[r.cli]
	if !ok {
		return ErrDropOut
	}

	if r.current.logpos == r.target.conf.batchsize {

		//commit to next batch
		curBatch := pos.batch()
		delete(curBatch.readers, r.cli)

		pos = pos.next()
		pos.batch().readers[r.cli] = true
		r.current = pos.toCache()

		//update passed status
		if len(curBatch.readers) == 0 &&
			curBatch.series == r.target.clients.passedSeries {
			r.target.clients.passedSeries = pos.batch().series
			r.target.setPassed(pos)
		}

		r.target.clients.readers[r.cli] = pos

	} else {
		//simple update logpos
		pos.logpos = r.current.logpos
	}

	return nil
}

func (r *reader) AutoReset(br bool) {
	r.autoReset = br
}

func (r *reader) Position() uint64 {
	return r.target._position(r.current.batch, r.current.logpos)
}

func (r *reader) ReadOne() (interface{}, error) {

	if r.eof() {
		return nil, ErrEOF
	}

	v := r.current.logs[r.current.logpos]
	r.current.logpos++

	return v, r.commit()
}

func (r *reader) ReadBatch() (ret []interface{}, e error) {
	if r.eof() {
		e = ErrEOF
		return
	}

	if r.endBatch() {
		ret = r.current.logs[r.current.logpos:r.end.logpos]
		r.current.logpos = r.end.logpos
	} else {
		ret = r.current.logs[r.current.logpos:]
		r.current.logpos = r.target.conf.batchsize
	}

	e = r.commit()
	return
}
