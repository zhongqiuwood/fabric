package litekfk

import (
	"container/list"
	"sync"
)

type topicConfiguration struct {
	batchsize int
	maxbatch  int //if 0, not limit the max batch
	maxkeep   int //when batch is passed out, how long it can keep before purge
	//must less than maxbatch (if not 0)
	maxDelay      int //client can start, must not larger than 1/2 maxkeep
	notAutoResume bool
}

type batch struct {
	series  uint64
	wriPos  int
	logs    []interface{}
	readers map[client]bool
}

type readerPos struct {
	*list.Element
	logpos int
}

func (r *readerPos) batch() *batch {
	b, ok := r.Value.(*batch)
	if !ok {
		panic("wrong type, not batch")
	}

	return b
}

type clientReg struct {
	sync.Mutex
	cliCounter   int
	passedSeries uint64
	readers      map[client]*readerPos
}

type topicUint struct {
	sync.RWMutex
	conf    *topicConfiguration
	data    *list.List
	dryRun  bool
	passed  *readerPos
	start   *readerPos
	clients clientReg
}

func InitTopic(conf *topicConfiguration) *topicUint {

	ret := &topicUint{
		conf:   conf,
		dryRun: true,
		data:   list.New(),
	}

	ret.clients.readers = make(map[client]*readerPos)
	pos := ret.data.PushFront(&batch{
		logs:    make([]interface{}, conf.batchsize),
		readers: make(map[client]bool),
	})

	ret.passed = &readerPos{Element: pos}
	ret.start = &readerPos{Element: pos}

	return ret
}

func (t *topicUint) getStart() *readerPos {
	t.RLock()
	defer t.RUnlock()

	return &readerPos{Element: t.start.Element}
}

func (t *topicUint) getTail() *readerPos {
	t.RLock()
	defer t.RUnlock()

	return &readerPos{
		Element: t.data.Back(),
		logpos:  t.data.Back().Value.(*batch).wriPos,
	}
}

func (t *topicUint) _head() *readerPos {

	return &readerPos{Element: t.data.Front()}
}

func (t *topicUint) setDryrun(d bool) {
	t.Lock()
	defer t.Unlock()

	t.dryRun = d
}

func (t *topicUint) setPassed(n *readerPos) {
	t.Lock()
	defer t.Unlock()

	if n.batch().series < t.passed.batch().series {
		panic("Wrong set passed behavior")
	}

	t.passed = n
}

func (t *topicUint) addBatch() (ret *batch) {

	t.Lock()
	defer t.Unlock()

	ret = &batch{
		series:  t.data.Back().Value.(*batch).series + 1,
		logs:    make([]interface{}, t.conf.batchsize),
		readers: make(map[client]bool),
	}

	t.data.PushBack(ret)

	//sequence of following steps is subtle!

	//1. in dryrun (no clients are active) mode, we must manage the passed position
	if t.dryRun {
		for t.passed.batch().series+uint64(t.conf.maxDelay*2) < ret.series {
			t.passed.Element = t.passed.Next()
		}
	}

	//2. adjust start
	for t.start.batch().series+uint64(t.conf.maxDelay) < ret.series {
		t.start.Element = t.start.Element.Next()
	}

	//3. purge
	for pos := t._head(); pos.batch().series+uint64(t.conf.maxkeep) < t.passed.batch().series; pos.Element = t.data.Front() {

		t.data.Remove(t.data.Front())
	}

	//4. force clean
	if t.conf.maxbatch != 0 {
		for pos := t._head(); t.conf.maxbatch < t.data.Len(); t.data.Remove(t.data.Front()) {

			//purge or force unreg
			if pos.batch().series >= t.passed.batch().series {
				t.Unlock()
				//force unreg
				for cli, _ := range pos.batch().readers {
					cli.UnReg(t)
				}
				t.Lock()
			}
		}
	}

	return
}
