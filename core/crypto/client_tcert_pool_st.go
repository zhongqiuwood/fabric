/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package crypto

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/abchain/fabric/core/crypto/primitives"
	"github.com/op/go-logging"
	"sort"
	"sync"
	"time"
)

var poolLogger = logging.MustGetLogger("tcertpool")

//TCertDBBlock is an object used to store the TCert in the database. A raw field is used to represent the TCert and the preK0, a string field is use to the attributesHash.
type TCertDBBlock struct {
	tCertDER       []byte
	attributesHash string
	preK0          []byte
}

type tCertPoolSingleThreadImpl struct {
	client *clientImpl

	// empty bool
	// length map[string]int
	// tCerts map[string][]*TCertBlock

	tcblMap map[string]tcertBlockList

	m sync.RWMutex
}

func (tCertPool *tCertPoolSingleThreadImpl) init(client *clientImpl) error {
	tCertPool.client = client
	tCertPool.client.Debug("Init TCert Pool...")
	tCertPool.tcblMap = make(map[string]tcertBlockList)
	return nil
}

//Start starts the pool processing.
func (tCertPool *tCertPoolSingleThreadImpl) Start() (err error) {

	tCertPool.client.Debug("Starting TCert Pool...")

	tCertPool.m.Lock()
	defer tCertPool.m.Unlock()

	// Load unused TCerts if any
	tCertDBBlocks, err := tCertPool.client.ks.loadUnusedTCerts()
	if err != nil {
		tCertPool.client.Errorf("Failed loading TCerts from cache: [%s]", err)

		return
	}

	if len(tCertDBBlocks) > 0 {

		tCertPool.client.Debug("TCerts in cache found! Loading them...")

		for _, tCertDBBlock := range tCertDBBlocks {
			tCertBlock, err := tCertPool.client.getTCertFromDER(tCertDBBlock)
			if err != nil {
				tCertPool.client.Errorf("Failed paring TCert [% x]: [%s]", tCertDBBlock.tCertDER, err)

				continue
			}
			tCertPool.AddTCert(tCertBlock)
		}
	} //END-IF

	return
}

//Stop stops the pool.
func (tCertPool *tCertPoolSingleThreadImpl) Stop() (err error) {

	tCertPool.m.Lock()
	defer tCertPool.m.Unlock()

	for k := range tCertPool.tcblMap {
		certList := tCertPool.tcblMap[k]
		tCertPool.client.ks.storeUnusedTCerts(certList.GetUnusedTCertBlocks())
	}

	tCertPool.client.Debug("Store unused TCerts...done!")

	return
}

//calculateAttributesHash generates a unique hash using the passed attributes.
func calculateAttributesHash(attributes []string) (attrHash string) {

	keys := make([]string, len(attributes))

	for _, k := range attributes {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	values := make([]byte, len(keys))

	for _, k := range keys {
		vb := []byte(k)
		for _, bval := range vb {
			values = append(values, bval)
		}
	}
	attributesHash := primitives.Hash(values)
	return hex.EncodeToString(attributesHash)

}

//GetNextTCert returns a TCert from the pool valid to the passed attributes. If no TCert is available TCA is invoked to generate it.
func (tCertPool *tCertPoolSingleThreadImpl) GetNextTCerts(nCerts int, attributes ...string) ([]*TCertBlock, error) {

	tCertPool.m.Lock()
	defer tCertPool.m.Unlock()

	attributesHash := calculateAttributesHash(attributes)
	blocks := make([]*TCertBlock, nCerts)
	for i := 0; i < nCerts; i++ { // nCerts
		// tCertPool.client.Debugf("********** %d *******\n", i)
		block, err := tCertPool.getNextTCert(attributesHash, attributes...)
		if err != nil {
			return nil, err
		}
		blocks[i] = block
	}
	return blocks, nil
}

func (tCertPool *tCertPoolSingleThreadImpl) getNextTCert(attrHash string, attributes ...string) (tCert *TCertBlock, err error) {
	tCertPool.client.Debug("getNextTCert ...", attrHash)
	tcbl := tCertPool.getTCertBlockList(attrHash)

	tCertBlk, err := tcbl.Get()
	if err != nil {
		num := tcbl.GetUpdateNum()
		tCertPool.refillTCerts(num, attrHash, attributes...)
		tCertBlk, err = tcbl.Get()
		if err != nil {
			return nil, err
		}
	}

	return tCertBlk, nil
}

// AddTCert adds a TCert into the pool is invoked by the client after TCA is called.
func (tCertPool *tCertPoolSingleThreadImpl) AddTCert(tCertBlock *TCertBlock) error {
	tCertPool.client.Debugf("Adding new Cert to tCertPool")

	//AddTCert is re-entried by other routine so no lock is applied

	tcbl := tCertPool.getTCertBlockList(tCertBlock.attributesHash)
	tcbl.Add(tCertBlock)
	return nil
}

func (tCertPool *tCertPoolSingleThreadImpl) refillTCerts(num int, attributesHash string, attributes ...string) error {
	tCertPool.client.Debugf("refillTCerts %d \n", num)
	if err := tCertPool.client.getTCertsFromTCA(attributesHash, attributes, num); err != nil {
		return fmt.Errorf("Failed loading TCerts from TCA")
	}
	return nil
}

func (tCertPool *tCertPoolSingleThreadImpl) getTCertBlockList(attrHash string) tcertBlockList {

	v, ok := tCertPool.tcblMap[attrHash]
	if !ok {
		isReuseable := tCertPool.client.conf.getTCertReusedEnable()
		batchSize := tCertPool.client.conf.getTCertBatchSize()
		if isReuseable {
			rr := tCertPool.client.conf.getTCertReusedRoundRobin()
			counterLimit := tCertPool.client.conf.getTCertReusedBatch()
			reusedUpdateSecond := tCertPool.client.conf.getTCertReusedUpdateSecond()
			var ffilter func(*TCertBlock) bool
			if counterLimit > 0 {
				ffilter = func(blk *TCertBlock) bool {
					return blk.isExpired() || blk.counter >= counterLimit
				}
				batchSize = batchSize/counterLimit + 1
			} else {
				ffilter = func(blk *TCertBlock) bool {
					return blk.isUpdateExpired(reusedUpdateSecond)
				}
				batchSize = 1
			}

			if rr > 1 {
				tcbl := newTCertBlockList(attrHash, rr*batchSize, "RoundRobin").(*tcertBlockListRoundRobin)
				tcbl.filter = ffilter
				tCertPool.tcblMap[attrHash] = tcbl
				return tcbl
			} else {
				tcbl := newTCertBlockList(attrHash, batchSize, "Reuse").(*tcertBlockListReuse)
				tcbl.filter = ffilter
				tCertPool.tcblMap[attrHash] = tcbl
				return tcbl
			}
		}
		tcbl := newTCertBlockList(attrHash, batchSize, "Normal")
		tCertPool.tcblMap[attrHash] = tcbl
		return tcbl
	}
	return v
}

const (
	fivemin = time.Minute * 5
	oneweek = time.Hour * 24 * 7
)

//TCertBlock is an object that include the generated TCert and the attributes used to generate it.
type TCertBlock struct {
	tCert          tCert
	attributesHash string
	counter        int
}

func (tCertBlock *TCertBlock) GetTCert() tCert {
	return tCertBlock.tCert
}

func (tCertBlock *TCertBlock) GetAttrHash() string {
	return tCertBlock.attributesHash
}

// expired if tcert NotAfter in 5 min
func (tcertBlock *TCertBlock) isExpired() bool {
	tsNow := time.Now()
	notAfter := tcertBlock.GetTCert().GetCertificate().NotAfter
	poolLogger.Debugf("#isExpired: %s now: %s deadline: %s \n ", tsNow.Add(fivemin).After(notAfter), tsNow, notAfter)
	if tsNow.Add(fivemin).After(notAfter) {
		return true
	}
	return false
}

func (tcertBlock *TCertBlock) isCounterOverflow(counterLimit int) bool {
	return counterLimit > 0 && tcertBlock.counter >= counterLimit
}

// expired if tcert NotAfter in 5 min
func (tcertBlock *TCertBlock) isUpdateExpired(reusedUpdateSecond int) bool {
	notAfter := tcertBlock.GetTCert().GetCertificate().NotAfter
	tsNow := time.Now()
	if reusedUpdateSecond == 0 {

		//default reusedUpdateSecond is one week or 1/3 of the cert life, the shorter takes
		notBefore := tcertBlock.GetTCert().GetCertificate().NotBefore
		timeDel := notAfter.Sub(notBefore) / 3

		if timeDel < time.Duration(oneweek) {
			reusedUpdateSecond = int(timeDel)
		} else {
			reusedUpdateSecond = int(oneweek)
		}
	}

	poolLogger.Debugf("#isUpdateExpired now: %s deadline: %s \n ", tsNow, notAfter)

	return tsNow.Add(time.Duration(reusedUpdateSecond)).After(notAfter)

}

type tcertBlockList interface {
	Add(tcBlk *TCertBlock)
	Get() (*TCertBlock, error)
	GetUpdateNum() int
	GetUnusedTCertBlocks() []*TCertBlock
}

func newTCertBlockList(attrHash string, size int, mode string) tcertBlockList {
	normal := &tcertBlockListNormal{
		attrHash: attrHash,
		blkList:  make([]*TCertBlock, size),
	}
	switch mode {
	case "Normal":
		return normal
	case "Reuse":
		return &tcertBlockListReuse{
			tcertBlockListNormal: normal,
		}
	case "RoundRobin":
		ru := &tcertBlockListReuse{
			tcertBlockListNormal: normal,
		}
		return &tcertBlockListRoundRobin{
			tcertBlockListReuse: ru,
		}
	}
	return normal
}

type tcertBlockListNormal struct {
	attrHash string
	blkList  []*TCertBlock
	beg      uint64
	end      uint64

	sync.Mutex
}

func (tcbl *tcertBlockListNormal) Add(tcBlk *TCertBlock) {
	tcbl.Lock()
	defer tcbl.Unlock()

	//full
	if int(tcbl.end-tcbl.beg) >= len(tcbl.blkList) {
		poolLogger.Debugf("#TCBL ADD# full \n ")
		return
	}

	pos := int(tcbl.end % uint64(len(tcbl.blkList)))
	tcbl.blkList[pos] = tcBlk
	tcbl.end = tcbl.end + 1

	poolLogger.Debugf("#TCBL ADD# hash: %s at: %d size: %d  \n", tcbl.attrHash, pos, tcbl.end-tcbl.beg)
}

type EmptyListErrType struct {
	error
}

var emptylist = &EmptyListErrType{errors.New("empty")}

func (tcbl *tcertBlockListNormal) get() (*TCertBlock, error) {
	if tcbl.end == tcbl.beg {
		return nil, emptylist
	}

	pos := int(tcbl.beg % uint64(len(tcbl.blkList)))

	tcBlk := tcbl.blkList[pos]
	//always add a counter
	tcBlk.counter++

	poolLogger.Debugf("#TCBL Get# hash: %s at: %d size: %d  \n", tcbl.attrHash, pos, tcbl.end-tcbl.beg)
	return tcBlk, nil
}

func (tcbl *tcertBlockListNormal) pop() {
	if tcbl.end > tcbl.beg {
		tcbl.beg = tcbl.beg + 1
	}
}

func (tcbl *tcertBlockListNormal) Get() (*TCertBlock, error) {

	tcbl.Lock()
	defer tcbl.Unlock()

	tcbl.remove((*TCertBlock).isExpired)

	defer tcbl.pop()
	return tcbl.get()
}

func (tcbl *tcertBlockListNormal) GetUnusedTCertBlocks() []*TCertBlock {

	tcbl.Lock()
	defer tcbl.Unlock()

	//	poolLogger.Debugf("#GetUnusedTCertBlocks  %d TCertBlock to fillter  \n", tcbl.len)
	tcbs := make([]*TCertBlock, 0, len(tcbl.blkList))
	for i := tcbl.beg; i < tcbl.end; i++ {
		pos := int(tcbl.beg % uint64(len(tcbl.blkList)))
		block := tcbl.blkList[pos]
		if block.counter > 0 {
			continue
		}
		tcbs = append(tcbs, block)
	}
	poolLogger.Debugf("#GetUnusedTCertBlocks  %d TCertBlock unused  \n", len(tcbs))
	return tcbs
}

func (tcbl *tcertBlockListNormal) remove(filter func(*TCertBlock) bool) (rmcnt int) {

	for ; tcbl.end > tcbl.beg; tcbl.beg++ {

		pos := int(tcbl.beg % uint64(len(tcbl.blkList)))
		if !filter(tcbl.blkList[pos]) {
			return
		}
		rmcnt++
	}

	return
}

type tcertBlockListReuse struct {
	*tcertBlockListNormal
	filter func(*TCertBlock) bool
}

func (tcbl *tcertBlockListReuse) Get() (*TCertBlock, error) {

	tcbl.Lock()
	defer tcbl.Unlock()

	tcbl.remove(tcbl.filter)

	return tcbl.get()
}

type tcertBlockListRoundRobin struct {
	*tcertBlockListReuse

	cursor int
	rrcnt  int
}

func (tcbl *tcertBlockListRoundRobin) resumePos(pos uint64) {
	tcbl.beg = pos
}

func (tcbl *tcertBlockListRoundRobin) Get() (*TCertBlock, error) {

	tcbl.Lock()
	defer tcbl.Unlock()

	rmcnt := tcbl.remove(tcbl.filter)
	if rmcnt%tcbl.rrcnt != 0 {
		//always remove a group of rr
		tcbl.beg = tcbl.beg + uint64(tcbl.rrcnt-rmcnt%tcbl.rrcnt)
		if tcbl.beg > tcbl.end {
			tcbl.beg = tcbl.end
		}
	}

	//need more slot to do a round-robin
	if int(tcbl.end-tcbl.beg) < tcbl.rrcnt {
		return nil, emptylist
	}

	defer tcbl.resumePos(tcbl.beg)

	tcbl.beg = tcbl.beg + uint64(tcbl.cursor)

	tcbl.cursor++
	if tcbl.cursor >= tcbl.rrcnt {
		tcbl.cursor = 0
	}

	return tcbl.get()
}

func (tcbl *tcertBlockListNormal) GetUpdateNum() int {

	tcbl.Lock()
	defer tcbl.Unlock()

	return len(tcbl.blkList) - int(tcbl.end-tcbl.beg)
}
