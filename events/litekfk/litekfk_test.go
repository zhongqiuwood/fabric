package litekfk

import (
	"fmt"
	"testing"
)

func testConf() *topicConfiguration {

	c := NewDefaultConfig()

	c.batchsize = 3
	c.maxkeep = 3
	c.maxDelay = 1
	c.notAutoResume = true

	return c
}

var defaultTestConfig = testConf()

//all the test only use single writer
func checkbatch(b *batch, expectid int) error {

	if b.series != uint64(expectid) {
		return fmt.Errorf("Id not match: expect %v but get %v", expectid, b.series)
	}

	for i, v := range b.logs {

		if v == nil { //not written yet
			break
		}

		ii, ok := v.(int)
		if !ok {
			return fmt.Errorf("fail type: %v", v)
		}

		expii := expectid*defaultTestConfig.batchsize + i
		if ii != expii {
			return fmt.Errorf("contain not match: expect %v but get %v", expii, ii)
		}
	}

	return nil
}

func dumpbatch(t *testing.T, b *batch) {

	var dumplog []int

	for _, v := range b.logs {
		if v == nil { //not written yet
			break
		}
		ii, ok := v.(int)
		if !ok {
			t.Fatalf("fail type: %v", v)
		}

		dumplog = append(dumplog, ii)
	}

	t.Log("batch: ", b.series, "@", b.wriPos, ":", dumplog, " with", b.readers)
}

func dumpTopic(t *testing.T, title string, u *topicUint) {

	t.Log(" ------ Dumping topic for phase", title)

	for i := u.data.Front(); i != nil; i = i.Next() {
		dumpbatch(t, i.Value.(*batch))
	}

	t.Log("-------- Pass:", u.passed.batch().series)

	t.Log("-------- End dumping topic for phase")

}

func checkTopic(t *testing.T, u *topicUint, tailId int, headId int, startId int) {

	if err := checkbatch(u.getTail().batch(), tailId); err != nil {
		t.Fatal("tail error", err)
	}

	if err := checkbatch(u._head().batch(), headId); err != nil {
		t.Fatal("head error", err)
	}

	if err := checkbatch(u.getStart().batch(), startId); err != nil {
		t.Fatal("start error", err)
	}

}

func TestDryRun(t *testing.T) {

	var counter int

	topic := InitTopic(defaultTestConfig)

	topic.Write(int(counter))
	counter++

	dumpTopic(t, "dryrun phase 1", topic)
	checkTopic(t, topic, 0, 0, 0)

	for ; counter < 10; counter++ {
		topic.Write(int(counter))
	}

	dumpTopic(t, "dryrun phase 2", topic)
	checkTopic(t, topic, 3, 0, 2)

	for ; counter < 20; counter++ {
		topic.Write(int(counter))
	}

	dumpTopic(t, "dryrun phase 3", topic)
	checkTopic(t, topic, 6, 1, 5)

	for ; counter < 39; counter++ {
		topic.Write(int(counter))
	}

	dumpTopic(t, "dryrun phase 4", topic)
	checkTopic(t, topic, 12, 7, 11)
}

func TestOneReader(t *testing.T) {

	var counter int
	var err error

	topic := InitTopic(defaultTestConfig)

	cli1 := topic.Clients().AssignClient()

	_, err = cli1.Read(topic, ReadPos_Default)

	if err != nil {
		t.Fatal("cli1 reader fail", err)
	}

	for ; counter < 4; counter++ {
		topic.Write(int(counter))
	}
}

func TestMutipleReader(t *testing.T) {
}

func TestUnReg(t *testing.T) {
}

func TestForceDropOut(t *testing.T) {
}
