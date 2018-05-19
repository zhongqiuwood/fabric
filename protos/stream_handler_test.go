package protos

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"testing"
)

type dummyError struct {
	error
}

type dummyHandler struct {
}

func (*dummyHandler) Stop() {}

func (h *dummyHandler) Tag() string { return "Dummy" }

func (h *dummyHandler) EnableLoss() bool { return true }

func (h *dummyHandler) NewMessage() proto.Message { return nil }

func (h *dummyHandler) HandleMessage(m proto.Message) error {
	return &dummyError{fmt.Errorf("No implement")}
}

//fail so we won't send message actually
func (h *dummyHandler) BeforeSendMessage(proto.Message) error {
	return &dummyError{fmt.Errorf("Must fail")}
}
func (h *dummyHandler) OnWriteError(e error) {}

type verifySet map[string]bool

func newVSet(name []string) verifySet {

	vset := make(map[string]bool)

	for _, n := range name {
		vset[n] = false
	}

	return verifySet(vset)
}

func (v verifySet) allPass(t *testing.T) {

	for n, ok := range v {
		if !ok {
			t.Fatal("Verify set have fail item", n)
		}
	}
}

func (v verifySet) excludeFail() (ret []string) {

	for n, ok := range v {
		if !ok {
			ret = append(ret, n)
		}
	}

	for _, n := range ret {
		delete(v, n)
	}

	return
}

func toPeerId(name []string) (ret []*PeerID) {
	for _, n := range name {
		ret = append(ret, &PeerID{n})
	}

	return
}

func Test_StreamHub_OverHandler(t *testing.T) {

	tstub := NewStreamStub(nil)

	peerNames := []string{"peer1", "peer2", "peer3", "peer4", "peer5", "peer6", "peer7", "peer8"}

	//populate streamhub with dummy handler
	for _, n := range peerNames {
		tstub.registerHandler(newStreamHandler(&dummyHandler{}), &PeerID{n})
	}

	testName1 := []string{"peer3", "peer4", "peer6", "peer8"}

	vset1 := newVSet(testName1)

	//work on all
	for p := range tstub.OverHandlers(context.Background(), toPeerId(testName1)) {

		vset1[p.Id.GetName()] = true
	}

	vset1.allPass(t)

	//cancel part
	counter := len(testName1) - 1

	ctx, cancel := context.WithCancel(context.Background())
	vset2 := newVSet(testName1)

	for p := range tstub.OverHandlers(ctx, toPeerId(testName1)) {

		vset2[p.Id.GetName()] = true
		counter--
		if counter == 0 {
			break
		}
	}

	cancel()

	failSet2 := vset2.excludeFail()
	if len(failSet2) != 1 {
		t.Fatal("Obtain unexpected fail set", failSet2)
	}

	vset2.allPass(t)

	//wrong peer

	testName3 := []string{"peer3", "peer4", "peer6", "wrongPeer"}

	vset3 := newVSet(testName3)

	for p := range tstub.OverHandlers(context.Background(), toPeerId(testName3)) {

		vset3[p.Id.GetName()] = true
	}

	failSet3 := vset3.excludeFail()
	if len(failSet3) != 1 && failSet3[0] != "wrongPeer" {
		t.Fatal("Obtain unexpected fail set", failSet3)
	}

	vset3.allPass(t)

	//on all
	vset4 := newVSet(peerNames)
	for p := range tstub.OverAllHandlers(context.Background()) {

		vset4[p.Id.GetName()] = true
	}

	vset4.allPass(t)

	//unregister one on "for on all"
	vset5 := newVSet(peerNames)
	counter = len(peerNames) / 2
	for p := range tstub.OverAllHandlers(context.Background()) {

		vset5[p.Id.GetName()] = true
		counter--
		if counter == 0 {
			notTouch := vset5.excludeFail()
			tstub.unRegisterHandler(&PeerID{Name: notTouch[0]})
		}
	}

	vset5.allPass(t)
	if len(vset5)+1 != len(peerNames) {
		t.Fatal("Unexpected vset", vset5)
	}

}

func Test_StreamHub_Broadcast(t *testing.T) {

	tstub := NewStreamStub(nil)

	peerNames := []string{"peer1", "peer2", "peer3", "peer4", "peer5", "peer6", "peer7", "peer8"}

	//populate streamhub with dummy handler
	for _, n := range peerNames {
		tstub.registerHandler(newStreamHandler(&dummyHandler{}), &PeerID{n})
	}

	err, ret := tstub.Broadcast(context.Background(), &PeerID{})

	if err != nil {
		t.Fatal("Broadcast fail", err)
	}

	if len(ret) != len(peerNames) {
		t.Fatal("Unexpected result set", ret)
	}

	for _, r := range ret {
		if r.WorkError != nil {
			t.Fatal("Unexpected result set in working error", r.WorkError, ret)
		}
	}
}
