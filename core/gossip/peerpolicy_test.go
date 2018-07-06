package gossip_test

import (
	"github.com/abchain/fabric/core/gossip"
	"testing"
	"time"
)

func TestTokenBucket(t *testing.T) {

	bk := gossip.NewTokenBucket(2)
	bkBoom := gossip.NewTokenBucket(3)

	if bk.TestIn(10, 10) {
		t.Fatalf("bucket fail when in just limit: %v", bk)
	}

	if bkBoom.TestIn(10, 10) {
		t.Fatalf("bucket 2 fail when in just limit: %v", bk)
	}

	time.Sleep(time.Second)

	if bk.TestIn(2, 10) {
		t.Fatalf("bucket fail when in 2 after one sec: %v", bk)
	}

	if !bkBoom.TestIn(5, 10) {
		t.Fatalf("bucket not exceed when in 5 after one sec: %v", bk)
	}

	if bkBoom.Available(10) >= 0 {
		t.Fatalf("bucket exceed not have minus value: %v", bk)
	}

	if bk.Available(10) < 3 {
		t.Fatalf("bucket fail with expected availiabe less than 3: %v", bk)
	}

	time.Sleep(time.Second)

	if bkBoom.TestIn(0, 10) {
		t.Fatalf("bucket exceeded not resume after another one sec: %v", bk)
	}

}

func TestPeerPolicyRecv(t *testing.T) {

	cpo := gossip.NewPeerPolicy("test")

	cpo.RecvUpdate(1024 * 1024 * 8)

	if !cpo.AllowRecvUpdate() {
		t.Fatalf("cpo do not allow more bytes: %v", cpo)
	}
}
