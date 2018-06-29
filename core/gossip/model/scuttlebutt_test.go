package gossip_model_test

import (
	_ "fmt"
	model "github.com/abchain/fabric/core/gossip/model"
	"testing"
)

//m1 pull from m2
func pull(m1 *model.Model, m2 *model.Model) (error, model.Update) {

	u := m2.RecvPullDigest(m1.GenDigest())
	return m1.RecvUpdate(u), u
}

func comparePeer(d1 map[string]int, d2 map[string]int, t *testing.T) {

	for k, v := range d1 {
		vv, ok := d2[k]
		if !ok || vv != v {
			t.Fatalf("data compare fail at key %s [%v : %v]", k, d1, d2)
		}
	}

	for k, v := range d2 {
		vv, ok := d1[k]
		if !ok || vv != v {
			t.Fatalf("data compare fail at key %s [%v : %v]", k, d1, d2)
		}
	}
}

func TestScuttlebuttModelBase(t *testing.T) {

	p1 := model.NewTestPeer(t, "alice")
	p2 := model.NewTestPeer(t, "bob")

	m1 := p1.CreateModel()
	m2 := p2.CreateModel()

	//known each other
	if err, _ := pull(m2, m1); err != nil {
		t.Fatal("m2 known m1 fail", err)
	}

	if err, _ := pull(m1, m2); err != nil {
		t.Fatal("m1 known m2 fail", err)
	}

	p1.LocalUpdate([]string{"a1", "a2", "a3"})

	err, ud := pull(m2, m1)
	t.Log("dump update 1", model.DumpUpdate(ud))
	if err != nil {
		t.Fatal("pull 1 fail", err)
	}

	p2.LocalUpdate([]string{"b1", "b2"})

	err, ud = pull(m1, m2)
	t.Log("dump update 2", model.DumpUpdate(ud))
	if err != nil {
		t.Fatal("pull 2 fail", err)
	}

	p1.LocalUpdate([]string{"a2", "a3"})

	err, ud = pull(m2, m1)
	t.Log("dump update 3", model.DumpUpdate(ud))
	if err != nil {
		t.Fatal("pull 3 fail", err)
	}

	p2.LocalUpdate([]string{"b2"})

	err, ud = pull(m1, m2)
	t.Log("dump update 4", model.DumpUpdate(ud))
	if err != nil {
		t.Fatal("pull 4 fail", err)
	}

	comparePeer(p1.DumpData(), p2.DumpData(), t)

	_, ok := p1.DumpData()["b1"]
	if !ok {
		t.Fatal("p1 not know key b1:", p1.DumpData())
	}

	_, ok = p2.DumpData()["a1"]
	if !ok {
		t.Fatal("p2 not know key a1:", p2.DumpData())
	}

	va3 := p2.DumpData()["a3"]

	p2.LocalUpdate([]string{"a3", "b3"})
	err, ud = pull(m1, m2)
	t.Log("dump update 5", model.DumpUpdate(ud))
	if err != nil {
		t.Fatal("pull 5 fail", err)
	}

	comparePeer(p1.DumpData(), p2.DumpData(), t)

	if p1.DumpData()["a3"] <= va3 {
		t.Fatal("p1 still know old a3:", p1.DumpData())
	}
}
