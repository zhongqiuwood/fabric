package utils

import (
	"encoding/asn1"
	"testing"
)

func TestObjIdIndexing(t *testing.T) {

	ind := new(ObjIdIndex)

	ind.AddItem("test1", asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6, 7, 8})
	ind.AddItem("test2", asn1.ObjectIdentifier{5})

	v, matched, resd := ind.GetItem(asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6, 7, 8})
	if vs, ok := v.(string); !ok || vs != "test1" {
		t.Fatal("find 1 v fail", v, matched, resd)
	}

	if !matched || len(resd) != 8 {
		t.Fatal("data 1 fail", v, matched, resd)
	}

	v, matched, resd = ind.GetItem(asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6})
	if v != nil {
		t.Fatal("find 2 v fail", v, matched, resd)
	}

	if !matched || len(resd) != 6 {
		t.Fatal("data 2 fail", v, matched, resd)
	}

	ind.AddItem("test3", asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6})
	v, _, _ = ind.GetItem(asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6})

	if vs, ok := v.(string); !ok || vs != "test3" {
		t.Fatal("find 3 v fail", v)
	}

	_, matched, resd = ind.GetItem(asn1.ObjectIdentifier{1, 2, 3, 4, 6, 6, 9})

	if matched || len(resd) != 4 {
		t.Fatal("data 3 fail", matched, resd)
	}

	v, matched, resd = ind.GetItem(asn1.ObjectIdentifier{5})
	if vs, ok := v.(string); !ok || vs != "test2" {
		t.Fatal("find 4 v fail", v)
	}

	if !matched || len(resd) != 1 {
		t.Fatal("data 4 fail", v, matched, resd)
	}

	_, matched, resd = ind.GetItem(asn1.ObjectIdentifier{9, 8, 7, 6, 5})

	if matched || len(resd) != 0 {
		t.Fatal("data 5 fail", matched, resd)
	}

	v, matched, resd = ind.GetItem(asn1.ObjectIdentifier{5, 9, 2, 6})

	if vs, ok := v.(string); !ok || vs != "test2" {
		t.Fatal("find 6 v fail", v)
	}

	if matched || len(resd) != 1 {
		t.Fatal("data 6 fail", matched, resd)
	}

}
