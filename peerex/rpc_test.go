package peerex

import (
    "testing"
    "strings"
)

func TestMakeStrings(t *testing.T) {
	
	funcname1 := "testA"
	args1 := []string{"A","100","B","200"}
	
	ret1 := makeStringArgsToPb(funcname1, args1)
	
	t.Log(ret1.Args)
	
	if len(ret1.Args) != 5{
		t.Fatal("Len not match")
	}
	
	if strings.Compare(string(ret1.Args[0]), funcname1) != 0{
		t.Fatal("function name not match")
	}
	
	ret2 := makeStringArgsToPb("", args1)
	
	t.Log(ret2.Args)
	
	if len(ret2.Args) != 4{
		t.Fatal("Len not match")
	}
	
	if strings.Compare(string(ret2.Args[0]), args1[0]) != 0{
		t.Fatal("function name not match")
	}	
	
}

