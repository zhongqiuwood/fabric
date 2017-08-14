package peerex

import (
	"strings"
    "testing"
)

func TestDecoding(t *testing.T) {
	
	
	ret, err := DecodeChaincodeName("Eg9nYW1lcGFpY29yZV92MDE=")
	
	if err != nil{
		t.Fatal(err)
	}
	
	if strings.Compare("gamepaicore_v01", ret) != 0{
		t.Fatal("wrong chaincodename:", ret)
	}
	
	invoke, err := DecodeTransactionToInvoke("CpQDCAESERIPZ2FtZXBhaWNvcmVfdjAxGt4CCglVU0VSX0ZVTkQKtAFDaUpCV2pKT04zcFVSRFpvVEhwaFJWTXpVbFJVYW00MU9FOUhOMk5JZDBkTGJYUm5FbUhtczZqbWhJL3Z2SnJvcjdma3VJM29wb0hvdjU3bnU2MHk1cXloNW9pVzVhU2E1cXloNVpDUjU1dTQ1WkNNNVp5dzVaMkE1WStSNllDQjVaQ001cUMzNXBXdzZZZVA1WktNNVpDTTVxQzM1YVNINXJPbzU1cUU2TDJzNkxTbTQ0Q0MKOENpWUlDUklpUVZWWFVqQmFYekZsTjBsek5EUjRhVk40TjBRNWJuazVkVWwwY0RWT1UyaGpRUT09CmBDa1FLSURXM012RUVQbllYdWhIazZlVzFEVHRVYVh1MEtvWTBSNzQ0Mkhwa0laSVpFaUE2bzdwYzZLS0I5MTBoREZFZ2pRUVo0NUxRa2YwTDVBcWVFZWsyY2czQWVRPT1CDFBhaUFkbWluUm9sZUIOUGFpQWRtaW5SZWdpb24=")
	
	if err != nil{
		t.Fatal(err)
	}

	fund := string(invoke.ChaincodeSpec.CtorMsg.Args[0])

	if strings.Compare(fund, "USER_FUND") != 0{
		t.Fatal("wrong function:", fund)		
	}

	t.Log(invoke)

	invoke, err = DecodeTransactionToInvoke("")
	
	if err == nil{
		t.Fatal("not recognized empty payload")
	}
}

