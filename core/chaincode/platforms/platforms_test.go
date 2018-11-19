package platforms

import (
	"bytes"
	pb "github.com/abchain/fabric/protos"
	"strings"
	"testing"
)

func TestWritePackage(t *testing.T) {

	t.Skip("This is a temporary test only for deep inspecting purpose")

	url := "github.com/abchain/fabric/examples/chaincode/go/chaincode_example01"
	spec := &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_GOLANG, ChaincodeID: &pb.ChaincodeID{Path: url}, CtorMsg: &pb.ChaincodeInput{Args: [][]byte{[]byte("init")}}}

	buf := new(bytes.Buffer)
	h, err := WritePackage(spec, buf)
	if err != nil {
		t.Fatal("write package fail:", err)
	}

	if strings.Compare(h, "80e3ac242a65d9fa916c74deb337890ead23bbc041c28fdf50ef8ef7a6c1b76d03a815dda969a1de54aca206fe2fe3bdb677006e633d90aa52ea2eed0a0a4e27") != 0 {
		t.Fatal("hash is not expected:", h)
	}
}
