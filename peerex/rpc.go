package peerex

import (
	pb "github.com/hyperledger/fabric/protos"
	"golang.org/x/net/context"
)

type rpcManager struct{
	ctx 	context.Context
	cancel  context.CancelFunc
}

type Rpc struct{
	
}

func (_ *Rpc) NewManager() *rpcManager{
	
	ctx, cancel := context.WithCancel(context.Background())
	
	return &rpcManager{ctx, cancel}
}

type RpcBuilder struct{
	ChaincodeName	 string
//	ChaincodeLang    string
	Function		 string
	
	Security		 *SecurityPolicy
	
	Conn			 ClientConn	
	ConnManager		 *rpcManager	
}

type SecurityPolicy struct{
	User			string
	Attributes		[]string
	Metadata		[]byte
	CustomIDGenAlg  string
}


var defaultSecPolicy = &SecurityPolicy{Attributes: []string{}}

func makeStringArgsToPb(funcname string, args []string) *pb.ChaincodeInput{
	
	input := &pb.ChaincodeInput{}
	//please remember the trick fabric used:
	//it push the "function name" as the first argument
	//in a rpc call
	var inarg [][]byte
	if len(funcname) == 0{
		input.Args = make([][]byte, len(args))	
		inarg = input.Args[:]
	}else{
		input.Args = make([][]byte, len(args) + 1)
		input.Args[0] = []byte(funcname)
		inarg = input.Args[1:]
	}
	
	for i, arg := range args{
		inarg[i] = []byte(arg)
	}
	
	return input
}

func (b *RpcBuilder) prepare(args []string) *pb.ChaincodeInvocationSpec{
	spec := &pb.ChaincodeSpec{
		Type: pb.ChaincodeSpec_GOLANG,	//always set it as golang
		ChaincodeID: &pb.ChaincodeID{Name: b.ChaincodeName},
		CtorMsg : makeStringArgsToPb(b.Function, args),
	}
	
	invocation := &pb.ChaincodeInvocationSpec{ChaincodeSpec: spec}
	
	if b.Security != nil{
		spec.Attributes = b.Security.Attributes
		if len(b.Security.CustomIDGenAlg) != 0{
			invocation.IdGenerationAlg = b.Security.CustomIDGenAlg
		}
	}
	
	//final check attributes
	if spec.Attributes == nil{
		spec.Attributes = defaultSecPolicy.Attributes
	}
	
	return invocation	
}

func (b *RpcBuilder) context() context.Context{
		
	if b.ConnManager != nil{
		ctx, _ := context.WithCancel(b.ConnManager.ctx)
		return ctx
	}else{
		ctx, _ := context.WithCancel(context.Background())
		return ctx
	}
}

func (b *RpcBuilder) Fire(args []string) (string, error){	
	
	resp, err := pb.NewDevopsClient(b.Conn.C).Invoke(b.context(), b.prepare(args))
	
	if err != nil{
		return "", err
	}
	
	return string(resp.Msg), nil
}

func (b *RpcBuilder) Query(args []string) ([]byte, error){	
	
	resp, err := pb.NewDevopsClient(b.Conn.C).Query(b.context(), b.prepare(args))
	
	if err != nil{
		return nil, err
	}
	
	return resp.Msg, nil
}
