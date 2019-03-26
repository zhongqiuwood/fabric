// Code generated by protoc-gen-go. DO NOT EDIT.
// source: api.proto

/*
Package protos is a generated protocol buffer package.

It is generated from these files:
	api.proto
	chaincode.proto
	chaincodeevent.proto
	devops.proto
	events.proto
	fabric.proto
	gossip.proto
	server_admin.proto
	sync.proto

It has these top-level messages:
	BlockNumber
	BlockCount
	ChaincodeID
	ChaincodeInput
	ChaincodeSpec
	ChaincodeDeploymentSpec
	ChaincodeInvocationSpec
	ChaincodeSecurityContext
	ChaincodeMessage
	PutStateInfo
	RangeQueryState
	RangeQueryStateNext
	RangeQueryStateClose
	RangeQueryStateKeyValue
	RangeQueryStateResponse
	ChaincodeEvent
	Secret
	SigmaInput
	ExecuteWithBinding
	SigmaOutput
	BuildResult
	TransactionRequest
	ChaincodeReg
	Interest
	Register
	Rejection
	Unregister
	Event
	GlobalState
	GlobalStateUpdateTask
	Transaction
	TransactionBlock
	TransactionResult
	Block
	BlockchainInfo
	NonHashData
	PeerAddress
	PeerID
	PeerEndpoint
	PeersMessage
	PeersAddresses
	HelloMessage
	Message
	Response
	GossipMsg
	HotTransactionBlock
	PeerTxState
	Gossip_Tx
	Gossip_TxState
	ServerStatus
	BlockState
	SyncBlockRange
	SyncBlocks
	SyncStateSnapshotRequest
	SyncStateSnapshot
	SyncStateDeltasRequest
	SyncStateDeltas
	SyncBlockState
	SyncMsg
	SyncStateQuery
	SyncStateResp
	SyncStartRequest
	SyncStartResponse
	UpdatedValue
	ChaincodeStateDelta
	SyncStateChunk
	SyncMessage
	SyncState
	SyncOffset
	BucketTreeOffset
	BlockOffset
	SyncMetadata
*/
package protos

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import google_protobuf1 "github.com/golang/protobuf/ptypes/empty"
import google_protobuf2 "github.com/golang/protobuf/ptypes/wrappers"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// Specifies the block number to be returned from the blockchain.
type BlockNumber struct {
	Number uint64 `protobuf:"varint,1,opt,name=number" json:"number,omitempty"`
}

func (m *BlockNumber) Reset()                    { *m = BlockNumber{} }
func (m *BlockNumber) String() string            { return proto.CompactTextString(m) }
func (*BlockNumber) ProtoMessage()               {}
func (*BlockNumber) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *BlockNumber) GetNumber() uint64 {
	if m != nil {
		return m.Number
	}
	return 0
}

// Specifies the current number of blocks in the blockchain.
type BlockCount struct {
	Count uint64 `protobuf:"varint,1,opt,name=count" json:"count,omitempty"`
}

func (m *BlockCount) Reset()                    { *m = BlockCount{} }
func (m *BlockCount) String() string            { return proto.CompactTextString(m) }
func (*BlockCount) ProtoMessage()               {}
func (*BlockCount) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *BlockCount) GetCount() uint64 {
	if m != nil {
		return m.Count
	}
	return 0
}

func init() {
	proto.RegisterType((*BlockNumber)(nil), "protos.BlockNumber")
	proto.RegisterType((*BlockCount)(nil), "protos.BlockCount")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for Openchain service

type OpenchainClient interface {
	// GetBlockchainInfo returns information about the blockchain ledger such as
	// height, current block hash, and previous block hash.
	GetBlockchainInfo(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*BlockchainInfo, error)
	// GetBlockByNumber returns the data contained within a specific block in the
	// blockchain. The genesis block is block zero.
	GetBlockByNumber(ctx context.Context, in *BlockNumber, opts ...grpc.CallOption) (*Block, error)
	// GetBlockCount returns the current number of blocks in the blockchain data
	// structure.
	GetBlockCount(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*BlockCount, error)
	// returns a transaction by its txid
	GetTransactionByID(ctx context.Context, in *google_protobuf2.StringValue, opts ...grpc.CallOption) (*Transaction, error)
	// GetPeers returns a list of all peer nodes currently connected to the target
	// peer.
	GetPeers(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*PeersMessage, error)
	// GetPeerEndpoint returns self peer node
	GetPeerEndpoint(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*PeersMessage, error)
}

type openchainClient struct {
	cc *grpc.ClientConn
}

func NewOpenchainClient(cc *grpc.ClientConn) OpenchainClient {
	return &openchainClient{cc}
}

func (c *openchainClient) GetBlockchainInfo(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*BlockchainInfo, error) {
	out := new(BlockchainInfo)
	err := grpc.Invoke(ctx, "/protos.Openchain/GetBlockchainInfo", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openchainClient) GetBlockByNumber(ctx context.Context, in *BlockNumber, opts ...grpc.CallOption) (*Block, error) {
	out := new(Block)
	err := grpc.Invoke(ctx, "/protos.Openchain/GetBlockByNumber", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openchainClient) GetBlockCount(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*BlockCount, error) {
	out := new(BlockCount)
	err := grpc.Invoke(ctx, "/protos.Openchain/GetBlockCount", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openchainClient) GetTransactionByID(ctx context.Context, in *google_protobuf2.StringValue, opts ...grpc.CallOption) (*Transaction, error) {
	out := new(Transaction)
	err := grpc.Invoke(ctx, "/protos.Openchain/GetTransactionByID", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openchainClient) GetPeers(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*PeersMessage, error) {
	out := new(PeersMessage)
	err := grpc.Invoke(ctx, "/protos.Openchain/GetPeers", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openchainClient) GetPeerEndpoint(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*PeersMessage, error) {
	out := new(PeersMessage)
	err := grpc.Invoke(ctx, "/protos.Openchain/GetPeerEndpoint", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for Openchain service

type OpenchainServer interface {
	// GetBlockchainInfo returns information about the blockchain ledger such as
	// height, current block hash, and previous block hash.
	GetBlockchainInfo(context.Context, *google_protobuf1.Empty) (*BlockchainInfo, error)
	// GetBlockByNumber returns the data contained within a specific block in the
	// blockchain. The genesis block is block zero.
	GetBlockByNumber(context.Context, *BlockNumber) (*Block, error)
	// GetBlockCount returns the current number of blocks in the blockchain data
	// structure.
	GetBlockCount(context.Context, *google_protobuf1.Empty) (*BlockCount, error)
	// returns a transaction by its txid
	GetTransactionByID(context.Context, *google_protobuf2.StringValue) (*Transaction, error)
	// GetPeers returns a list of all peer nodes currently connected to the target
	// peer.
	GetPeers(context.Context, *google_protobuf1.Empty) (*PeersMessage, error)
	// GetPeerEndpoint returns self peer node
	GetPeerEndpoint(context.Context, *google_protobuf1.Empty) (*PeersMessage, error)
}

func RegisterOpenchainServer(s *grpc.Server, srv OpenchainServer) {
	s.RegisterService(&_Openchain_serviceDesc, srv)
}

func _Openchain_GetBlockchainInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(google_protobuf1.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenchainServer).GetBlockchainInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Openchain/GetBlockchainInfo",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenchainServer).GetBlockchainInfo(ctx, req.(*google_protobuf1.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Openchain_GetBlockByNumber_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BlockNumber)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenchainServer).GetBlockByNumber(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Openchain/GetBlockByNumber",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenchainServer).GetBlockByNumber(ctx, req.(*BlockNumber))
	}
	return interceptor(ctx, in, info, handler)
}

func _Openchain_GetBlockCount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(google_protobuf1.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenchainServer).GetBlockCount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Openchain/GetBlockCount",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenchainServer).GetBlockCount(ctx, req.(*google_protobuf1.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Openchain_GetTransactionByID_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(google_protobuf2.StringValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenchainServer).GetTransactionByID(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Openchain/GetTransactionByID",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenchainServer).GetTransactionByID(ctx, req.(*google_protobuf2.StringValue))
	}
	return interceptor(ctx, in, info, handler)
}

func _Openchain_GetPeers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(google_protobuf1.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenchainServer).GetPeers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Openchain/GetPeers",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenchainServer).GetPeers(ctx, req.(*google_protobuf1.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Openchain_GetPeerEndpoint_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(google_protobuf1.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenchainServer).GetPeerEndpoint(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Openchain/GetPeerEndpoint",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenchainServer).GetPeerEndpoint(ctx, req.(*google_protobuf1.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

var _Openchain_serviceDesc = grpc.ServiceDesc{
	ServiceName: "protos.Openchain",
	HandlerType: (*OpenchainServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetBlockchainInfo",
			Handler:    _Openchain_GetBlockchainInfo_Handler,
		},
		{
			MethodName: "GetBlockByNumber",
			Handler:    _Openchain_GetBlockByNumber_Handler,
		},
		{
			MethodName: "GetBlockCount",
			Handler:    _Openchain_GetBlockCount_Handler,
		},
		{
			MethodName: "GetTransactionByID",
			Handler:    _Openchain_GetTransactionByID_Handler,
		},
		{
			MethodName: "GetPeers",
			Handler:    _Openchain_GetPeers_Handler,
		},
		{
			MethodName: "GetPeerEndpoint",
			Handler:    _Openchain_GetPeerEndpoint_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api.proto",
}

func init() { proto.RegisterFile("api.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 308 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x92, 0x4f, 0x4b, 0x03, 0x31,
	0x10, 0xc5, 0xb7, 0xa8, 0xc5, 0x8e, 0x16, 0x75, 0x2c, 0x45, 0x56, 0x11, 0x09, 0x08, 0x9e, 0xb6,
	0xa0, 0x17, 0x11, 0x3c, 0x58, 0x2d, 0xa5, 0x07, 0xff, 0xa0, 0xe2, 0x3d, 0xbb, 0x4e, 0xd7, 0x60,
	0x9b, 0x84, 0x24, 0x8b, 0xf4, 0x83, 0xf8, 0x7d, 0xa5, 0xc9, 0xae, 0xb8, 0xca, 0x7a, 0xca, 0xcc,
	0x9b, 0xf7, 0x26, 0xbf, 0x40, 0xa0, 0xc3, 0xb5, 0x48, 0xb4, 0x51, 0x4e, 0x61, 0xdb, 0x1f, 0x36,
	0xde, 0x9c, 0xf2, 0xd4, 0x88, 0x2c, 0xa8, 0xf1, 0x7e, 0xae, 0x54, 0x3e, 0xa3, 0x81, 0xef, 0xd2,
	0x62, 0x3a, 0xa0, 0xb9, 0x76, 0x8b, 0x72, 0x78, 0xf8, 0x7b, 0xf8, 0x61, 0xb8, 0xd6, 0x64, 0x6c,
	0x98, 0xb3, 0x63, 0xd8, 0x18, 0xce, 0x54, 0xf6, 0x7e, 0x57, 0xcc, 0x53, 0x32, 0xd8, 0x87, 0xb6,
	0xf4, 0xd5, 0x5e, 0xeb, 0xa8, 0x75, 0xb2, 0xfa, 0x58, 0x76, 0x8c, 0x01, 0x78, 0xdb, 0xb5, 0x2a,
	0xa4, 0xc3, 0x1e, 0xac, 0x65, 0xcb, 0xa2, 0x34, 0x85, 0xe6, 0xf4, 0x73, 0x05, 0x3a, 0xf7, 0x9a,
	0x64, 0xf6, 0xc6, 0x85, 0xc4, 0x11, 0xec, 0x8c, 0xc9, 0xf9, 0x90, 0x17, 0x26, 0x72, 0xaa, 0xb0,
	0x9f, 0x04, 0x9c, 0xa4, 0xc2, 0x49, 0x46, 0x4b, 0xd6, 0xb8, 0x1f, 0x04, 0x9b, 0xd4, 0xfd, 0x2c,
	0xc2, 0x73, 0xd8, 0xae, 0xd6, 0x0c, 0x17, 0x25, 0xe4, 0x6e, 0xcd, 0x1d, 0xc4, 0xb8, 0x5b, 0x13,
	0x59, 0x84, 0x97, 0xd0, 0xad, 0x92, 0x81, 0xba, 0xe9, 0x72, 0xac, 0x25, 0xbd, 0x97, 0x45, 0x38,
	0x01, 0x1c, 0x93, 0x7b, 0x36, 0x5c, 0x5a, 0x9e, 0x39, 0xa1, 0xe4, 0x70, 0x31, 0xb9, 0xc1, 0x83,
	0x3f, 0x3b, 0x9e, 0x9c, 0x11, 0x32, 0x7f, 0xe1, 0xb3, 0x82, 0xe2, 0x6f, 0xb0, 0x1f, 0x31, 0x16,
	0xe1, 0x05, 0xac, 0x8f, 0xc9, 0x3d, 0x10, 0x19, 0xdb, 0x08, 0xd1, 0xab, 0xa2, 0xde, 0x76, 0x4b,
	0xd6, 0xf2, 0x9c, 0x58, 0x84, 0x57, 0xb0, 0x55, 0x66, 0x47, 0xf2, 0x55, 0x2b, 0xf1, 0xcf, 0x3b,
	0x1a, 0x56, 0xa4, 0xe1, 0xd7, 0x9c, 0x7d, 0x05, 0x00, 0x00, 0xff, 0xff, 0x6b, 0xdb, 0xfb, 0x6c,
	0x49, 0x02, 0x00, 0x00,
}
