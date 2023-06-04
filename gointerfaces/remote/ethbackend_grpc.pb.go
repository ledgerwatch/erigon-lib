// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.22.1
// source: remote/ethbackend.proto

package remote

import (
	context "context"
	types "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	ETHBACKEND_Etherbase_FullMethodName                       = "/remote.ETHBACKEND/Etherbase"
	ETHBACKEND_NetVersion_FullMethodName                      = "/remote.ETHBACKEND/NetVersion"
	ETHBACKEND_NetPeerCount_FullMethodName                    = "/remote.ETHBACKEND/NetPeerCount"
	ETHBACKEND_EngineNewPayload_FullMethodName                = "/remote.ETHBACKEND/EngineNewPayload"
	ETHBACKEND_EngineForkChoiceUpdated_FullMethodName         = "/remote.ETHBACKEND/EngineForkChoiceUpdated"
	ETHBACKEND_EngineGetPayload_FullMethodName                = "/remote.ETHBACKEND/EngineGetPayload"
	ETHBACKEND_EngineGetPayloadBodiesByHashV1_FullMethodName  = "/remote.ETHBACKEND/EngineGetPayloadBodiesByHashV1"
	ETHBACKEND_EngineGetPayloadBodiesByRangeV1_FullMethodName = "/remote.ETHBACKEND/EngineGetPayloadBodiesByRangeV1"
	ETHBACKEND_Version_FullMethodName                         = "/remote.ETHBACKEND/Version"
	ETHBACKEND_ProtocolVersion_FullMethodName                 = "/remote.ETHBACKEND/ProtocolVersion"
	ETHBACKEND_ClientVersion_FullMethodName                   = "/remote.ETHBACKEND/ClientVersion"
	ETHBACKEND_Subscribe_FullMethodName                       = "/remote.ETHBACKEND/Subscribe"
	ETHBACKEND_SubscribeLogs_FullMethodName                   = "/remote.ETHBACKEND/SubscribeLogs"
	ETHBACKEND_Block_FullMethodName                           = "/remote.ETHBACKEND/Block"
	ETHBACKEND_TxnLookup_FullMethodName                       = "/remote.ETHBACKEND/TxnLookup"
	ETHBACKEND_NodeInfo_FullMethodName                        = "/remote.ETHBACKEND/NodeInfo"
	ETHBACKEND_Peers_FullMethodName                           = "/remote.ETHBACKEND/Peers"
	ETHBACKEND_PendingBlock_FullMethodName                    = "/remote.ETHBACKEND/PendingBlock"
)

// ETHBACKENDClient is the client API for ETHBACKEND service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ETHBACKENDClient interface {
	Etherbase(ctx context.Context, in *EtherbaseRequest, opts ...grpc.CallOption) (*EtherbaseReply, error)
	NetVersion(ctx context.Context, in *NetVersionRequest, opts ...grpc.CallOption) (*NetVersionReply, error)
	NetPeerCount(ctx context.Context, in *NetPeerCountRequest, opts ...grpc.CallOption) (*NetPeerCountReply, error)
	// Validate and possibly execute the payload.
	EngineNewPayload(ctx context.Context, in *types.ExecutionPayload, opts ...grpc.CallOption) (*EnginePayloadStatus, error)
	// Update fork choice
	EngineForkChoiceUpdated(ctx context.Context, in *EngineForkChoiceUpdatedRequest, opts ...grpc.CallOption) (*EngineForkChoiceUpdatedResponse, error)
	// Fetch the payload along with its blobs by ID.
	EngineGetPayload(ctx context.Context, in *EngineGetPayloadRequest, opts ...grpc.CallOption) (*EngineGetPayloadResponse, error)
	EngineGetPayloadBodiesByHashV1(ctx context.Context, in *EngineGetPayloadBodiesByHashV1Request, opts ...grpc.CallOption) (*EngineGetPayloadBodiesV1Response, error)
	EngineGetPayloadBodiesByRangeV1(ctx context.Context, in *EngineGetPayloadBodiesByRangeV1Request, opts ...grpc.CallOption) (*EngineGetPayloadBodiesV1Response, error)
	// Version returns the service version number
	Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.VersionReply, error)
	// ProtocolVersion returns the Ethereum protocol version number (e.g. 66 for ETH66).
	ProtocolVersion(ctx context.Context, in *ProtocolVersionRequest, opts ...grpc.CallOption) (*ProtocolVersionReply, error)
	// ClientVersion returns the Ethereum client version string using node name convention (e.g. TurboGeth/v2021.03.2-alpha/Linux).
	ClientVersion(ctx context.Context, in *ClientVersionRequest, opts ...grpc.CallOption) (*ClientVersionReply, error)
	Subscribe(ctx context.Context, in *SubscribeRequest, opts ...grpc.CallOption) (ETHBACKEND_SubscribeClient, error)
	// Only one subscription is needed to serve all the users, LogsFilterRequest allows to dynamically modifying the subscription
	SubscribeLogs(ctx context.Context, opts ...grpc.CallOption) (ETHBACKEND_SubscribeLogsClient, error)
	// High-level method - can read block from db, snapshots or apply any other logic
	// it doesn't provide consistency
	// Request fields are optional - it's ok to request block only by hash or only by number
	Block(ctx context.Context, in *BlockRequest, opts ...grpc.CallOption) (*BlockReply, error)
	// High-level method - can find block number by txn hash
	// it doesn't provide consistency
	TxnLookup(ctx context.Context, in *TxnLookupRequest, opts ...grpc.CallOption) (*TxnLookupReply, error)
	// NodeInfo collects and returns NodeInfo from all running sentry instances.
	NodeInfo(ctx context.Context, in *NodesInfoRequest, opts ...grpc.CallOption) (*NodesInfoReply, error)
	// Peers collects and returns peers information from all running sentry instances.
	Peers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*PeersReply, error)
	PendingBlock(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*PendingBlockReply, error)
}

type eTHBACKENDClient struct {
	cc grpc.ClientConnInterface
}

func NewETHBACKENDClient(cc grpc.ClientConnInterface) ETHBACKENDClient {
	return &eTHBACKENDClient{cc}
}

func (c *eTHBACKENDClient) Etherbase(ctx context.Context, in *EtherbaseRequest, opts ...grpc.CallOption) (*EtherbaseReply, error) {
	out := new(EtherbaseReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_Etherbase_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) NetVersion(ctx context.Context, in *NetVersionRequest, opts ...grpc.CallOption) (*NetVersionReply, error) {
	out := new(NetVersionReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_NetVersion_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) NetPeerCount(ctx context.Context, in *NetPeerCountRequest, opts ...grpc.CallOption) (*NetPeerCountReply, error) {
	out := new(NetPeerCountReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_NetPeerCount_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) EngineNewPayload(ctx context.Context, in *types.ExecutionPayload, opts ...grpc.CallOption) (*EnginePayloadStatus, error) {
	out := new(EnginePayloadStatus)
	err := c.cc.Invoke(ctx, ETHBACKEND_EngineNewPayload_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) EngineForkChoiceUpdated(ctx context.Context, in *EngineForkChoiceUpdatedRequest, opts ...grpc.CallOption) (*EngineForkChoiceUpdatedResponse, error) {
	out := new(EngineForkChoiceUpdatedResponse)
	err := c.cc.Invoke(ctx, ETHBACKEND_EngineForkChoiceUpdated_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) EngineGetPayload(ctx context.Context, in *EngineGetPayloadRequest, opts ...grpc.CallOption) (*EngineGetPayloadResponse, error) {
	out := new(EngineGetPayloadResponse)
	err := c.cc.Invoke(ctx, ETHBACKEND_EngineGetPayload_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) EngineGetPayloadBodiesByHashV1(ctx context.Context, in *EngineGetPayloadBodiesByHashV1Request, opts ...grpc.CallOption) (*EngineGetPayloadBodiesV1Response, error) {
	out := new(EngineGetPayloadBodiesV1Response)
	err := c.cc.Invoke(ctx, ETHBACKEND_EngineGetPayloadBodiesByHashV1_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) EngineGetPayloadBodiesByRangeV1(ctx context.Context, in *EngineGetPayloadBodiesByRangeV1Request, opts ...grpc.CallOption) (*EngineGetPayloadBodiesV1Response, error) {
	out := new(EngineGetPayloadBodiesV1Response)
	err := c.cc.Invoke(ctx, ETHBACKEND_EngineGetPayloadBodiesByRangeV1_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.VersionReply, error) {
	out := new(types.VersionReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_Version_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) ProtocolVersion(ctx context.Context, in *ProtocolVersionRequest, opts ...grpc.CallOption) (*ProtocolVersionReply, error) {
	out := new(ProtocolVersionReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_ProtocolVersion_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) ClientVersion(ctx context.Context, in *ClientVersionRequest, opts ...grpc.CallOption) (*ClientVersionReply, error) {
	out := new(ClientVersionReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_ClientVersion_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) Subscribe(ctx context.Context, in *SubscribeRequest, opts ...grpc.CallOption) (ETHBACKEND_SubscribeClient, error) {
	stream, err := c.cc.NewStream(ctx, &ETHBACKEND_ServiceDesc.Streams[0], ETHBACKEND_Subscribe_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &eTHBACKENDSubscribeClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type ETHBACKEND_SubscribeClient interface {
	Recv() (*SubscribeReply, error)
	grpc.ClientStream
}

type eTHBACKENDSubscribeClient struct {
	grpc.ClientStream
}

func (x *eTHBACKENDSubscribeClient) Recv() (*SubscribeReply, error) {
	m := new(SubscribeReply)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *eTHBACKENDClient) SubscribeLogs(ctx context.Context, opts ...grpc.CallOption) (ETHBACKEND_SubscribeLogsClient, error) {
	stream, err := c.cc.NewStream(ctx, &ETHBACKEND_ServiceDesc.Streams[1], ETHBACKEND_SubscribeLogs_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &eTHBACKENDSubscribeLogsClient{stream}
	return x, nil
}

type ETHBACKEND_SubscribeLogsClient interface {
	Send(*LogsFilterRequest) error
	Recv() (*SubscribeLogsReply, error)
	grpc.ClientStream
}

type eTHBACKENDSubscribeLogsClient struct {
	grpc.ClientStream
}

func (x *eTHBACKENDSubscribeLogsClient) Send(m *LogsFilterRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *eTHBACKENDSubscribeLogsClient) Recv() (*SubscribeLogsReply, error) {
	m := new(SubscribeLogsReply)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *eTHBACKENDClient) Block(ctx context.Context, in *BlockRequest, opts ...grpc.CallOption) (*BlockReply, error) {
	out := new(BlockReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_Block_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) TxnLookup(ctx context.Context, in *TxnLookupRequest, opts ...grpc.CallOption) (*TxnLookupReply, error) {
	out := new(TxnLookupReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_TxnLookup_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) NodeInfo(ctx context.Context, in *NodesInfoRequest, opts ...grpc.CallOption) (*NodesInfoReply, error) {
	out := new(NodesInfoReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_NodeInfo_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) Peers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*PeersReply, error) {
	out := new(PeersReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_Peers_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eTHBACKENDClient) PendingBlock(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*PendingBlockReply, error) {
	out := new(PendingBlockReply)
	err := c.cc.Invoke(ctx, ETHBACKEND_PendingBlock_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ETHBACKENDServer is the server API for ETHBACKEND service.
// All implementations must embed UnimplementedETHBACKENDServer
// for forward compatibility
type ETHBACKENDServer interface {
	Etherbase(context.Context, *EtherbaseRequest) (*EtherbaseReply, error)
	NetVersion(context.Context, *NetVersionRequest) (*NetVersionReply, error)
	NetPeerCount(context.Context, *NetPeerCountRequest) (*NetPeerCountReply, error)
	// Validate and possibly execute the payload.
	EngineNewPayload(context.Context, *types.ExecutionPayload) (*EnginePayloadStatus, error)
	// Update fork choice
	EngineForkChoiceUpdated(context.Context, *EngineForkChoiceUpdatedRequest) (*EngineForkChoiceUpdatedResponse, error)
	// Fetch the payload along with its blobs by ID.
	EngineGetPayload(context.Context, *EngineGetPayloadRequest) (*EngineGetPayloadResponse, error)
	EngineGetPayloadBodiesByHashV1(context.Context, *EngineGetPayloadBodiesByHashV1Request) (*EngineGetPayloadBodiesV1Response, error)
	EngineGetPayloadBodiesByRangeV1(context.Context, *EngineGetPayloadBodiesByRangeV1Request) (*EngineGetPayloadBodiesV1Response, error)
	// Version returns the service version number
	Version(context.Context, *emptypb.Empty) (*types.VersionReply, error)
	// ProtocolVersion returns the Ethereum protocol version number (e.g. 66 for ETH66).
	ProtocolVersion(context.Context, *ProtocolVersionRequest) (*ProtocolVersionReply, error)
	// ClientVersion returns the Ethereum client version string using node name convention (e.g. TurboGeth/v2021.03.2-alpha/Linux).
	ClientVersion(context.Context, *ClientVersionRequest) (*ClientVersionReply, error)
	Subscribe(*SubscribeRequest, ETHBACKEND_SubscribeServer) error
	// Only one subscription is needed to serve all the users, LogsFilterRequest allows to dynamically modifying the subscription
	SubscribeLogs(ETHBACKEND_SubscribeLogsServer) error
	// High-level method - can read block from db, snapshots or apply any other logic
	// it doesn't provide consistency
	// Request fields are optional - it's ok to request block only by hash or only by number
	Block(context.Context, *BlockRequest) (*BlockReply, error)
	// High-level method - can find block number by txn hash
	// it doesn't provide consistency
	TxnLookup(context.Context, *TxnLookupRequest) (*TxnLookupReply, error)
	// NodeInfo collects and returns NodeInfo from all running sentry instances.
	NodeInfo(context.Context, *NodesInfoRequest) (*NodesInfoReply, error)
	// Peers collects and returns peers information from all running sentry instances.
	Peers(context.Context, *emptypb.Empty) (*PeersReply, error)
	PendingBlock(context.Context, *emptypb.Empty) (*PendingBlockReply, error)
	mustEmbedUnimplementedETHBACKENDServer()
}

// UnimplementedETHBACKENDServer must be embedded to have forward compatible implementations.
type UnimplementedETHBACKENDServer struct {
}

func (UnimplementedETHBACKENDServer) Etherbase(context.Context, *EtherbaseRequest) (*EtherbaseReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Etherbase not implemented")
}
func (UnimplementedETHBACKENDServer) NetVersion(context.Context, *NetVersionRequest) (*NetVersionReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NetVersion not implemented")
}
func (UnimplementedETHBACKENDServer) NetPeerCount(context.Context, *NetPeerCountRequest) (*NetPeerCountReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NetPeerCount not implemented")
}
func (UnimplementedETHBACKENDServer) EngineNewPayload(context.Context, *types.ExecutionPayload) (*EnginePayloadStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineNewPayload not implemented")
}
func (UnimplementedETHBACKENDServer) EngineForkChoiceUpdated(context.Context, *EngineForkChoiceUpdatedRequest) (*EngineForkChoiceUpdatedResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineForkChoiceUpdated not implemented")
}
func (UnimplementedETHBACKENDServer) EngineGetPayload(context.Context, *EngineGetPayloadRequest) (*EngineGetPayloadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineGetPayload not implemented")
}
func (UnimplementedETHBACKENDServer) EngineGetPayloadBodiesByHashV1(context.Context, *EngineGetPayloadBodiesByHashV1Request) (*EngineGetPayloadBodiesV1Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineGetPayloadBodiesByHashV1 not implemented")
}
func (UnimplementedETHBACKENDServer) EngineGetPayloadBodiesByRangeV1(context.Context, *EngineGetPayloadBodiesByRangeV1Request) (*EngineGetPayloadBodiesV1Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineGetPayloadBodiesByRangeV1 not implemented")
}
func (UnimplementedETHBACKENDServer) Version(context.Context, *emptypb.Empty) (*types.VersionReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Version not implemented")
}
func (UnimplementedETHBACKENDServer) ProtocolVersion(context.Context, *ProtocolVersionRequest) (*ProtocolVersionReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ProtocolVersion not implemented")
}
func (UnimplementedETHBACKENDServer) ClientVersion(context.Context, *ClientVersionRequest) (*ClientVersionReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ClientVersion not implemented")
}
func (UnimplementedETHBACKENDServer) Subscribe(*SubscribeRequest, ETHBACKEND_SubscribeServer) error {
	return status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}
func (UnimplementedETHBACKENDServer) SubscribeLogs(ETHBACKEND_SubscribeLogsServer) error {
	return status.Errorf(codes.Unimplemented, "method SubscribeLogs not implemented")
}
func (UnimplementedETHBACKENDServer) Block(context.Context, *BlockRequest) (*BlockReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Block not implemented")
}
func (UnimplementedETHBACKENDServer) TxnLookup(context.Context, *TxnLookupRequest) (*TxnLookupReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TxnLookup not implemented")
}
func (UnimplementedETHBACKENDServer) NodeInfo(context.Context, *NodesInfoRequest) (*NodesInfoReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NodeInfo not implemented")
}
func (UnimplementedETHBACKENDServer) Peers(context.Context, *emptypb.Empty) (*PeersReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Peers not implemented")
}
func (UnimplementedETHBACKENDServer) PendingBlock(context.Context, *emptypb.Empty) (*PendingBlockReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PendingBlock not implemented")
}
func (UnimplementedETHBACKENDServer) mustEmbedUnimplementedETHBACKENDServer() {}

// UnsafeETHBACKENDServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ETHBACKENDServer will
// result in compilation errors.
type UnsafeETHBACKENDServer interface {
	mustEmbedUnimplementedETHBACKENDServer()
}

func RegisterETHBACKENDServer(s grpc.ServiceRegistrar, srv ETHBACKENDServer) {
	s.RegisterService(&ETHBACKEND_ServiceDesc, srv)
}

func _ETHBACKEND_Etherbase_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EtherbaseRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).Etherbase(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_Etherbase_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).Etherbase(ctx, req.(*EtherbaseRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_NetVersion_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NetVersionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).NetVersion(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_NetVersion_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).NetVersion(ctx, req.(*NetVersionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_NetPeerCount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NetPeerCountRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).NetPeerCount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_NetPeerCount_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).NetPeerCount(ctx, req.(*NetPeerCountRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_EngineNewPayload_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.ExecutionPayload)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).EngineNewPayload(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_EngineNewPayload_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).EngineNewPayload(ctx, req.(*types.ExecutionPayload))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_EngineForkChoiceUpdated_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EngineForkChoiceUpdatedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).EngineForkChoiceUpdated(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_EngineForkChoiceUpdated_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).EngineForkChoiceUpdated(ctx, req.(*EngineForkChoiceUpdatedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_EngineGetPayload_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EngineGetPayloadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).EngineGetPayload(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_EngineGetPayload_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).EngineGetPayload(ctx, req.(*EngineGetPayloadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_EngineGetPayloadBodiesByHashV1_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EngineGetPayloadBodiesByHashV1Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).EngineGetPayloadBodiesByHashV1(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_EngineGetPayloadBodiesByHashV1_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).EngineGetPayloadBodiesByHashV1(ctx, req.(*EngineGetPayloadBodiesByHashV1Request))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_EngineGetPayloadBodiesByRangeV1_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EngineGetPayloadBodiesByRangeV1Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).EngineGetPayloadBodiesByRangeV1(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_EngineGetPayloadBodiesByRangeV1_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).EngineGetPayloadBodiesByRangeV1(ctx, req.(*EngineGetPayloadBodiesByRangeV1Request))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_Version_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).Version(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_Version_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).Version(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_ProtocolVersion_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProtocolVersionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).ProtocolVersion(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_ProtocolVersion_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).ProtocolVersion(ctx, req.(*ProtocolVersionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_ClientVersion_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClientVersionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).ClientVersion(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_ClientVersion_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).ClientVersion(ctx, req.(*ClientVersionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_Subscribe_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SubscribeRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ETHBACKENDServer).Subscribe(m, &eTHBACKENDSubscribeServer{stream})
}

type ETHBACKEND_SubscribeServer interface {
	Send(*SubscribeReply) error
	grpc.ServerStream
}

type eTHBACKENDSubscribeServer struct {
	grpc.ServerStream
}

func (x *eTHBACKENDSubscribeServer) Send(m *SubscribeReply) error {
	return x.ServerStream.SendMsg(m)
}

func _ETHBACKEND_SubscribeLogs_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ETHBACKENDServer).SubscribeLogs(&eTHBACKENDSubscribeLogsServer{stream})
}

type ETHBACKEND_SubscribeLogsServer interface {
	Send(*SubscribeLogsReply) error
	Recv() (*LogsFilterRequest, error)
	grpc.ServerStream
}

type eTHBACKENDSubscribeLogsServer struct {
	grpc.ServerStream
}

func (x *eTHBACKENDSubscribeLogsServer) Send(m *SubscribeLogsReply) error {
	return x.ServerStream.SendMsg(m)
}

func (x *eTHBACKENDSubscribeLogsServer) Recv() (*LogsFilterRequest, error) {
	m := new(LogsFilterRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _ETHBACKEND_Block_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BlockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).Block(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_Block_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).Block(ctx, req.(*BlockRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_TxnLookup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TxnLookupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).TxnLookup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_TxnLookup_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).TxnLookup(ctx, req.(*TxnLookupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_NodeInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NodesInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).NodeInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_NodeInfo_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).NodeInfo(ctx, req.(*NodesInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_Peers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).Peers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_Peers_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).Peers(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _ETHBACKEND_PendingBlock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ETHBACKENDServer).PendingBlock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ETHBACKEND_PendingBlock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ETHBACKENDServer).PendingBlock(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// ETHBACKEND_ServiceDesc is the grpc.ServiceDesc for ETHBACKEND service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ETHBACKEND_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "remote.ETHBACKEND",
	HandlerType: (*ETHBACKENDServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Etherbase",
			Handler:    _ETHBACKEND_Etherbase_Handler,
		},
		{
			MethodName: "NetVersion",
			Handler:    _ETHBACKEND_NetVersion_Handler,
		},
		{
			MethodName: "NetPeerCount",
			Handler:    _ETHBACKEND_NetPeerCount_Handler,
		},
		{
			MethodName: "EngineNewPayload",
			Handler:    _ETHBACKEND_EngineNewPayload_Handler,
		},
		{
			MethodName: "EngineForkChoiceUpdated",
			Handler:    _ETHBACKEND_EngineForkChoiceUpdated_Handler,
		},
		{
			MethodName: "EngineGetPayload",
			Handler:    _ETHBACKEND_EngineGetPayload_Handler,
		},
		{
			MethodName: "EngineGetPayloadBodiesByHashV1",
			Handler:    _ETHBACKEND_EngineGetPayloadBodiesByHashV1_Handler,
		},
		{
			MethodName: "EngineGetPayloadBodiesByRangeV1",
			Handler:    _ETHBACKEND_EngineGetPayloadBodiesByRangeV1_Handler,
		},
		{
			MethodName: "Version",
			Handler:    _ETHBACKEND_Version_Handler,
		},
		{
			MethodName: "ProtocolVersion",
			Handler:    _ETHBACKEND_ProtocolVersion_Handler,
		},
		{
			MethodName: "ClientVersion",
			Handler:    _ETHBACKEND_ClientVersion_Handler,
		},
		{
			MethodName: "Block",
			Handler:    _ETHBACKEND_Block_Handler,
		},
		{
			MethodName: "TxnLookup",
			Handler:    _ETHBACKEND_TxnLookup_Handler,
		},
		{
			MethodName: "NodeInfo",
			Handler:    _ETHBACKEND_NodeInfo_Handler,
		},
		{
			MethodName: "Peers",
			Handler:    _ETHBACKEND_Peers_Handler,
		},
		{
			MethodName: "PendingBlock",
			Handler:    _ETHBACKEND_PendingBlock_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Subscribe",
			Handler:       _ETHBACKEND_Subscribe_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SubscribeLogs",
			Handler:       _ETHBACKEND_SubscribeLogs_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "remote/ethbackend.proto",
}
