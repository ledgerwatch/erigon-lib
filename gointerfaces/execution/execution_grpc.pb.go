// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.22.3
// source: execution/execution.proto

package execution

import (
	context "context"
	types "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	Execution_InsertHeaders_FullMethodName                   = "/execution.Execution/InsertHeaders"
	Execution_InsertBodies_FullMethodName                    = "/execution.Execution/InsertBodies"
	Execution_ValidateChain_FullMethodName                   = "/execution.Execution/ValidateChain"
	Execution_UpdateForkChoice_FullMethodName                = "/execution.Execution/UpdateForkChoice"
	Execution_AssembleBlock_FullMethodName                   = "/execution.Execution/AssembleBlock"
	Execution_GetHeader_FullMethodName                       = "/execution.Execution/GetHeader"
	Execution_GetBody_FullMethodName                         = "/execution.Execution/GetBody"
	Execution_IsCanonicalHash_FullMethodName                 = "/execution.Execution/IsCanonicalHash"
	Execution_GetHeaderHashNumber_FullMethodName             = "/execution.Execution/GetHeaderHashNumber"
	Execution_EngineNewPayload_FullMethodName                = "/execution.Execution/EngineNewPayload"
	Execution_EngineForkChoiceUpdated_FullMethodName         = "/execution.Execution/EngineForkChoiceUpdated"
	Execution_EngineGetPayload_FullMethodName                = "/execution.Execution/EngineGetPayload"
	Execution_EngineGetPayloadBodiesByHashV1_FullMethodName  = "/execution.Execution/EngineGetPayloadBodiesByHashV1"
	Execution_EngineGetPayloadBodiesByRangeV1_FullMethodName = "/execution.Execution/EngineGetPayloadBodiesByRangeV1"
)

// ExecutionClient is the client API for Execution service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ExecutionClient interface {
	// Chain Putters.
	InsertHeaders(ctx context.Context, in *InsertHeadersRequest, opts ...grpc.CallOption) (*EmptyMessage, error)
	InsertBodies(ctx context.Context, in *InsertBodiesRequest, opts ...grpc.CallOption) (*EmptyMessage, error)
	// Chain Validation and ForkChoice.
	ValidateChain(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*ValidationReceipt, error)
	UpdateForkChoice(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*ForkChoiceReceipt, error)
	AssembleBlock(ctx context.Context, in *EmptyMessage, opts ...grpc.CallOption) (*types.ExecutionPayload, error)
	// Chain Getters.
	GetHeader(ctx context.Context, in *GetSegmentRequest, opts ...grpc.CallOption) (*GetHeaderResponse, error)
	GetBody(ctx context.Context, in *GetSegmentRequest, opts ...grpc.CallOption) (*GetBodyResponse, error)
	IsCanonicalHash(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*IsCanonicalResponse, error)
	GetHeaderHashNumber(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*GetHeaderHashNumberResponse, error)
	// Validate and possibly execute the payload.
	EngineNewPayload(ctx context.Context, in *types.ExecutionPayload, opts ...grpc.CallOption) (*EnginePayloadStatus, error)
	// Update fork choice
	EngineForkChoiceUpdated(ctx context.Context, in *EngineForkChoiceUpdatedRequest, opts ...grpc.CallOption) (*EngineForkChoiceUpdatedResponse, error)
	// Fetch the payload along with its blobs by ID.
	EngineGetPayload(ctx context.Context, in *EngineGetPayloadRequest, opts ...grpc.CallOption) (*EngineGetPayloadResponse, error)
	EngineGetPayloadBodiesByHashV1(ctx context.Context, in *EngineGetPayloadBodiesByHashV1Request, opts ...grpc.CallOption) (*EngineGetPayloadBodiesV1Response, error)
	EngineGetPayloadBodiesByRangeV1(ctx context.Context, in *EngineGetPayloadBodiesByRangeV1Request, opts ...grpc.CallOption) (*EngineGetPayloadBodiesV1Response, error)
}

type executionClient struct {
	cc grpc.ClientConnInterface
}

func NewExecutionClient(cc grpc.ClientConnInterface) ExecutionClient {
	return &executionClient{cc}
}

func (c *executionClient) InsertHeaders(ctx context.Context, in *InsertHeadersRequest, opts ...grpc.CallOption) (*EmptyMessage, error) {
	out := new(EmptyMessage)
	err := c.cc.Invoke(ctx, Execution_InsertHeaders_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) InsertBodies(ctx context.Context, in *InsertBodiesRequest, opts ...grpc.CallOption) (*EmptyMessage, error) {
	out := new(EmptyMessage)
	err := c.cc.Invoke(ctx, Execution_InsertBodies_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) ValidateChain(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*ValidationReceipt, error) {
	out := new(ValidationReceipt)
	err := c.cc.Invoke(ctx, Execution_ValidateChain_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) UpdateForkChoice(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*ForkChoiceReceipt, error) {
	out := new(ForkChoiceReceipt)
	err := c.cc.Invoke(ctx, Execution_UpdateForkChoice_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) AssembleBlock(ctx context.Context, in *EmptyMessage, opts ...grpc.CallOption) (*types.ExecutionPayload, error) {
	out := new(types.ExecutionPayload)
	err := c.cc.Invoke(ctx, Execution_AssembleBlock_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) GetHeader(ctx context.Context, in *GetSegmentRequest, opts ...grpc.CallOption) (*GetHeaderResponse, error) {
	out := new(GetHeaderResponse)
	err := c.cc.Invoke(ctx, Execution_GetHeader_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) GetBody(ctx context.Context, in *GetSegmentRequest, opts ...grpc.CallOption) (*GetBodyResponse, error) {
	out := new(GetBodyResponse)
	err := c.cc.Invoke(ctx, Execution_GetBody_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) IsCanonicalHash(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*IsCanonicalResponse, error) {
	out := new(IsCanonicalResponse)
	err := c.cc.Invoke(ctx, Execution_IsCanonicalHash_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) GetHeaderHashNumber(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*GetHeaderHashNumberResponse, error) {
	out := new(GetHeaderHashNumberResponse)
	err := c.cc.Invoke(ctx, Execution_GetHeaderHashNumber_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) EngineNewPayload(ctx context.Context, in *types.ExecutionPayload, opts ...grpc.CallOption) (*EnginePayloadStatus, error) {
	out := new(EnginePayloadStatus)
	err := c.cc.Invoke(ctx, Execution_EngineNewPayload_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) EngineForkChoiceUpdated(ctx context.Context, in *EngineForkChoiceUpdatedRequest, opts ...grpc.CallOption) (*EngineForkChoiceUpdatedResponse, error) {
	out := new(EngineForkChoiceUpdatedResponse)
	err := c.cc.Invoke(ctx, Execution_EngineForkChoiceUpdated_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) EngineGetPayload(ctx context.Context, in *EngineGetPayloadRequest, opts ...grpc.CallOption) (*EngineGetPayloadResponse, error) {
	out := new(EngineGetPayloadResponse)
	err := c.cc.Invoke(ctx, Execution_EngineGetPayload_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) EngineGetPayloadBodiesByHashV1(ctx context.Context, in *EngineGetPayloadBodiesByHashV1Request, opts ...grpc.CallOption) (*EngineGetPayloadBodiesV1Response, error) {
	out := new(EngineGetPayloadBodiesV1Response)
	err := c.cc.Invoke(ctx, Execution_EngineGetPayloadBodiesByHashV1_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) EngineGetPayloadBodiesByRangeV1(ctx context.Context, in *EngineGetPayloadBodiesByRangeV1Request, opts ...grpc.CallOption) (*EngineGetPayloadBodiesV1Response, error) {
	out := new(EngineGetPayloadBodiesV1Response)
	err := c.cc.Invoke(ctx, Execution_EngineGetPayloadBodiesByRangeV1_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ExecutionServer is the server API for Execution service.
// All implementations must embed UnimplementedExecutionServer
// for forward compatibility
type ExecutionServer interface {
	// Chain Putters.
	InsertHeaders(context.Context, *InsertHeadersRequest) (*EmptyMessage, error)
	InsertBodies(context.Context, *InsertBodiesRequest) (*EmptyMessage, error)
	// Chain Validation and ForkChoice.
	ValidateChain(context.Context, *types.H256) (*ValidationReceipt, error)
	UpdateForkChoice(context.Context, *types.H256) (*ForkChoiceReceipt, error)
	AssembleBlock(context.Context, *EmptyMessage) (*types.ExecutionPayload, error)
	// Chain Getters.
	GetHeader(context.Context, *GetSegmentRequest) (*GetHeaderResponse, error)
	GetBody(context.Context, *GetSegmentRequest) (*GetBodyResponse, error)
	IsCanonicalHash(context.Context, *types.H256) (*IsCanonicalResponse, error)
	GetHeaderHashNumber(context.Context, *types.H256) (*GetHeaderHashNumberResponse, error)
	// Validate and possibly execute the payload.
	EngineNewPayload(context.Context, *types.ExecutionPayload) (*EnginePayloadStatus, error)
	// Update fork choice
	EngineForkChoiceUpdated(context.Context, *EngineForkChoiceUpdatedRequest) (*EngineForkChoiceUpdatedResponse, error)
	// Fetch the payload along with its blobs by ID.
	EngineGetPayload(context.Context, *EngineGetPayloadRequest) (*EngineGetPayloadResponse, error)
	EngineGetPayloadBodiesByHashV1(context.Context, *EngineGetPayloadBodiesByHashV1Request) (*EngineGetPayloadBodiesV1Response, error)
	EngineGetPayloadBodiesByRangeV1(context.Context, *EngineGetPayloadBodiesByRangeV1Request) (*EngineGetPayloadBodiesV1Response, error)
	mustEmbedUnimplementedExecutionServer()
}

// UnimplementedExecutionServer must be embedded to have forward compatible implementations.
type UnimplementedExecutionServer struct {
}

func (UnimplementedExecutionServer) InsertHeaders(context.Context, *InsertHeadersRequest) (*EmptyMessage, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InsertHeaders not implemented")
}
func (UnimplementedExecutionServer) InsertBodies(context.Context, *InsertBodiesRequest) (*EmptyMessage, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InsertBodies not implemented")
}
func (UnimplementedExecutionServer) ValidateChain(context.Context, *types.H256) (*ValidationReceipt, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ValidateChain not implemented")
}
func (UnimplementedExecutionServer) UpdateForkChoice(context.Context, *types.H256) (*ForkChoiceReceipt, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateForkChoice not implemented")
}
func (UnimplementedExecutionServer) AssembleBlock(context.Context, *EmptyMessage) (*types.ExecutionPayload, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AssembleBlock not implemented")
}
func (UnimplementedExecutionServer) GetHeader(context.Context, *GetSegmentRequest) (*GetHeaderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetHeader not implemented")
}
func (UnimplementedExecutionServer) GetBody(context.Context, *GetSegmentRequest) (*GetBodyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBody not implemented")
}
func (UnimplementedExecutionServer) IsCanonicalHash(context.Context, *types.H256) (*IsCanonicalResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method IsCanonicalHash not implemented")
}
func (UnimplementedExecutionServer) GetHeaderHashNumber(context.Context, *types.H256) (*GetHeaderHashNumberResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetHeaderHashNumber not implemented")
}
func (UnimplementedExecutionServer) EngineNewPayload(context.Context, *types.ExecutionPayload) (*EnginePayloadStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineNewPayload not implemented")
}
func (UnimplementedExecutionServer) EngineForkChoiceUpdated(context.Context, *EngineForkChoiceUpdatedRequest) (*EngineForkChoiceUpdatedResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineForkChoiceUpdated not implemented")
}
func (UnimplementedExecutionServer) EngineGetPayload(context.Context, *EngineGetPayloadRequest) (*EngineGetPayloadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineGetPayload not implemented")
}
func (UnimplementedExecutionServer) EngineGetPayloadBodiesByHashV1(context.Context, *EngineGetPayloadBodiesByHashV1Request) (*EngineGetPayloadBodiesV1Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineGetPayloadBodiesByHashV1 not implemented")
}
func (UnimplementedExecutionServer) EngineGetPayloadBodiesByRangeV1(context.Context, *EngineGetPayloadBodiesByRangeV1Request) (*EngineGetPayloadBodiesV1Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EngineGetPayloadBodiesByRangeV1 not implemented")
}
func (UnimplementedExecutionServer) mustEmbedUnimplementedExecutionServer() {}

// UnsafeExecutionServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ExecutionServer will
// result in compilation errors.
type UnsafeExecutionServer interface {
	mustEmbedUnimplementedExecutionServer()
}

func RegisterExecutionServer(s grpc.ServiceRegistrar, srv ExecutionServer) {
	s.RegisterService(&Execution_ServiceDesc, srv)
}

func _Execution_InsertHeaders_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InsertHeadersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).InsertHeaders(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_InsertHeaders_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).InsertHeaders(ctx, req.(*InsertHeadersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_InsertBodies_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InsertBodiesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).InsertBodies(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_InsertBodies_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).InsertBodies(ctx, req.(*InsertBodiesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_ValidateChain_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.H256)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).ValidateChain(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_ValidateChain_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).ValidateChain(ctx, req.(*types.H256))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_UpdateForkChoice_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.H256)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).UpdateForkChoice(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_UpdateForkChoice_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).UpdateForkChoice(ctx, req.(*types.H256))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_AssembleBlock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EmptyMessage)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).AssembleBlock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_AssembleBlock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).AssembleBlock(ctx, req.(*EmptyMessage))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_GetHeader_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetSegmentRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).GetHeader(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_GetHeader_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).GetHeader(ctx, req.(*GetSegmentRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_GetBody_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetSegmentRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).GetBody(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_GetBody_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).GetBody(ctx, req.(*GetSegmentRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_IsCanonicalHash_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.H256)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).IsCanonicalHash(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_IsCanonicalHash_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).IsCanonicalHash(ctx, req.(*types.H256))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_GetHeaderHashNumber_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.H256)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).GetHeaderHashNumber(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_GetHeaderHashNumber_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).GetHeaderHashNumber(ctx, req.(*types.H256))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_EngineNewPayload_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.ExecutionPayload)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).EngineNewPayload(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_EngineNewPayload_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).EngineNewPayload(ctx, req.(*types.ExecutionPayload))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_EngineForkChoiceUpdated_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EngineForkChoiceUpdatedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).EngineForkChoiceUpdated(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_EngineForkChoiceUpdated_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).EngineForkChoiceUpdated(ctx, req.(*EngineForkChoiceUpdatedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_EngineGetPayload_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EngineGetPayloadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).EngineGetPayload(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_EngineGetPayload_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).EngineGetPayload(ctx, req.(*EngineGetPayloadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_EngineGetPayloadBodiesByHashV1_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EngineGetPayloadBodiesByHashV1Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).EngineGetPayloadBodiesByHashV1(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_EngineGetPayloadBodiesByHashV1_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).EngineGetPayloadBodiesByHashV1(ctx, req.(*EngineGetPayloadBodiesByHashV1Request))
	}
	return interceptor(ctx, in, info, handler)
}

func _Execution_EngineGetPayloadBodiesByRangeV1_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EngineGetPayloadBodiesByRangeV1Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServer).EngineGetPayloadBodiesByRangeV1(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Execution_EngineGetPayloadBodiesByRangeV1_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).EngineGetPayloadBodiesByRangeV1(ctx, req.(*EngineGetPayloadBodiesByRangeV1Request))
	}
	return interceptor(ctx, in, info, handler)
}

// Execution_ServiceDesc is the grpc.ServiceDesc for Execution service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Execution_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "execution.Execution",
	HandlerType: (*ExecutionServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "InsertHeaders",
			Handler:    _Execution_InsertHeaders_Handler,
		},
		{
			MethodName: "InsertBodies",
			Handler:    _Execution_InsertBodies_Handler,
		},
		{
			MethodName: "ValidateChain",
			Handler:    _Execution_ValidateChain_Handler,
		},
		{
			MethodName: "UpdateForkChoice",
			Handler:    _Execution_UpdateForkChoice_Handler,
		},
		{
			MethodName: "AssembleBlock",
			Handler:    _Execution_AssembleBlock_Handler,
		},
		{
			MethodName: "GetHeader",
			Handler:    _Execution_GetHeader_Handler,
		},
		{
			MethodName: "GetBody",
			Handler:    _Execution_GetBody_Handler,
		},
		{
			MethodName: "IsCanonicalHash",
			Handler:    _Execution_IsCanonicalHash_Handler,
		},
		{
			MethodName: "GetHeaderHashNumber",
			Handler:    _Execution_GetHeaderHashNumber_Handler,
		},
		{
			MethodName: "EngineNewPayload",
			Handler:    _Execution_EngineNewPayload_Handler,
		},
		{
			MethodName: "EngineForkChoiceUpdated",
			Handler:    _Execution_EngineForkChoiceUpdated_Handler,
		},
		{
			MethodName: "EngineGetPayload",
			Handler:    _Execution_EngineGetPayload_Handler,
		},
		{
			MethodName: "EngineGetPayloadBodiesByHashV1",
			Handler:    _Execution_EngineGetPayloadBodiesByHashV1_Handler,
		},
		{
			MethodName: "EngineGetPayloadBodiesByRangeV1",
			Handler:    _Execution_EngineGetPayloadBodiesByRangeV1_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "execution/execution.proto",
}
