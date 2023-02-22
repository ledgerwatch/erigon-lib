// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.12
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
}

type executionClient struct {
	cc grpc.ClientConnInterface
}

func NewExecutionClient(cc grpc.ClientConnInterface) ExecutionClient {
	return &executionClient{cc}
}

func (c *executionClient) InsertHeaders(ctx context.Context, in *InsertHeadersRequest, opts ...grpc.CallOption) (*EmptyMessage, error) {
	out := new(EmptyMessage)
	err := c.cc.Invoke(ctx, "/execution.Execution/InsertHeaders", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) InsertBodies(ctx context.Context, in *InsertBodiesRequest, opts ...grpc.CallOption) (*EmptyMessage, error) {
	out := new(EmptyMessage)
	err := c.cc.Invoke(ctx, "/execution.Execution/InsertBodies", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) ValidateChain(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*ValidationReceipt, error) {
	out := new(ValidationReceipt)
	err := c.cc.Invoke(ctx, "/execution.Execution/ValidateChain", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) UpdateForkChoice(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*ForkChoiceReceipt, error) {
	out := new(ForkChoiceReceipt)
	err := c.cc.Invoke(ctx, "/execution.Execution/UpdateForkChoice", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) AssembleBlock(ctx context.Context, in *EmptyMessage, opts ...grpc.CallOption) (*types.ExecutionPayload, error) {
	out := new(types.ExecutionPayload)
	err := c.cc.Invoke(ctx, "/execution.Execution/AssembleBlock", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) GetHeader(ctx context.Context, in *GetSegmentRequest, opts ...grpc.CallOption) (*GetHeaderResponse, error) {
	out := new(GetHeaderResponse)
	err := c.cc.Invoke(ctx, "/execution.Execution/GetHeader", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) GetBody(ctx context.Context, in *GetSegmentRequest, opts ...grpc.CallOption) (*GetBodyResponse, error) {
	out := new(GetBodyResponse)
	err := c.cc.Invoke(ctx, "/execution.Execution/GetBody", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) IsCanonicalHash(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*IsCanonicalResponse, error) {
	out := new(IsCanonicalResponse)
	err := c.cc.Invoke(ctx, "/execution.Execution/IsCanonicalHash", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionClient) GetHeaderHashNumber(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*GetHeaderHashNumberResponse, error) {
	out := new(GetHeaderHashNumberResponse)
	err := c.cc.Invoke(ctx, "/execution.Execution/GetHeaderHashNumber", in, out, opts...)
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
		FullMethod: "/execution.Execution/InsertHeaders",
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
		FullMethod: "/execution.Execution/InsertBodies",
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
		FullMethod: "/execution.Execution/ValidateChain",
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
		FullMethod: "/execution.Execution/UpdateForkChoice",
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
		FullMethod: "/execution.Execution/AssembleBlock",
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
		FullMethod: "/execution.Execution/GetHeader",
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
		FullMethod: "/execution.Execution/GetBody",
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
		FullMethod: "/execution.Execution/IsCanonicalHash",
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
		FullMethod: "/execution.Execution/GetHeaderHashNumber",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServer).GetHeaderHashNumber(ctx, req.(*types.H256))
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
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "execution/execution.proto",
}
