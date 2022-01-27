package starknet

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/starknet"
	"google.golang.org/grpc"
)

type stubGrpcClient struct {
	starknet.CAIROVMClient
	callCount    int
	addressCount int
}

func NewGrpcClientStub() *stubGrpcClient {
	return &stubGrpcClient{}
}

func (c *stubGrpcClient) Call(ctx context.Context, in *starknet.CallRequest, opts ...grpc.CallOption) (*starknet.CallResponse, error) {
	c.callCount++

	out := new(starknet.CallResponse)
	return out, nil
}

func (c *stubGrpcClient) Address(ctx context.Context, in *starknet.AddressRequest, opts ...grpc.CallOption) (*starknet.AddressResponse, error) {
	c.addressCount++

	out := new(starknet.AddressResponse)
	return out, nil
}

func (c stubGrpcClient) GetCallCalls() int {
	return c.callCount
}

func (c stubGrpcClient) GetAddressCalls() int {
	return c.addressCount
}
