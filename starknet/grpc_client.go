package starknet

import (
	"fmt"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/starknet"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/log/v3"
)

var StarknetAPIVersion = &types2.VersionReply{Major: 1, Minor: 0, Patch: 0}

type GrpcClient struct {
	starknet.CAIROVMClient
	log     log.Logger
	version gointerfaces.Version
}

func NewGrpcClient(address string) (*GrpcClient, error) {
	if address == "" {
		return nil, nil
	}

	cc, err := grpcutil.Connect(nil, address)
	if err != nil {
		return nil, fmt.Errorf("could not connect to starknet api: %w", err)
	}

	return &GrpcClient{
		CAIROVMClient: starknet.NewCAIROVMClient(cc),
		version:       gointerfaces.VersionFromProto(StarknetAPIVersion),
		log:           log.New("remote_service", "starknet"),
	}, nil
}

func (s *GrpcClient) EnsureVersionCompatibility() bool {
	//TODO: add version check
	return true
}
