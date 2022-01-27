package starknet

import (
	"encoding/json"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/starknet"
	"io"
)

func DecodeContractDefinition(r io.Reader) (*starknet.ContractDefinition, error) {
	var contractDefinition *starknet.ContractDefinition
	err := json.NewDecoder(r).Decode(&contractDefinition)
	if err != nil {
		return nil, err
	}

	return contractDefinition, nil
}
