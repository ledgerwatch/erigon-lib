package starknet_test

import (
	starknet2 "github.com/ledgerwatch/erigon-lib/starknet"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"path"
	"testing"
)

const (
	testDataFolder   = "testdata"
	contractFileName = "cairo.json"
)

func TestUnmarshalContractDefinition(t *testing.T) {
	require := require.New(t)

	currentFolder, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}

	cairoContractFileName := path.Join(currentFolder, testDataFolder, contractFileName)
	file, err := os.Open(cairoContractFileName)

	cd, err := starknet2.DecodeContractDefinition(file)

	require.NoError(err)

	//EntryPointsByType
	epbt := cd.GetEntryPointsByType()
	require.Empty(epbt.GetCONSTRUCTOR())
	require.Equal(2, len(epbt.GetEXTERNAL()))
	require.Equal("0x3a", epbt.GetEXTERNAL()[0].GetOffset())
	require.Equal("0x362398bec32bc0ebb411203221a35a0301193a96f317ebe5e40be9f60d15320", epbt.GetEXTERNAL()[0].GetSelector())
	require.Equal("0x5b", epbt.GetEXTERNAL()[1].GetOffset())
	require.Equal("0x39e11d48192e4333233c7eb19d10ad67c362bb28580c604d67884c85da39695", epbt.GetEXTERNAL()[1].GetSelector())

	//Program
	p := cd.GetProgram()
	require.Empty(p.GetAttributes())
	require.Equal(2, len(p.GetBuiltins()))
	require.Equal(106, len(p.GetData()))

	println(p.GetHints())
}
