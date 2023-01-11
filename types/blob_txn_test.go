package types

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/protolambda/ztyp/codec"
)

var (
	// signedBlobTxHex is the SSZ encoding of the following tx:
	// {
	//   "nonce": "0xa",
	//   "gasPrice": null,
	//   "maxPriorityFeePerGas": "0x2a",
	//   "maxFeePerGas": "0xa",
	//   "gas": "0x1e241",
	//   "value": "0x64",
	//   "input": "0x616263646566",
	//   "v": "0x1",
	//   "r": "0xe995f26f2f424703e00ef9c9709248dc6587f3045e2dd536eedf96651a4b680d",
	//   "s": "0x13836dded49612eb61c61e9c61aa343a26f4ba37b5d53da3b6d9326b64a09668",
	//   "to": "0x095e7baea6a6c7c4c2dfeb977efac326af552d87",
	//   "chainId": "0x1",
	//   "accessList": [
	//     {
	//       "address": "0x0000000000000000000000000000000000000001",
	//       "storageKeys": [
	//         "0x0000000000000000000000000000000000000000000000000000000000000000",
	//         "0x0100000000000000000000000000000000000000000000000000000000000000"
	//       ]
	//     },
	//     {
	//       "address": "0x0000000000000000000000000000000000000002",
	//       "storageKeys": [
	//         "0x0200000000000000000000000000000000000000000000000000000000000000"
	//       ]
	//     }
	//   ],
	//   "maxFeePerDataGas": "0x0",
	//   "blobVersionedHashes": [
	//     "0x010657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c444014",
	//     "0x00000000000000000000000000000000000000000000000000000000deadbeef"
	//   ],
	//   "kzgAggregatedProof": "0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
	//   "hash": "0xabfab29ef05293b52e448f5e85eae4a99c1496cdf59f59987a37ba90912c8801"
	// }

	signedBlobTxHex = "45000000010d684b1a6596dfee36d52d5e04f38765dc489270c9f90ee00347422f6ff295e96896a0646b32d9b6a33dd5b537baf4263a34aa619c1ec661eb1296d4de6d831301000000000000000000000000000000000000000000000000000000000000000a000000000000002a000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000041e2010000000000c00000006400000000000000000000000000000000000000000000000000000000000000d5000000db00000000000000000000000000000000000000000000000000000000000000000000007301000001095e7baea6a6c7c4c2dfeb977efac326af552d876162636465660800000060000000000000000000000000000000000000000000000118000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002180000000200000000000000000000000000000000000000000000000000000000000000010657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c44401400000000000000000000000000000000000000000000000000000000deadbeef"

	// Same tx as above only with nil To field to test contract creation indicator
	signedBlobTxNoRecipientHex = "45000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000a000000000000002a000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000041e2010000000000c00000006400000000000000000000000000000000000000000000000000000000000000c1000000c700000000000000000000000000000000000000000000000000000000000000000000005f010000006162636465660800000060000000000000000000000000000000000000000000000118000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002180000000200000000000000000000000000000000000000000000000000000000000000010657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c44401400000000000000000000000000000000000000000000000000000000deadbeef"

	// blobTxNetworkWrapperHex is an ssz encoded BlobTxNetworkWrapper with 2 valid blobs & a valid
	// aggregated kzg proof
	//go:embed testdata/blobtx.txt
	blobTxNetworkWrapperHex string
)

func txFromHex(hexStr string, tx codec.Deserializable) error {
	txBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return err
	}
	buf := bytes.NewReader(txBytes)
	dr := codec.NewDecodingReader(buf, uint64(len(txBytes)))
	err = tx.Deserialize(dr)
	if err != nil {
		return err
	}
	return nil
}

func TestParseSignedBlobTx(t *testing.T) {
	tx := SignedBlobTx{}
	err := txFromHex(signedBlobTxHex, &tx)
	if err != nil {
		t.Fatalf("couldn't create test case: %v", err)
	}
	msg := tx.Message
	if msg.ChainID.Uint64() != 1 {
		t.Errorf("Expected chain id == 1, got: %v", msg.ChainID.Uint64())
	}
	if msg.Nonce != 10 {
		t.Errorf("Expected nonce == 10, got: %v", msg.Nonce)
	}
	if msg.MaxPriorityFeePerGas.Uint64() != 42 {
		t.Errorf("Expected MaxPriorityFeePerGas == 42, got %v", msg.MaxPriorityFeePerGas.Uint64())
	}
	if msg.MaxFeePerGas.Uint64() != 10 {
		t.Errorf("Expected MaxFeePerGas == 10, got %v", msg.MaxFeePerGas.Uint64())
	}
	if msg.Gas != 123457 {
		t.Errorf("Expected Gas == 123457, got %v", msg.Gas)
	}
	if msg.Creation == true {
		t.Errorf("Expected !msg.Creation")
	}
	if msg.Value.Uint64() != 100 {
		t.Errorf("Expected msg.Value == 100, got %v", msg.Value.Uint64())
	}
	if msg.DataLen != 6 {
		t.Errorf("Expected DataLen == 6, got %v", msg.DataLen)
	}
	if len(msg.BlobVersionedHashes) != 2 {
		t.Errorf("Expected 2 blob hashes, got %v", len(msg.BlobVersionedHashes))
	}
	if msg.AccessListAddressCount != 2 {
		t.Errorf("Expected 2 addresses in access list, got %v", msg.AccessListAddressCount)
	}
	if msg.AccessListKeyCount != 3 {
		t.Errorf("Expected 3 keys in access list, got %v", msg.AccessListKeyCount)
	}

	sig := tx.Signature
	if sig.V != 1 {
		t.Errorf("Expected sig.V == 1, got %v", sig.V)
	}

	// Test "Creation == true"
	tx = SignedBlobTx{}
	err = txFromHex(signedBlobTxNoRecipientHex, &tx)
	if err != nil {
		t.Fatalf("couldn't create test case: %v", err)
	}
	if tx.Message.Creation == false {
		t.Errorf("Expected msg.Creation")
	}
}

func TestParseBlobTxNetworkWrapper(t *testing.T) {
	tx := BlobTxNetworkWrapper{}
	err := txFromHex(strings.TrimSpace(blobTxNetworkWrapperHex), &tx)
	if err != nil {
		t.Fatalf("couldn't create test case: %v", err)
	}
	l1, l2, l3 := len(tx.BlobKZGs), len(tx.Blobs), len(tx.Tx.Message.BlobVersionedHashes)
	if l1 != 2 || l2 != 2 || l3 = l3 {
		t.Errorf("Expected 2 each of kzgs / blobs / hashes, got: %v %v %v", l1, l2, l3)
	}
}
