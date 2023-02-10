package types

// Minimal Blob Transaction parser for txpool purposes

import (
	"fmt"

	"github.com/holiman/uint256"
	"github.com/protolambda/ztyp/codec"
	"github.com/protolambda/ztyp/view"
)

const (
	FieldElementsPerBlob = 4096
)

type BlobTxNetworkWrapper struct {
	Tx                 SignedBlobTx
	BlobKZGs           [][48]byte
	Blobs              [][FieldElementsPerBlob * 32]byte
	KZGAggregatedProof [48]byte
}

type SignedBlobTx struct {
	Message   BlobTx
	Signature ECDSASignature
}

func (sbtx SignedBlobTx) FixedLength() uint64 { return 0 }
func (sbtx *SignedBlobTx) Deserialize(dr *codec.DecodingReader) error {
	err := dr.Container(&sbtx.Message, &sbtx.Signature)
	if err != nil {
		return fmt.Errorf("failed to deserialize SignedBlobTx: %w", err)
	}
	return nil
}

type BlobTx struct {
	ChainID                uint256.Int
	Nonce                  uint64
	MaxPriorityFeePerGas   uint256.Int
	MaxFeePerGas           uint256.Int
	Gas                    uint64
	Creation               bool // true if To field is nil, indicating contract creation
	Value                  uint256.Int
	DataLen                int // length of the Data in bytes
	AccessListAddressCount int // number of addresses in access list
	AccessListKeyCount     int // number of storage keys in access list

	BlobVersionedHashes [][32]byte
}

func (tx BlobTx) FixedLength() uint64 { return 0 }
func (tx *BlobTx) Deserialize(dr *codec.DecodingReader) error {
	var chainID view.Uint256View
	var nonce view.Uint64View
	var maxPriorityFeePerGas view.Uint256View
	var maxFeePerGas view.Uint256View
	var gas view.Uint64View
	var hasToAddress addressView
	var value view.Uint256View
	var data dataView
	var accessList accessListView
	var maxFeePerDataGas view.Uint256View
	var blobVersionedHashes blobVersionedHashesView
	err := dr.Container(&chainID, &nonce, &maxPriorityFeePerGas, &maxFeePerGas, &gas, &hasToAddress, &value, &data, &accessList, &maxFeePerDataGas, &blobVersionedHashes)
	if err != nil {
		return fmt.Errorf("failed to deserialize BlobTx: %w", err)
	}
	tx.ChainID = uint256.Int(chainID)
	tx.Nonce = uint64(nonce)
	tx.MaxPriorityFeePerGas = uint256.Int(maxPriorityFeePerGas)
	tx.MaxFeePerGas = uint256.Int(maxFeePerGas)
	tx.Gas = uint64(gas)
	if !hasToAddress {
		tx.Creation = true
	}
	tx.Value = uint256.Int(value)
	tx.DataLen = len(data)
	tx.BlobVersionedHashes = [][32]byte(blobVersionedHashes)
	tx.AccessListAddressCount = accessList.addresses
	tx.AccessListKeyCount = accessList.keys
	return nil
}

// For deserializing To field, true if address is non-nil
type addressView bool

func (av addressView) FixedLength() uint64 { return 0 }
func (av *addressView) Deserialize(dr *codec.DecodingReader) error {
	len := dr.Scope()
	b, err := dr.ReadByte()
	if len == 1 {
		if err != nil {
			return err
		}
		if b != 0 {
			return fmt.Errorf("expected 0 byte, got %v", b)
		}
		*av = false
		return nil
	}
	if len != 21 {
		return fmt.Errorf("expected 1 or 21 bytes, got %v", len)
	}
	if b != 1 {
		return fmt.Errorf("expected byte == 1, got %v", b)
	}
	*av = true
	dr.Skip(20)
	return nil
}

// For deserializing the Data field
type dataView []byte

func (dv dataView) FixedLength() uint64 { return 0 }
func (dv *dataView) Deserialize(dr *codec.DecodingReader) error {
	err := dr.ByteList((*[]byte)(dv), 1<<24 /*MAX_CALLDATA_SIZE*/)
	if err != nil {
		return fmt.Errorf("failed to deserialize dataView: %w", err)
	}
	return nil
}

// For deserializing access list field
type accessListView struct {
	addresses int
	keys      int
}

func (alv accessListView) FixedLength() uint64 { return 0 }
func (alv *accessListView) Deserialize(dr *codec.DecodingReader) error {
	// an access list is a list of access list tuples
	tuples := []*tuple{}
	add := func() codec.Deserializable {
		alv.addresses++
		tuple := new(tuple)
		tuples = append(tuples, tuple)
		return tuple
	}
	err := dr.List(add, 0, 1<<24)
	if err != nil {
		return err
	}
	for i := range tuples {
		alv.keys += int(*tuples[i])
	}
	return nil
}

type tuple int // count of keys in the access list tuple

func (t tuple) FixedLength() uint64 { return 0 }
func (t *tuple) Deserialize(dr *codec.DecodingReader) error {
	// an access list tuple consists of 20 bytes for the address, and then 4 bytes for the
	// "offset", followed by the list of 32-byte storage keys.
	scope := dr.Scope()
	if scope < 24 {
		return fmt.Errorf("expected scope >= 24, got %v", scope)
	}
	// subtract address & offset
	scope -= 24
	if scope%32 != 0 {
		return fmt.Errorf("expected multiple of 32 bytes got: %v", scope)
	}
	length := scope / 32
	if length > 1<<24 {
		return fmt.Errorf("too many storage keys: %v", length)
	}
	*t = tuple(length)
	return nil
}

type blobVersionedHashesView [][32]byte

func (b blobVersionedHashesView) FixedLength() uint64 { return 0 }
func (b *blobVersionedHashesView) Deserialize(dr *codec.DecodingReader) error {
	*b = nil
	scope := dr.Scope()
	if scope == 0 {
		return nil
	}
	if scope%32 != 0 {
		return fmt.Errorf("scope not a multiple of 32. got: %v", scope)
	}
	length := scope / 32
	if length > 1<<24 /*MAX_VERSIONED_HASHES_LIST_SIZE*/ {
		return fmt.Errorf("access list too long: %v", length)
	}
	hashes := make([]byte, scope)
	_, err := dr.Read(hashes)
	if err != nil {
		return err
	}
	*b = make([][32]byte, length)
	for i := 0; i < int(length); i++ {
		copy((*b)[i][:], hashes[i*32:i*32+32])
	}
	return nil
}

type ECDSASignature struct {
	V byte
	R [32]byte
	S [32]byte
}

func (sig ECDSASignature) FixedLength() uint64 { return 1 + 32 + 32 }
func (sig *ECDSASignature) Deserialize(dr *codec.DecodingReader) error {
	len := sig.FixedLength()
	scope := dr.Scope()
	if scope != len {
		return fmt.Errorf("failed to decode signature: expected %v bytes got %v", len, scope)
	}
	data := make([]byte, len)
	_, err := dr.Read(data)
	if err != nil {
		return err
	}
	sig.V = data[0]
	copy(sig.R[:], data[1:33])
	copy(sig.S[:], data[33:len])
	return nil
}
