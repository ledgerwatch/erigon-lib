package commitment

import (
	"fmt"
	"math/bits"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/common/length"
)

// Trie represents commitment variant.
type Trie interface {
	ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (branchNodeUpdates map[string][]byte, err error)

	// RootHash produces root hash of the trie
	RootHash() (hash []byte, err error)

	// Variant returns commitment trie variant
	Variant() TrieVariant

	// Reset Drops everything from the trie
	Reset()

	ResetFns(
		branchFn func(prefix []byte) ([]byte, error),
		accountFn func(plainKey []byte, cell *Cell) error,
		storageFn func(plainKey []byte, cell *Cell) error,
	)

	// Makes trie more verbose
	SetTrace(bool)
}

type TrieVariant string

const (
	// HexPatriciaHashed used as default commitment approach
	VariantHexPatriciaTrie TrieVariant = "hex-patricia-hashed"
	// Experimental mode with binary key representation
	VariantBinPatriciaTrie TrieVariant = "bin-patricia-hashed"
)

func InitializeTrie(tv TrieVariant) Trie {
	switch tv {
	case VariantBinPatriciaTrie:
		return NewBinaryPatriciaTrie()
	case VariantHexPatriciaTrie:
		fallthrough
	default:
		return NewHexPatriciaHashed(length.Addr, nil, nil, nil)
	}
}

type Account struct {
	CodeHash []byte // hash of the bytecode
	Nonce    uint64
	Balance  uint256.Int
}

func (a *Account) String() string {
	return fmt.Sprintf("Account{Nonce: %d, Balance: %d, CodeHash: %x}", a.Nonce, a.Balance, a.CodeHash)
}

func (a *Account) decode(buffer []byte) *Account {
	size := uint64(buffer[0])
	var pos int
	if size < 192+56 {
		size -= 192
		_ = size
	} else {
		sizeBytes := int(size - 247)
		size = 0
		for i := 1; i <= sizeBytes; i++ {
			n := uint64(buffer[pos+i])
			size |= n << (8 * (sizeBytes - i))
		}
		pos += sizeBytes
	}
	pos++

	if buffer[pos] < 128 {
		a.Nonce = uint64(buffer[pos])
	} else {
		var nonce uint64
		sizeBytes := int(buffer[pos] - 128)
		for i := 1; i <= sizeBytes; i++ {
			nonce |= uint64(buffer[pos+i]) << (8 * (sizeBytes - i))
		}
		a.Nonce = nonce
		pos += sizeBytes
	}
	pos++

	if buffer[pos] < 128 {
		b := uint256.NewInt(uint64(buffer[pos]))
		a.Balance = *b
	} else {
		bc := int(buffer[pos] - 128)
		a.Balance.SetBytes(buffer[pos+1 : pos+1+bc])
		pos += bc
	}
	pos++

	if buffer[pos] == 160 {
		pos++
		copy(a.CodeHash, buffer[pos:pos+32])
	}
	return a
}

func (a *Account) encode(buffer []byte) int {
	balanceBytes := 0
	if !a.Balance.LtUint64(128) {
		balanceBytes = a.Balance.ByteLen()
	}

	var nonceBytes int
	if a.Nonce < 128 && a.Nonce != 0 {
		nonceBytes = 0
	} else {
		nonceBytes = (bits.Len64(a.Nonce) + 7) / 8
	}

	var structLength = uint(balanceBytes + nonceBytes + 2)
	structLength += 36 // onee 32-byte array + 2 prefixes

	var pos int
	if structLength < 56 {
		buffer[0] = byte(192 + structLength)
		pos = 1
	} else {
		lengthBytes := (bits.Len(structLength) + 7) / 8
		buffer[0] = byte(247 + lengthBytes)

		for i := lengthBytes; i > 0; i-- {
			buffer[i] = byte(structLength)
			structLength >>= 8
		}

		pos = lengthBytes + 1
	}

	// Encoding nonce
	if a.Nonce < 128 && a.Nonce != 0 {
		buffer[pos] = byte(a.Nonce)
	} else {
		buffer[pos] = byte(128 + nonceBytes)
		var nonce = a.Nonce
		for i := nonceBytes; i > 0; i-- {
			buffer[pos+i] = byte(nonce)
			nonce >>= 8
		}
	}
	pos += 1 + nonceBytes

	// Encoding balance
	if a.Balance.LtUint64(128) && !a.Balance.IsZero() {
		buffer[pos] = byte(a.Balance.Uint64())
		pos++
	} else {
		buffer[pos] = byte(128 + balanceBytes)
		pos++
		a.Balance.WriteToSlice(buffer[pos : pos+balanceBytes])
		pos += balanceBytes
	}

	// Encoding CodeHash
	buffer[pos] = 128

	pos++
	if len(a.CodeHash) == 32 {
		buffer[pos-1] += 32

		copy(buffer[pos:pos+32], a.CodeHash)
		pos += 32
	}
	return pos
}
