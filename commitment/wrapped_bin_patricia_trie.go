package commitment

import (
	"errors"
	"fmt"
	"math/bits"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/commitment/bin/mbt"
)

type wrappedBinaryTrie struct {
	t     *mbt.BinaryTrie
	trace bool
}

func NewBinPatriciaTrie() *wrappedBinaryTrie {
	return &wrappedBinaryTrie{
		t:     mbt.NewBinaryTrie(),
		trace: false,
	}
}

func (w *wrappedBinaryTrie) RootHash() ([]byte, error) {
	return w.t.Hash(), nil
}

func (w *wrappedBinaryTrie) Reset() {}
func (w *wrappedBinaryTrie) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) error,
	storageFn func(plainKey []byte, cell *Cell) error,
) {
}

func (w *wrappedBinaryTrie) SetTrace(trace bool) { w.trace = trace }

func (w *wrappedBinaryTrie) ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (branchNodeUpdates map[string][]byte, err error) {
	branchNodeUpdates = make(map[string][]byte)
	for i, update := range updates {
		account := new(Account)
		node, err := w.t.TryGet(hashedKeys[i]) // check if key exist
		if err != nil {
			if !errors.Is(err, mbt.ErrKeyNotPresent) || errors.Is(err, mbt.ErrReadFromEmptyTree) {
				return nil, fmt.Errorf("failed to check key: %w", err)
			}
		} else {
			// key exists, decode value to account
			account.decode(node)
		}

		// apply supported updates
		switch {
		case update.Flags&BALANCE_UPDATE != 0:
			account.Balance.Set(&update.Balance)
		case update.Flags&NONCE_UPDATE != 0:
			account.Nonce = update.Nonce
		case update.Flags&CODE_UPDATE != 0:
			copy(account.CodeHash[:], update.CodeHashOrStorage[:])
		default:
			if w.trace {
				fmt.Printf("update of type %d has been skipped: unsupported for bin trie", update.Flags)
			}
			continue

		}
		aux := make([]byte, 128)
		n := account.encode(aux)

		if err := w.t.TryUpdate(hashedKeys[i], aux[:n]); err != nil {
			return nil, fmt.Errorf("update failed: %w", err)
		}
	}

	return branchNodeUpdates, nil
}

type Account struct {
	CodeHash [32]byte // hash of the bytecode
	Nonce    uint64
	Balance  uint256.Int
}

func (a *Account) decode(buffer []byte) *Account {
	size := uint64(buffer[0])
	var pos int
	if size < 192+56 {
		size -= 192
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
		copy(a.CodeHash[:], buffer[pos:pos+32])
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
	buffer[pos] = 128 + 32
	pos++
	copy(buffer[pos:], a.CodeHash[:])
	pos += 32
	return pos
}
