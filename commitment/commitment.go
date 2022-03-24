package commitment

import (
	"github.com/ledgerwatch/erigon-lib/common/length"
)

// Trie represents commitment variant.
type Trie interface {
	ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (branchNodeUpdates map[string][]byte, err error)

	RootHash() (hash []byte, err error)

	Reset()

	ResetFns(
		branchFn func(prefix []byte) ([]byte, error),
		accountFn func(plainKey []byte, cell *Cell) error,
		storageFn func(plainKey []byte, cell *Cell) error,
	)

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
		return NewBinPatriciaTrie()
	case VariantHexPatriciaTrie:
		fallthrough
	default:
		return NewHexPatriciaHashed(length.Addr, nil, nil, nil)
	}
}
