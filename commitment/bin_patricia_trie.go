package commitment

import (
	"encoding/hex"
	"fmt"

	"golang.org/x/crypto/sha3"
)

type BinPatriciaTrie struct {
	root  *Node
	trace bool

	keccak keccakState
}

func NewBinaryPatriciaTrie() *BinPatriciaTrie {
	return &BinPatriciaTrie{
		keccak: sha3.NewLegacyKeccak256().(keccakState),
	}
}

func (t *BinPatriciaTrie) Update(key, value []byte) {
	keyPath := newBitstring(key)
	if t.root == nil {
		t.root = &Node{
			Key:   key,
			Value: value,
		}
		return
	}

	edge, keyPathRest, splitAt, latest := t.root.followPath(keyPath)
	if len(edge) == 0 && len(keyPathRest) == 0 {
		latest.Value = value
		return
	}

	newLeaf := &Node{P: latest, Key: key, Value: value}
	latest.splitEdge(edge[:splitAt], edge[splitAt:], keyPathRest, keyPath, newLeaf)
}

// Key describes path in trie to value. When UpdateHashed is used,
// hashed key describes path to the leaf node and plainKey is stored in the leaf node Key field.
func (t *BinPatriciaTrie) UpdateHahsed(plainKey, hashedKey, value []byte) {
	keyPath := newBitstring(hashedKey)
	if t.root == nil {
		t.root = &Node{
			Key:   hashedKey,
			Value: value,
		}
		return
	}

	edge, keyPathRest, splitAt, latest := t.root.followPath(keyPath)
	if len(edge) == 0 && len(keyPathRest) == 0 {
		latest.Value = value
		return
	}

	newLeaf := &Node{P: latest, Key: plainKey, Value: value}
	latest.splitEdge(edge[:splitAt], edge[splitAt:], keyPathRest, keyPath, newLeaf)
}

// Get returns value stored by provided key.
func (t *BinPatriciaTrie) Get(key []byte) ([]byte, bool) {
	keyPath := newBitstring(key)
	if t.root == nil {
		return nil, false
	}

	edge, keyPathRest, _, latest := t.root.followPath(keyPath)
	if len(edge) == 0 && len(keyPathRest) == 0 {
		return latest.Value, true
	}
	return nil, false
}

func (t *BinPatriciaTrie) RootHash() ([]byte, error) {
	if t.root == nil {
		return EmptyRootHash, nil
	}
	return t.hash(t.root, t.root.Key, 0), nil
}

func (t *BinPatriciaTrie) ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (branchNodeUpdates map[string][]byte, err error) {
	branchNodeUpdates = make(map[string][]byte)
	for i, update := range updates {
		account := new(Account)
		node, found := t.Get(hashedKeys[i]) // check if key exist
		if found {
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
			if t.trace {
				fmt.Printf("update of type %d has been skipped: unsupported for bin trie", update.Flags)
			}
			continue

		}
		aux := make([]byte, 128)
		n := account.encode(aux)

		t.UpdateHahsed(plainKeys[i], hashedKeys[i], aux[:n])
	}

	return branchNodeUpdates, nil
}

func (t *BinPatriciaTrie) Reset() { t.root = nil }

func (t *BinPatriciaTrie) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) error,
	storageFn func(plainKey []byte, cell *Cell) error,
) {
}

func (t *BinPatriciaTrie) SetTrace(b bool) {
	t.trace = b
}

// There are three types of nodes:
// - Leaf (with a value and without branches)
// - Branch (with left and right child)
// - Root - Either leaf or branch. When root is branch, it's Key contains their common prefix as a bitstring.
type Node struct {
	L, R, P *Node     // left and right child, parent. For root P is nil
	LPrefix bitstring // left child prefix, always begins with 0
	RPrefix bitstring // right child prefix, always begins with 1
	hash    []byte    // node hash
	Key     []byte    // same as common prefix, useful for debugging, actual key should be reconstructed by path to the node
	Value   []byte    // exists only in LEAF node
}

func (n *Node) splitEdge(commonPath, detachedPath, restKeyPath, fullPath bitstring, newLeaf *Node) {
	var movedNode *Node
	switch {
	case n.Value == nil:
		movedNode = &Node{ // move existed branch
			L:       n.L,
			R:       n.R,
			P:       n,
			LPrefix: n.LPrefix,
			RPrefix: n.RPrefix,
			hash:    n.hash,
		}
	default:
		movedNode = &Node{ // move existed leaf
			P:     n,
			Key:   n.Key,
			Value: n.Value,
		}
	}

	switch restKeyPath[0] {
	case 0:
		n.LPrefix, n.L = restKeyPath, newLeaf
		n.RPrefix, n.R = detachedPath, movedNode
	case 1:
		n.LPrefix, n.L = detachedPath, movedNode
		n.RPrefix, n.R = restKeyPath, newLeaf
	}

	// node become extented, reset key and value
	n.Key, n.Value, n.hash = nil, nil, nil
	if n.P == nil {
		n.Key = commonPath // root Key stores common prefix for L and R branches
		return
	}

	if len(commonPath) > 0 {
		switch commonPath[0] {
		case 1:
			if n.P != nil {
				n.P.RPrefix = commonPath
				return
			}
			n.RPrefix = commonPath
		case 0:
			if n.P != nil {
				n.P.LPrefix = commonPath
				return
			}
			n.LPrefix = commonPath
		}
	}
}

// followPath goes by provided path and exits when
//  - node path splits with desired path
//  - desired path is not finished but node path is finished
func (n *Node) followPath(path bitstring) (nodePath, pathRest bitstring, splitAt int, current *Node) {
	if n.P == nil { // it's root
		if n.Value != nil {
			nodePath = newBitstring(n.Key) // this key is not stored as bitstring
		} else {
			nodePath = n.Key // this key is stored as bitstring
		}
	}

	current = n
	var bit uint8
	var equal bool
	for current != nil {
		splitAt, bit, equal = nodePath.splitPoint(path)
		if equal {
			return bitstring{}, bitstring{}, 0, current
		}

		if splitAt < len(nodePath) {
			return nodePath, path[splitAt:], splitAt, current
		}

		if splitAt == 0 || splitAt == len(nodePath) {
			path = path[splitAt:]

			switch bit {
			case 1:
				if current.R == nil {
					return nodePath, path, splitAt, current
				}
				nodePath = current.RPrefix
				current = current.R
			case 0:
				if current.L == nil {
					return nodePath, path, splitAt, current
				}
				nodePath = current.LPrefix
				current = current.L
			}

			continue
		}
		break
	}
	return nodePath, path, splitAt, current
}

func (t *BinPatriciaTrie) hash(n *Node, pref bitstring, off int) []byte {
	t.keccak.Reset()

	var hash []byte
	if n.Value == nil {
		// This is a branch node, so the rule is
		// branch_hash = hash(left_root_hash || right_root_hash)
		lh := t.hash(n.L, n.LPrefix, off+len(pref))
		rh := t.hash(n.R, n.RPrefix, off+len(pref))
		t.keccak.Write(lh)
		t.keccak.Write(rh)
		hash = t.keccak.Sum(nil)
		if t.trace {
			fmt.Printf("branch %v (%v|%v)\n", hex.EncodeToString(hash), hex.EncodeToString(lh), hex.EncodeToString(rh))
		}
		t.keccak.Reset()
	} else {
		// This is a leaf node, so the hashing rule is
		// leaf_hash = hash(hash(key) || hash(leaf_value))
		t.keccak.Write(n.Key)
		kh := t.keccak.Sum(nil)
		t.keccak.Reset()

		t.keccak.Write(n.Value)
		hash = t.keccak.Sum(nil)
		t.keccak.Reset()

		t.keccak.Write(kh)
		t.keccak.Write(hash)
		hash = t.keccak.Sum(nil)
		t.keccak.Reset()

		if t.trace {
			fmt.Printf("leaf   %v\n", hex.EncodeToString(hash))
		}
	}

	if len(pref) > 1 {
		fpLen := len(pref) + off
		t.keccak.Write([]byte{byte(fpLen), byte(fpLen >> 8)})
		t.keccak.Write(zero30)
		t.keccak.Write(hash)

		hash = t.keccak.Sum(nil)
		t.keccak.Reset()
	}
	if t.trace {
		fmt.Printf("hash   %v off %d, pref %d\n", hex.EncodeToString(hash), off, len(pref))
	}
	n.hash = hash

	return hash
}

var zero30 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

type bitstring []uint8

func newBitstring(key []byte) bitstring {
	bits := make([]byte, 8*len(key))
	for i := range bits {

		if key[i/8]&(1<<(7-i%8)) == 0 {
			bits[i] = 0
		} else {
			bits[i] = 1
		}
	}

	return bits
}

func (b bitstring) String() string {
	var s string
	for _, bit := range b {
		switch bit {
		case 1:
			s += "1"
		case 0:
			s += "0"
		default:
			panic(fmt.Errorf("invalid bit %d in bitstring", bit))

		}
	}
	return s
}

func (b bitstring) splitPoint(other bitstring) (at int, bit byte, equal bool) {
	for ; at < len(b) && at < len(other); at++ {
		if b[at] != other[at] {
			return at, other[at], false
		}
	}

	switch {
	case len(b) == len(other):
		return 0, 0, true
	case at == len(b): // b ends before other
		return at, other[at], false
	case at == len(other): // other ends before b
		return at, b[at], false
	default:
		panic("oroo")
	}
}
