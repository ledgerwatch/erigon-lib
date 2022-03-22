package bin_patricia_trie

import (
	"bytes"
	"hash"

	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/commitment"
)

type Trie struct {
	root *Node

	keccak keccakState
}

type Node struct {
	Flag    NodeFlag
	L, R, P *Node
	LPrefix bitstring
	RPrefix bitstring
	hash    []byte
	Key     []byte // same as common prefix, useful for debugging, actual key should be reconstructed by path to the node
	Value   []byte // exists only in LEAF node
}

func (n *Node) encode() []byte {
	return nil
}

type NodeFlag uint8

const (
	NODE_FLAG_EMPTY     NodeFlag = 0
	NODE_FLAG_EXTENSION NodeFlag = 1 << iota
	NODE_FLAG_LEAF
)

func (n NodeFlag) IsEmpty() bool {
	return n == NODE_FLAG_EMPTY
}

func (n NodeFlag) IsExtension() bool {
	return n&NODE_FLAG_EXTENSION != 0
}

func (n NodeFlag) IsLeaf() bool {
	return n&NODE_FLAG_LEAF != 0
}

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

func Hash(hasher keccakState, n *Node) []byte {
	aux := new(bytes.Buffer)
	switch {
	case n == nil, n.Flag.IsEmpty():
		return commitment.EmptyRootHash
	case n.Flag.IsLeaf():
		// This is a leaf node, so the hashing rule is
		// leaf_hash = hash(hash(key) || hash(leaf_value))
		if n.Key == nil {
			panic("nil key in leaf node")
		}
		hasher.Write(n.Key)
		aux.Write(hasher.Sum(nil))
		hasher.Reset()

		if n.Value == nil {
			panic("nil value in leaf node")
		}
		hasher.Write(n.Value)
		aux.Write(hasher.Sum(nil))
		hasher.Reset()
	case n.Flag.IsExtension():
		// what to do with prefix?
	default:
		// This is a branch node, so the rule is
		// branch_hash = hash(left_root_hash || right_root_hash)
		if n.L != nil && n.L.hash != nil {
			aux.Write(n.L.hash)
		}
		if n.R != nil && n.R.hash != nil {
			aux.Write(n.R.hash)
		}
	}

	hasher.Write(aux.Bytes())
	hash := hasher.Sum(nil)
	hasher.Reset()
	return hash
}

func NewBinPatriciaTrie() *Trie {
	return &Trie{
		keccak: sha3.NewLegacyKeccak256().(keccakState),
	}
}

func (n *Node) followKey(bit uint8, keyRest bitstring, leafValueReturner func() (key, value []byte)) (*Node, bitstring) {
	switch bit {
	case 1:
		if n.R == nil {
			fullKey, leafValue := leafValueReturner()
			n.R = &Node{
				Flag:  NODE_FLAG_LEAF,
				P:     n,
				Key:   fullKey,
				Value: leafValue,
			}
			n.RPrefix = keyRest
			return nil, []byte{}
		}
		return n.R, n.RPrefix
	case 0:
		if n.L == nil {
			fullKey, leafValue := leafValueReturner()
			n.L = &Node{
				Flag:  NODE_FLAG_LEAF,
				P:     n,
				Key:   fullKey,
				Value: leafValue,
			}
			n.LPrefix = keyRest
			return nil, []byte{}
		}
		return n.L, n.LPrefix
	default:
		panic("invalid bit provided")
	}
}

func (n *Node) splitLeftEdge(splittedPrefix, detachedSuffix, restOfKey bitstring, leafValueReturner func() (key, value []byte)) {
	// create extension and presave children if there are both childs
	fullKey, leafValue := leafValueReturner()
	if n.Flag.IsExtension() {
		n.RPrefix, n.R = detachedSuffix, &Node{
			Flag:    NODE_FLAG_EXTENSION,
			L:       n.L,
			R:       n.R,
			P:       n,
			LPrefix: n.LPrefix,
			RPrefix: n.RPrefix,
			hash:    n.hash,
		}

		n.LPrefix, n.L = restOfKey, &Node{
			Flag:  NODE_FLAG_LEAF,
			P:     n,
			Key:   fullKey,
			Value: leafValue,
		}
		if n.P != nil {
			n.P.LPrefix = splittedPrefix
		}
		return
	}

	// that's a leaf, split on splitting point, set inserted key to other n
	ext := &Node{
		Flag:    NODE_FLAG_EXTENSION,
		P:       n,
		LPrefix: restOfKey,
		L: &Node{
			Flag:  NODE_FLAG_LEAF,
			Key:   fullKey,
			Value: leafValue,
		},
		RPrefix: detachedSuffix,
		R: &Node{
			Flag:  NODE_FLAG_LEAF,
			Key:   n.Key,
			Value: n.Value,
		},
	}

	// update new childs parent pointers
	n.RPrefix, n.R = splittedPrefix, ext
	n.R.R.P, n.R.L.P = n.R, n.R
	// node become extented, reset key and value
	n.Flag, n.Key, n.Value = NODE_FLAG_EXTENSION, nil, nil
	// if node is not root, update prefix on edge R
	if n.P != nil {
		n.P.LPrefix = splittedPrefix
	}
}

func (n *Node) splitRightEdge(splittedPrefix, detachedSuffix, restOfKey bitstring, leafValueReturner func() (key, value []byte)) {
	// create extension and presave children if there are both childs
	fullKey, leafValue := leafValueReturner()
	if n.Flag.IsExtension() {
		n.LPrefix, n.L = detachedSuffix, &Node{
			//n.LPrefix, n.L = splittedPrefix, &Node{
			Flag:    NODE_FLAG_EXTENSION,
			L:       n.L,
			R:       n.R,
			P:       n,
			LPrefix: n.LPrefix,
			RPrefix: n.RPrefix,
			hash:    n.hash,
		}

		n.RPrefix, n.R = restOfKey, &Node{
			Flag:  NODE_FLAG_LEAF,
			P:     n,
			Key:   fullKey,
			Value: leafValue,
		}
		if n.P != nil {
			n.P.RPrefix = splittedPrefix
		}
		return
	}

	// that's a leaf, split on splitting point, set inserted key to other n
	ext := &Node{
		Flag:    NODE_FLAG_EXTENSION,
		P:       n,
		RPrefix: restOfKey,
		R: &Node{
			Flag:  NODE_FLAG_LEAF,
			Key:   fullKey,
			Value: leafValue,
		},
		LPrefix: detachedSuffix,
		L: &Node{
			Flag:  NODE_FLAG_LEAF,
			Key:   n.Key,
			Value: n.Value,
		},
	}

	// update new childs parent pointers
	n.LPrefix, n.L = splittedPrefix, ext
	n.L.R.P, n.L.L.P = n.L, n.L
	// node become extented, reset key and value
	n.Flag, n.Key, n.Value = NODE_FLAG_EXTENSION, nil, nil
	// if node is not root, update prefix on edge R
	if n.P != nil {
		n.P.LPrefix = splittedPrefix
	}
}

func (n *Node) child(keyChunk bitstring, leafValueReturner func() (key, value []byte)) (*Node, bitstring) {
	var nodeChunk bitstring
	if n.P == nil { // this node is root
		nodeChunk = newBitstring(n.Key)
	}

	child := n
	if len(nodeChunk) == 0 {
		child, nodeChunk = child.followKey(keyChunk[0], keyChunk, leafValueReturner)
		if child == nil {
			return nil, []byte{}
		}
	}

reeval:
	splitAt, bit, equal := nodeChunk.splitPoint(keyChunk)
	if equal {
		_, value := leafValueReturner()
		child.Value = value
		return nil, []byte{}
	}

	if splitAt == len(nodeChunk) {
		keyChunk = keyChunk[splitAt:]
		child, nodeChunk = child.followKey(bit, keyChunk, leafValueReturner)
		if child == nil {
			return nil, []byte{}
		}
		// FIXME(awskii): incorrect chunk returned which led to incorrect split
		goto reeval
	}

	if splitAt < len(nodeChunk) {
		if splitAt == 0 {
			splitAt++
		}

		// need extend edge labels
		splittedPrefix := nodeChunk[:splitAt]
		detachedSuffix := nodeChunk[splitAt:]
		keyChunk = keyChunk[splitAt:]

		switch bit {
		case 1: // new bit is 1, so previously it was 0, watch on left subtree
			child.splitRightEdge(splittedPrefix, detachedSuffix, keyChunk, leafValueReturner)
			// create extension and presave children if there are both childs
			return nil, []byte{}
		case 0:
			child.splitLeftEdge(splittedPrefix, detachedSuffix, keyChunk, leafValueReturner)
			return nil, []byte{}
		}
	}

	return child, keyChunk
}

func (t *Trie) Hash() []byte {
	if t.root.hash == nil {
		t.root.hash = Hash(t.keccak, t.root)
	}
	return t.root.hash
}

func (t *Trie) Insert(key, value []byte) {
	bkey := newBitstring(key)
	if t.root == nil {
		t.root = &Node{
			Flag:  NODE_FLAG_LEAF,
			Key:   key,
			Value: value,
		}
		return
	}

	values := func() (k, v []byte) {
		return key, value
	}

	keyChunk := bkey
	next := t.root
	for {
		next, keyChunk = next.child(keyChunk, values)
		if next == nil {
			break
		}
	}
}

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

func (b bitstring) splitPoint(other bitstring) (int, byte, bool) {
	eq := bytes.Equal(b[:], other[:])
	if eq {
		return 0, 0, true
	}
	for i, bi := range b {
		if bi != other[i] {
			return i, other[i], false
		}
	}
	if len(other) > len(b) && len(b) > 0 {
		return len(b), other[len(b)-1], false
	}
	return -1, 0, false
}
