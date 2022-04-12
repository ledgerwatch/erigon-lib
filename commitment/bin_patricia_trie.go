package commitment

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"
	"strings"

	"golang.org/x/crypto/sha3"
)

type BinPatriciaTrie struct {
	root       *Node
	rootPrefix bitstring
	trace      bool

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
		t.rootPrefix = keyPath
		return
	}

	if t.rootPrefix != nil {
		defer func() { t.rootPrefix = nil }() // and we clear it after first root split
	}

	edge, keyPathRest, splitAt, latest := t.root.followPath(keyPath, t.rootPrefix)
	if len(edge) == 0 && len(keyPathRest) == 0 {
		latest.Value = value
		return
	}

	newLeaf := &Node{P: latest, Key: key, Value: value}
	latest.splitEdge(edge[:splitAt], edge[splitAt:], keyPathRest, newLeaf)
}

// Key describes path in trie to value. When UpdateHashed is used,
// hashed key describes path to the leaf node and plainKey is stored in the leaf node Key field.
func (t *BinPatriciaTrie) UpdateHahsed(plainKey, hashedKey, value []byte) (string, []byte) {
	keyPath := newBitstring(hashedKey)
	if t.root == nil {
		t.root = &Node{
			Key:   plainKey,
			Value: value,
		}
		t.rootPrefix = keyPath // further root Key will be processed like common path, but that's plainKey
		return keyPath.String(), t.encodeUpdate(keyPath, 0, 0, t.root)
	}
	if t.rootPrefix != nil {
		defer func() { t.rootPrefix = nil }() // and we clear it after first root split
	}

	edge, keyPathRest, splitAt, latest := t.root.followPath(keyPath, t.rootPrefix)

	followedKey := keyPath[:len(keyPath)-len(keyPathRest)]
	before := latest.childMask()

	if len(edge) == 0 && len(keyPathRest) == 0 {
		if len(value) == 0 { // delete key
			if latest.P != nil {
				latest.P.deleteChild(latest)
			} else { // remove root
				t.root = nil
				t.rootPrefix = nil
			}
			return followedKey.String(), []byte{}
		}
		latest.Value = value

		return followedKey.String(), t.encodeUpdate(followedKey, before, before, latest)
	}

	newLeaf := &Node{P: latest, Key: plainKey, Value: value}
	latest.splitEdge(edge[:splitAt], edge[splitAt:], keyPathRest, newLeaf)

	after := latest.childMask()

	return followedKey.String(), t.encodeUpdate(followedKey, before, after, latest)
}

func (n *Node) deleteChild(child *Node) bitstring {
	var melt *Node
	var meltPrefix bitstring

	// remove child data
	switch child {
	case n.L:
		n.L, n.LPrefix = nil, nil
		melt = n.R
		meltPrefix = n.RPrefix
	case n.R:
		n.R, n.RPrefix = nil, nil
		melt = n.L
		meltPrefix = n.LPrefix
	default:
		panic("could delete only child nodes")
	}
	melt.P = n.P

	// merge parent path to skip this half-branch node
	if n.P != nil {
		switch {
		case n.P.L == n:
			n.P.L, n.P.LPrefix = melt, append(n.P.LPrefix, meltPrefix...)
		case n.P.R == n:
			n.P.R, n.P.RPrefix = melt, append(n.P.RPrefix, meltPrefix...)
		default:
			panic("failed to merge parent path")
		}
	}
	return meltPrefix
}

// Get returns value stored by provided key.
func (t *BinPatriciaTrie) Get(key []byte) ([]byte, bool) {
	keyPath := newBitstring(key)
	if t.root == nil {
		return nil, false
	}

	edge, keyPathRest, _, latest := t.root.followPath(keyPath, t.rootPrefix)
	if len(edge) == 0 && len(keyPathRest) == 0 {
		return latest.Value, true
	}
	return nil, false
}

func (t *BinPatriciaTrie) RootHash() ([]byte, error) {
	if t.root == nil {
		return EmptyRootHash, nil
	}
	if t.rootPrefix != nil {
		return t.hash(t.root, t.rootPrefix, 0), nil
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
		case update.Flags == DELETE_UPDATE:
			account.deleteNow = true
		case update.Flags&BALANCE_UPDATE != 0:
			account.Balance.Set(&update.Balance)
		case update.Flags&NONCE_UPDATE != 0:
			account.Nonce = update.Nonce
		case update.Flags&CODE_UPDATE != 0:
			copy(account.CodeHash, update.CodeHashOrStorage[:])
		default:
			if t.trace {
				fmt.Printf("update of type %d has been skipped: unsupported for bin trie", update.Flags)
			}
			continue
		}

		aux := make([]byte, 128)
		var n int
		if !account.deleteNow {
			n = account.encode(aux)
		}

		ukey, updbytes := t.UpdateHahsed(plainKeys[i], hashedKeys[i], aux[:n])
		branchNodeUpdates[ukey] = updbytes
		if updbytes != nil && t.trace {
			fmt.Printf("%q => %s\n", ukey, branchToString2(updbytes))
		}
	}

	return branchNodeUpdates, nil
}

func (t *BinPatriciaTrie) encodeUpdate(followedKey bitstring, before, after uint16, latest *Node) []byte {
	var bitmapBuf [4]byte
	binary.BigEndian.PutUint16(bitmapBuf[0:], before)
	binary.BigEndian.PutUint16(bitmapBuf[2:], after)

	branchData := make([]byte, 0)
	branchData = append(branchData, bitmapBuf[:]...)

	list := []*Node{latest, latest.L, latest.R}
	//keys := []string{string(followedKey), string(followedKey) + string(latest.LPrefix), string(followedKey) + string(latest.RPrefix)}

	var numBuf [binary.MaxVarintLen64]byte

	for _, node := range list {
		if node == nil {
			break
		}

		var fieldBits PartFlags

		aux := make([]byte, 0)
		if node.Value == nil {
			fieldBits = BRANCH_PART

			lenc, lpadding := node.LPrefix.reconstructHex()

			n := binary.PutUvarint(numBuf[:], uint64(len(lenc)))
			numBuf[n] = byte(lpadding)

			aux = append(aux, append(numBuf[:n+1], lenc...)...)

			renc, rpadding := node.RPrefix.reconstructHex()

			n = binary.PutUvarint(numBuf[:], uint64(len(renc)))
			numBuf[n] = byte(rpadding)

			aux = append(aux, append(numBuf[:n+1], renc...)...)

			if t.trace {
				fmt.Printf("encode BRANCH_PART LSize=%d|%d RSize=%d|%d\n", len(lenc), len(renc), lpadding, rpadding)
			}
		}

		if len(node.Key) > 0 && node.Value != nil {
			fieldBits = ACCOUNT_PLAIN_PART

			if t.trace {
				fmt.Printf("encode ACCOUNT_PLAIN_PART key_size=%d val_size=%d\n", len(node.Key), len(node.Value))
			}

			n := binary.PutUvarint(numBuf[:], uint64(len(node.Key)))
			aux = append(aux, append(numBuf[:n], node.Key...)...)
			n = binary.PutUvarint(numBuf[:], uint64(len(node.Value)))
			aux = append(aux, append(numBuf[:n], node.Value...)...)
		}

		branchData = append(branchData, byte(fieldBits))
		if len(aux) > 0 {
			branchData = append(branchData, aux...)
		} else {
			n := binary.PutUvarint(numBuf[:], 0)
			branchData = append(branchData, numBuf[:n]...)
		}
	}
	return branchData
}

func branchToString2(branchData []byte) string {
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4

	var sb strings.Builder
	fmt.Fprintf(&sb, "touchMap %016b, afterMap %016b\n", touchMap, afterMap)

	for i := 0; i < bits.OnesCount16(afterMap)+1; i++ {
		if pos >= len(branchData) {
			break
		}

		fieldBits := PartFlags(branchData[pos])
		pos++

		switch {
		case fieldBits == ACCOUNT_PLAIN_PART:
			sz, n := binary.Uvarint(branchData[pos:])
			pos += n
			size := int(sz)
			if size == 0 {
				continue
			}
			key := branchData[pos : pos+size]
			pos += size

			sz, n = binary.Uvarint(branchData[pos:])
			pos += n
			size = int(sz)

			value := branchData[pos : pos+size]
			pos += size

			sb.WriteString("{")

			acc := new(Account).decode(value)
			fmt.Fprintf(&sb, "accountPlainKey=[%x] -> %v", key, acc.String())
			sb.WriteString("}\n")
		case fieldBits == BRANCH_PART:
			sz, n := binary.Uvarint(branchData[pos:])
			pos += n
			size := int(sz)
			if size == 0 {
				continue
			}
			lpadding := int(branchData[pos])
			pos++

			lpref := branchData[pos : pos+size]
			pos += size

			lp := bitstringWithPadding(lpref, lpadding)

			sz, n = binary.Uvarint(branchData[pos:])
			pos += n
			size = int(sz)

			rpadding := int(branchData[pos])
			pos++

			rpref := branchData[pos : pos+size]
			pos += size
			rp := bitstringWithPadding(rpref, rpadding)

			sb.WriteString("{")
			fmt.Fprintf(&sb, "branch=\n\tL [%v]\n\tR [%v]", lp, rp)
			sb.WriteString("}\n")

		default:
			pos++
			continue
		}
	}
	return sb.String()
}

func (t *BinPatriciaTrie) Reset() { t.root = nil }

func (t *BinPatriciaTrie) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) error,
	storageFn func(plainKey []byte, cell *Cell) error,
) {
}

func (t *BinPatriciaTrie) Variant() TrieVariant { return VariantBinPatriciaTrie }

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

func (n *Node) childMask() uint16 {
	mask := uint16(0)
	//if latest.Value != nil {}
	if n.L != nil {
		mask |= 1 << 0
	}
	if n.R != nil {
		mask |= 1 << 1
	}
	return mask
}

func (n *Node) splitEdge(commonPath, detachedPath, restKeyPath bitstring, newLeaf *Node) {
	var movedNode *Node
	switch {
	case n.Value == nil:
		movedNode = &Node{ // move existed branch
			L:       n.L,
			R:       n.R,
			P:       n,
			LPrefix: n.LPrefix,
			RPrefix: n.RPrefix,
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
func (n *Node) followPath(path, rootPrefix bitstring) (nodePath, pathRest bitstring, splitAt int, current *Node) {
	if n.P == nil { // it's root
		if n.Value != nil {
			nodePath = rootPrefix // this key is not stored as bitstring
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

func bitstringWithPadding(key []byte, padding int) bitstring {
	bs := newBitstring(key)
	bs = bs[:len(bs)-padding]
	return bs
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

// Converts b into slice of bytes.
// if len of b is not a multiple of 8, we add 1 <= padding <= 7 zeros to the latest byte
// and return amount of added zeros
func (b bitstring) reconstructHex() (re []byte, padding int) {
	re = make([]byte, len(b)/8)

	var offt, i int
	for {
		bt, ok := b.readByte(offt)
		if !ok {
			break
		}
		re[i] = bt
		offt += 8
		i++
	}

	if offt >= len(b) {
		return re, 0
	}

	padding = offt + 8 - len(b)
	pd := append(b[offt:], bytes.Repeat([]byte{0}, padding)...)

	last, ok := pd.readByte(0)
	if !ok {
		panic(fmt.Errorf("reconstruct failed: padding %d padded size %d", padding, len(pd)))
	}
	return append(re, last), padding
}

func (b bitstring) readByte(offsetBits int) (byte, bool) {
	if len(b) <= offsetBits+7 {
		return 0, false
	}
	return b[offsetBits+7] | b[offsetBits+6]<<1 | b[offsetBits+5]<<2 | b[offsetBits+4]<<3 | b[offsetBits+3]<<4 | b[offsetBits+2]<<5 | b[offsetBits+1]<<6 | b[offsetBits]<<7, true
}

// ExtractPlainKeys parses branchData and extract the plain keys for accounts and storage in the same order
// they appear witjin the branchData
func ExtractBinPlainKeys(branchData []byte) (accountPlainKeys [][]byte, storagePlainKeys [][]byte, err error) {
	storagePlainKeys = make([][]byte, 0)
	accountPlainKeys = make([][]byte, 0)

	//touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4

	for i := 0; i < bits.OnesCount16(afterMap)+1; i++ {
		if pos >= len(branchData) {
			break
		}

		fieldBits := PartFlags(branchData[pos])
		pos++

		switch {
		case fieldBits == ACCOUNT_PLAIN_PART:
			// read plainkey, move pos
			sz, n := binary.Uvarint(branchData[pos:])
			switch {
			case n == 0:
				return nil, nil, fmt.Errorf("extractBinPlainKeys buffer too small for accountPlainKey")
			case n < 0:
				return nil, nil, fmt.Errorf("extractBinPlainKeys value overflow for accountPlainKey")
			default:
			}
			pos += n
			size := int(sz)
			if size == 0 {
				continue
			}
			plainKey := branchData[pos : pos+size]
			pos += size

			accountPlainKeys = append(accountPlainKeys, plainKey)

			// skip account encoded value, just move pos
			sz, n = binary.Uvarint(branchData[pos:])
			switch {
			case n == 0:
				return nil, nil, fmt.Errorf("extractBinPlainKeys buffer too small for accountValue")
			case n < 0:
				return nil, nil, fmt.Errorf("extractBinPlainKeys value overflow for accountValue")
			default:
			}

			pos += n + int(sz)
			//fmt.Printf("+ACCOUNT_PLAIN_PART key_size=%d val_size=%d\n", len(plainKey), len(value))
		case fieldBits == BRANCH_PART:
			// [ bytes
			//   0-10   : size
			//   11     : L padding
			//   12+size: LPrefix (hex encoded)
			//   same order for right branch
			// ]

			// here we just read sizes and move pos, without reading any values
			sz, n := binary.Uvarint(branchData[pos:])
			switch {
			case n == 0:
				return nil, nil, fmt.Errorf("extractBinPlainKeys buffer too small for branch left prefix size")
			case n < 0:
				return nil, nil, fmt.Errorf("extractBinPlainKeys value overflow for branch left prefix size")
			default:
			}

			pos += n
			size := int(sz)
			if size == 0 {
				continue
			}
			pos += size + 1 // LPrefix wrapped to slice of uint8 numbers + one byte stores amount of trailing zeros (padding)

			//fmt.Printf("!BRANCH l_size=%d vr_size=%d\n", size, len(branchData))
			if len(branchData) < pos {
				fmt.Printf("pos %d n %d sz %d\n\n%v", pos, n, sz, branchData)
			}

			sz, n = binary.Uvarint(branchData[pos:])
			switch {
			case n == 0:
				return nil, nil, fmt.Errorf("extractBinPlainKeys buffer too small for branch right prefix size")
			case n < 0:
				return nil, nil, fmt.Errorf("extractBinPlainKeys value overflow for branch right prefix size")
			default:
			}

			if pos+n+int(sz)+1 >= len(branchData) {
				fmt.Printf("extractBinPlainKeys buffer too small for right branch prefix l=%d p=%d\n", len(branchData), pos+n+int(sz)+1)
				return nil, nil, fmt.Errorf("extractBinPlainKeys buffer too small for right branch prefix")
			}

			pos += n + 1 + int(sz) // len of RPrefix + RPadding + RPrefix wrapped to slice of uint8 numbers
		default:
			pos++
			continue
		}
	}
	return
}

func ReplaceBinPlainKeys(branchData []byte, accountPlainKeys [][]byte, storagePlainKeys [][]byte, newData []byte) ([]byte, error) {
	var numBuf [binary.MaxVarintLen64]byte
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4
	newData = append(newData, branchData[:4]...)
	var accountI, storageI int
	_, _ = touchMap, storageI

	for i := 0; i < bits.OnesCount16(afterMap)+1; i++ {
		if pos >= len(branchData) {
			break
		}

		fieldBits := PartFlags(branchData[pos])
		newData = append(newData, byte(fieldBits))
		pos++

		switch {
		case fieldBits == ACCOUNT_PLAIN_PART:
			// read plainkey, move pos
			sz, n := binary.Uvarint(branchData[pos:])
			switch {
			case n == 0:
				return nil, fmt.Errorf("replaceBinPlainKeys buffer too small for accountPlainKey")
			case n < 0:
				return nil, fmt.Errorf("replaceBinPlainKeys value overflow for accountPlainKey")
			default:
			}
			pos += n
			size := int(sz)
			if size == 0 {
				continue
			}
			if len(branchData) < pos+size {
				return nil, fmt.Errorf("replaceBinPlainKeys buffer too small for accountPlainKey")
			}

			n = binary.PutUvarint(numBuf[:], uint64(len(accountPlainKeys[accountI])))
			newData = append(newData, numBuf[:n]...)
			newData = append(newData, accountPlainKeys[accountI]...)
			accountI++

			//plainKey := branchData[pos : pos+size]
			pos += size

			sz, n = binary.Uvarint(branchData[pos:])
			switch {
			case n == 0:
				return nil, fmt.Errorf("replaceBinPlainKeys buffer too small for accountValue")
			case n < 0:
				return nil, fmt.Errorf("replaceBinPlainKeys value overflow for accountValue")
			default:
			}
			size = int(sz)
			if len(branchData) < pos+size {
				return nil, fmt.Errorf("replaceBinPlainKeys buffer too small for accountValue")
			}
			newData = append(newData, branchData[pos:pos+n+size]...)

			pos += n + size
			//fmt.Printf("+ACCOUNT_PLAIN_PART key_size=%d val_size=%d\n", len(plainKey), len(value))
		case fieldBits == BRANCH_PART:
			// here we just read sizes and move pos, without reading any values
			sz, n := binary.Uvarint(branchData[pos:])
			switch {
			case n == 0:
				return nil, fmt.Errorf("extractBinPlainKeys buffer too small for branch len")
			case n < 0:
				return nil, fmt.Errorf("extractBinPlainKeys value overflow for branch len")
			case sz == 0:
				pos += n
				continue
			default:
			}

			size := int(sz)
			if len(branchData) < pos+n+1+size {
				fmt.Printf("\nbranch_size=%d wanted_pos=%d\n%s\n", len(branchData), pos+n+size+1, hex.EncodeToString(branchData))
				return newData, nil
			}

			newData = append(newData, branchData[pos:pos+n+1+size]...) // copy left prefix size + left padding + left prefix
			pos += n + 1 + size

			sz, n = binary.Uvarint(branchData[pos:])
			switch {
			case n == 0:
				return nil, fmt.Errorf("extractBinPlainKeys buffer too small for branch value")
			case n < 0:
				return nil, fmt.Errorf("extractBinPlainKeys value overflow for branch value")
			default:
			}
			size = int(sz)

			if len(branchData) < pos+n+1+size {
				fmt.Printf("\nbranch_size=%d wanted_pos=%d\n%s\n", len(branchData), pos+n+size+1, hex.EncodeToString(branchData))
				return newData, nil
			}

			newData = append(newData, branchData[pos:pos+n+1+size]...) // copy right prefix size + right padding + right prefix
			pos += n + 1 + size
		default:
			pos++
			continue
		}
	}
	return newData, nil
}
