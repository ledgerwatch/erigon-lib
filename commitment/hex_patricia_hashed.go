/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package commitment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"math/bits"
	"strings"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"golang.org/x/crypto/sha3"
)

// HexPatriciaHashed implements commitment based on patricia merkle tree with radix 16,
// with keys pre-hashed by keccak256
type HexPatriciaHashed struct {
	root Cell // Root cell of the tree
	// Rows of the grid correspond to the level of depth in the patricia tree
	// Columns of the grid correspond to pointers to the nodes further from the root
	grid     [128][16]Cell        // First 64 rows of this grid are for account trie, and next 64 rows are for storage trie
	accounts [16]AccountDecorator // Account decorators that augument non-account cells in given column
	storages [16][32]byte         // Storage decorators that augument non-storage cells in given column
	// How many rows (starting from row 0) are currently active and have corresponding selected columns
	// Last active row does not have selected column
	activeRows int
	// Length of the key that reflects current positioning of the grid. It maybe larger than number of active rows,
	// if a account leaf cell represents multiple nibbles in the key
	currentKeyLen int
	currentKey    [128]byte // For each row indicates which column is currently selected
	rootBefore    bool
	rootMod       bool
	rootDel       bool
	beforeBitmap  [128]uint16 // For each row, bitmap of cells that were present before modification
	modBitmap     [128]uint16 // For each row, bitmap of cells that were modified (not deleted)
	delBitmap     [128]uint16 // For each row, bitmap of cells that were deleted
	// Function used to load branch node and fill up the cells
	// For each cell, it sets the cell type, clears the modified flag, fills the hash,
	// and for the extension, account, and leaf type, the `l` and `k`
	branchFn func(prefix []byte) (*BranchNodeUpdate, error)
	// Function used to fetch account with given plain key. It loads
	accountFn func(plainKey []byte, account *AccountDecorator) error
	// Function used to fetch account with given plain key
	storageFn func(plainKey []byte, storage []byte) error
	keccak    hash.Hash
}

func NewHexPatriciaHashed() *HexPatriciaHashed {
	return &HexPatriciaHashed{
		keccak: sha3.NewLegacyKeccak256(),
	}
}

type Cell struct {
	h             [32]byte  // cell hash
	hl            int       // Length of the hash (or embedded)
	hk            [128]byte // part of the hashed key (for the extension and leaf nodes), one byte contains one nibble (hex digit 0-15)
	hkl           int       // length (for the hashed key part)
	apk           [20]byte  // account plain key
	apl           int       // length of account plain key
	spk           [52]byte  // storage plain key
	spl           int       // length of the storage plain key
	accountKeyLen int
}

func (cell *Cell) fillEmpty() {
	cell.apl = 0
	cell.spl = 0
	cell.hkl = 0
	cell.hl = 0
}

func (cell *Cell) fillFromUpperCell(upCell *Cell) {
	cell.hkl = upCell.hkl - 1
	copy(cell.hk[:], upCell.hk[1:upCell.hkl])
	cell.apl = upCell.apl
	if upCell.apl > 0 {
		copy(cell.apk[:], upCell.apk[:cell.apl])
	}
	cell.spl = upCell.spl
	if upCell.spl > 0 {
		copy(cell.spk[:], upCell.spk[:upCell.spl])
	}
	cell.hl = upCell.hl
	if upCell.hl > 0 {
		copy(cell.h[:], upCell.h[:upCell.hl])
	}
}

func (cell *Cell) fillFromLowerCell(lowCell *Cell, nibble int) {
	cell.apl = lowCell.apl
	if lowCell.apl > 0 {
		copy(cell.apk[:], lowCell.apk[:cell.apl])
	}
	cell.spl = lowCell.spl
	if lowCell.spl > 0 {
		copy(cell.spk[:], lowCell.spk[:cell.spl])
	}
	cell.hkl = lowCell.hkl + 1
	cell.hk[0] = byte(nibble)
	copy(cell.hk[1:], lowCell.hk[:lowCell.hkl])
	cell.hl = lowCell.hl
	if lowCell.hl > 0 {
		copy(cell.h[:], lowCell.h[:lowCell.hl])
	}
	cell.accountKeyLen = lowCell.accountKeyLen
}

func (cell *Cell) fillFromPart(row int, keccak hash.Hash, part *BranchNodePart) {
	if len(part.accountPlainKey) > 0 {
		if row >= 64 {
			panic("")
		}
		cell.hkl = 64 - row
		keccak.Reset()
		keccak.Write([]byte(part.accountPlainKey))
		h := keccak.Sum(nil)
		k := 0
		for _, c := range h {
			if k >= row {
				cell.hk[k-row] = (c >> 4) & 0xf
			}
			k++
			if k >= row {
				cell.hk[k-row] = c & 0xf
			}
			k++
		}
		cell.apl = len(part.accountPlainKey)
		copy(cell.apk[:], part.accountPlainKey)
	}
	if len(part.storagePlainKey) > 0 {
		if row >= 64 {
			cell.hkl = 128 - row
		} else {
			cell.hkl += 64
		}
		keccak.Reset()
		keccak.Write([]byte(part.storagePlainKey[part.accountKeyLen:]))
		h := keccak.Sum(nil)
		k := 64
		for _, c := range h {
			if k >= row {
				cell.hk[k-row] = (c >> 4) & 0xf
			}
			k++
			if k >= row {
				cell.hk[k-row] = c & 0xf
			}
			k++
		}
		cell.spl = len(part.storagePlainKey)
		copy(cell.spk[:], part.storagePlainKey)
	}
	if len(part.hashedKey) > 0 {
		cell.hkl += len(part.hashedKey)
		copy(cell.hk[cell.hkl:], part.hashedKey)
	}
	if len(part.hash) > 0 {
		cell.hl = len(part.hash)
		copy(cell.h[:], part.hash)
	}
	cell.accountKeyLen = part.accountKeyLen
}

func (cell *Cell) computeHash(keccak hash.Hash, buffer []byte) []byte {
	// TODO implement proper hash calculation
	buffer = append(buffer, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...)
	return buffer
}

type AccountDecorator struct {
	Nonce    uint64
	Balance  uint256.Int
	CodeHash [32]byte // hash of the bytecode
}

type BranchNodePart struct {
	hashedKey       []byte // Key (composed of nibbles), excluding the first nibble (except for the root node)
	accountPlainKey []byte // Plain key to retrieve account. Alternatively, pointer to inside of a state file
	storagePlainKey []byte // Part of the plain key to retrieve storage item. Alternatively, pointer to inside of a state file
	accountKeyLen   int    // When storagePlainKey is present, this is the length of its first part (account)
	// Hash of the node below if it is either:
	//     branch node (hashKey is empty) or
	//     extension node (hashedKey is not empty, but accountPlainKey and storagePlainKey are empty)
	hash []byte
}

func (bnp *BranchNodePart) fillFromCell(cell *Cell) {
	if cell.apl > 0 {
		bnp.accountPlainKey = common.Copy(cell.apk[:cell.apl])
	}
	if cell.spl > 0 {
		bnp.storagePlainKey = common.Copy(cell.spk[:cell.spl])
	}
	if cell.hl > 0 {
		bnp.hash = common.Copy(cell.h[:cell.hl])
	}
	if cell.hkl > 0 {
		if cell.hl > 0 {
			bnp.hashedKey = common.Copy(cell.hk[:cell.hkl])
		}
	}
	bnp.accountKeyLen = cell.accountKeyLen
}

func (bnp BranchNodePart) String() string {
	var sb strings.Builder
	sb.WriteString("{")
	var comma string
	if len(bnp.hashedKey) > 0 {
		fmt.Fprintf(&sb, "hashedKey=[%x]", bnp.hashedKey)
		comma = ","
	}
	if len(bnp.accountPlainKey) > 0 {
		fmt.Fprintf(&sb, "%saccountPlainKey=[%x]", comma, bnp.accountPlainKey)
		comma = ","
	}
	if len(bnp.storagePlainKey) > 0 {
		fmt.Fprintf(&sb, "%sstoragePlainKey=[%x]", comma, bnp.storagePlainKey)
		comma = ","
		fmt.Fprintf(&sb, "%saccountKeyLen=%d", comma, bnp.accountKeyLen)
		comma = ","
	}
	if len(bnp.hash) > 0 {
		fmt.Fprintf(&sb, "%shash=[%x]", comma, bnp.hash)
		comma = ","
	}
	sb.WriteString("}")
	return sb.String()
}

func (bnp BranchNodePart) encode(buf []byte, numBuf []byte) []byte {
	n := binary.PutUvarint(numBuf, uint64(len(bnp.hashedKey)))
	buf = append(buf, numBuf[:n]...)
	if len(bnp.hashedKey) > 0 {
		buf = append(buf, bnp.hashedKey...)
	}
	n = binary.PutUvarint(numBuf, uint64(len(bnp.accountPlainKey)))
	buf = append(buf, numBuf[:n]...)
	if len(bnp.accountPlainKey) > 0 {
		buf = append(buf, bnp.accountPlainKey...)
	}
	n = binary.PutUvarint(numBuf, uint64(len(bnp.storagePlainKey)))
	buf = append(buf, numBuf[:n]...)
	if len(bnp.storagePlainKey) > 0 {
		buf = append(buf, bnp.storagePlainKey...)
		n = binary.PutUvarint(numBuf, uint64(bnp.accountKeyLen))
		buf = append(buf, numBuf[:n]...)
	}
	n = binary.PutUvarint(numBuf, uint64(len(bnp.hash)))
	buf = append(buf, numBuf[:n]...)
	if len(bnp.hash) > 0 {
		buf = append(buf, bnp.hash...)
	}
	return buf
}

func (bnp *BranchNodePart) decode(buf []byte, pos int) (int, error) {
	l, n := binary.Uvarint(buf[pos:])
	if n == 0 {
		return 0, fmt.Errorf("decode BranchNodePart: buffer too small for hashedKey len")
	} else if n < 0 {
		return 0, fmt.Errorf("decode BranchNodePart: val overlow for hashedKey len")
	}
	pos += n
	if len(buf) < pos+int(l) {
		return 0, fmt.Errorf("decode BranchNodePart: buffer too small for hashedKey")
	}
	if l > 0 {
		bnp.hashedKey = common.Copy(buf[pos : pos+int(l)])
		pos += int(l)
	}
	l, n = binary.Uvarint(buf[pos:])
	if n == 0 {
		return 0, fmt.Errorf("decode BranchNodePart: buffer too small for accountPlainKey len")
	} else if n < 0 {
		return 0, fmt.Errorf("decode BranchNodePart: val overlow for accountPlainKey len")
	}
	pos += n
	if len(buf) < pos+int(l) {
		return 0, fmt.Errorf("decode BranchNodePart: buffer too small for accountPlainKey")
	}
	if l > 0 {
		bnp.accountPlainKey = common.Copy(buf[pos : pos+int(l)])
		pos += int(l)
	}
	l, n = binary.Uvarint(buf[pos:])
	if n == 0 {
		return 0, fmt.Errorf("decode BranchNodePart: buffer too small for storagePlainKey len")
	} else if n < 0 {
		return 0, fmt.Errorf("decode BranchNodePart: val overlow for storagePlainKey len")
	}
	pos += n
	if len(buf) < pos+int(l) {
		return 0, fmt.Errorf("decode BranchNodePart: buffer too small for storagePlainKey")
	}
	if l > 0 {
		bnp.storagePlainKey = common.Copy(buf[pos : pos+int(l)])
		pos += int(l)
		l, n = binary.Uvarint(buf[pos:])
		if n == 0 {
			return 0, fmt.Errorf("decode BranchNodePart: buffer too small for accountKeyLen")
		} else if n < 0 {
			return 0, fmt.Errorf("decode BranchNodePart: val overlow for accountKeyLen")
		}
		bnp.accountKeyLen = int(l)
	}
	l, n = binary.Uvarint(buf[pos:])
	if n == 0 {
		return 0, fmt.Errorf("decode BranchNodePart: buffer too small for hash len")
	} else if n < 0 {
		return 0, fmt.Errorf("decode BranchNodePart: val overlow for hash len")
	}
	pos += n
	if len(buf) < pos+int(l) {
		return 0, fmt.Errorf("decode BranchNodePart: buffer too small for hash")
	}
	if l > 0 {
		bnp.hash = common.Copy(buf[pos : pos+int(l)])
		pos += int(l)
	}
	return pos, nil
}

// BranchNodeUpdate describes an update to branch node (or root node, which can thought as a degenerate case of a branch node)
type BranchNodeUpdate struct {
	modMask uint16           // Mask of modifications
	delMask uint16           // Mask of deletions
	mods    []BranchNodePart // Modifications
}

func (bnu BranchNodeUpdate) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "modMask %016b, delMask %016b\n", bnu.modMask, bnu.delMask)
	for bitset, j := bnu.modMask, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		fmt.Fprintf(&sb, "   %x => %s\n", nibble, bnu.mods[j])
		bitset ^= bit
	}
	return sb.String()
}

func (bnu BranchNodeUpdate) encode(buf []byte, numBuf []byte) []byte {
	binary.BigEndian.PutUint16(numBuf, bnu.modMask)
	buf = append(buf, numBuf...)
	binary.BigEndian.PutUint16(numBuf, bnu.delMask)
	buf = append(buf, numBuf...)
	// Loop iterating over the set bits of modMask
	for bitset, j := bnu.modMask, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		buf = bnu.mods[j].encode(buf, numBuf)
		bitset ^= bit
	}
	return buf
}

func (bnu *BranchNodeUpdate) decode(buf []byte, pos int) (int, error) {
	if len(buf) < pos+2 {
		return 0, fmt.Errorf("decode BranchNodeUpdate: buffer too small for modMask")
	}
	bnu.modMask = binary.BigEndian.Uint16(buf[pos:])
	pos += 2
	if len(buf) < pos+2 {
		return 0, fmt.Errorf("decode BranchNodeUpdate: buffer too small for delMask")
	}
	bnu.delMask = binary.BigEndian.Uint16(buf[pos:])
	pos += 2
	bnu.mods = make([]BranchNodePart, bits.OnesCount16(bnu.modMask))
	// Loop iterating over the set bits of partMask
	for bitset, j := bnu.modMask, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		var err error
		if pos, err = bnu.mods[j].decode(buf, pos); err != nil {
			return 0, fmt.Errorf("decode BranchNodeUpdate: part %d: %w", j, err)
		}
		bitset ^= bit
	}
	return pos, nil
}

func (hph *HexPatriciaHashed) unfold(hashedKey []byte) error {
	fmt.Printf("unfold: activeRows: %d\n", hph.activeRows)
	var upCell *Cell
	row := hph.activeRows
	var before, modified bool
	if hph.activeRows == 0 {
		if hph.root.hl == 0 && hph.root.hkl == 0 {
			// No unfolding for empty root
			return nil
		}
		upCell = &hph.root
		before = hph.rootBefore
		modified = hph.rootMod
	} else {
		col := hashedKey[hph.currentKeyLen]
		upCell = &hph.grid[hph.activeRows-1][col]
		before = hph.beforeBitmap[hph.activeRows-1]&(uint16(1)<<col) != 0
		modified = hph.modBitmap[hph.activeRows-1]&(uint16(1)<<col) != 0
	}
	for i := 0; i < 16; i++ {
		hph.grid[row][i].fillEmpty()
	}
	hph.beforeBitmap[row] = 0
	hph.modBitmap[row] = 0
	hph.delBitmap[row] = 0
	var err error
	var branchNodeUpdate *BranchNodeUpdate
	if upCell.hkl == 0 {
		if branchNodeUpdate, err = hph.branchFn(hph.currentKey[:row]); err != nil {
			return err
		}
		hph.beforeBitmap[row] = branchNodeUpdate.modMask
		// Loop iterating over the set bits of modMask
		for bitset, j := branchNodeUpdate.modMask, 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			cell := &hph.grid[row][nibble]
			part := &branchNodeUpdate.mods[j]
			cell.fillFromPart(row, hph.keccak, part)
			if cell.apl > 0 && cell.hkl == 0 {
				if err = hph.accountFn(cell.apk[:cell.apl], &hph.accounts[nibble]); err != nil {
					return nil
				}
			}
			if cell.spl > 0 && cell.hkl == 0 {
				if err = hph.storageFn(cell.spk[:cell.spl], hph.storages[nibble][:]); err != nil {
					return nil
				}
			}
			bitset ^= bit
		}
	} else {
		nibble := upCell.hk[0]
		if before {
			hph.beforeBitmap[row] = uint16(1) << nibble
		}
		if modified {
			hph.modBitmap[row] = uint16(1) << nibble
		}
		cell := &hph.grid[row][nibble]
		cell.fillFromUpperCell(upCell)
		if cell.apl > 0 && cell.hkl == 0 {
			if err = hph.accountFn(cell.apk[:cell.apl], &hph.accounts[nibble]); err != nil {
				return nil
			}
		}
		if cell.spl > 0 && cell.hkl == 0 {
			if err = hph.storageFn(cell.spk[:cell.spl], hph.storages[nibble][:]); err != nil {
				return nil
			}
		}
	}
	if hph.activeRows > 0 {
		hph.currentKey[hph.currentKeyLen] = hashedKey[hph.currentKeyLen]
		hph.currentKeyLen++
	}
	hph.activeRows++
	return nil
}

func (hph *HexPatriciaHashed) foldRoot() (*BranchNodeUpdate, error) {
	fmt.Printf("foldRoot: activeRows: %d\n", hph.activeRows)
	if hph.activeRows != 0 {
		return nil, fmt.Errorf("cannot fold root - there are still active rows: %d", hph.activeRows)
	}
	var branchNodeUpdate BranchNodeUpdate
	if hph.root.hkl == 0 {
		return nil, nil
	}
	branchNodeUpdate.modMask = uint16(1)
	branchNodeUpdate.mods = append(branchNodeUpdate.mods, BranchNodePart{})
	branchNodeUpdate.mods[0].fillFromCell(&hph.root)
	return &branchNodeUpdate, nil
}

func (hph *HexPatriciaHashed) fold() (*BranchNodeUpdate, []byte, error) {
	updateKey := common.Copy(hph.currentKey[:hph.currentKeyLen])
	if hph.activeRows == 0 {
		return nil, nil, fmt.Errorf("cannot fold - no active rows")
	}
	fmt.Printf("fold: activeRows: %d, modBitmap: %016b, delBitmap: %016b\n", hph.activeRows, hph.modBitmap[hph.activeRows-1], hph.delBitmap[hph.activeRows-1])
	// Move information to the row above
	row := hph.activeRows - 1
	var upCell *Cell
	var col int
	if row == 0 {
		fmt.Printf("upcell is root\n")
		upCell = &hph.root
	} else {
		col = int(hph.currentKey[row-1])
		fmt.Printf("upcell is (%d x %x)\n", row-1, col)
		upCell = &hph.grid[row-1][col]
	}
	branchNodeUpdate := &BranchNodeUpdate{}
	bitmap := (hph.beforeBitmap[row] | hph.modBitmap[row]) ^ hph.delBitmap[row]
	switch bits.OnesCount16(bitmap) {
	case 0:
		// Everything deleted
		if hph.delBitmap[row] != 0 {
			if row == 0 {
				hph.rootDel = true
			} else {
				hph.delBitmap[row-1] |= (uint16(1) << col)
			}
		}
		upCell.apl = 0
		upCell.spl = 0
		upCell.hkl = 0
		if bits.OnesCount16(hph.beforeBitmap[row]) <= 1 {
			// No update
			branchNodeUpdate = nil
		}
	case 1:
		// Leaf or extension node
		if hph.modBitmap[row] != 0 || hph.delBitmap[row] != 0 {
			// any modifications
			if row == 0 {
				hph.rootMod = true
			} else {
				hph.modBitmap[row-1] |= (uint16(1) << col)
			}
		}
		nibble := bits.TrailingZeros16(bitmap)
		cell := &hph.grid[row][nibble]
		upCell.fillFromLowerCell(cell, nibble)
		if bits.OnesCount16(hph.beforeBitmap[row]) <= 1 {
			// No update
			branchNodeUpdate = nil
		}
	default:
		// Branch node
		if hph.modBitmap[row] != 0 || hph.delBitmap[row] != 0 {
			// any modifications
			if row == 0 {
				hph.rootMod = true
			} else {
				hph.modBitmap[row-1] |= (uint16(1) << col)
			}
		}
		upCell.hkl = 0
		branchNodeUpdate.delMask = hph.delBitmap[row]
		branchNodeUpdate.modMask = hph.modBitmap[row]
		branchNodeUpdate.mods = make([]BranchNodePart, branchNodeUpdate.modMask)
		for bitset, j := hph.modBitmap[row], 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			cell := &hph.grid[row][nibble]
			cell.computeHash(hph.keccak, nil)
			branchNodeUpdate.mods[j].fillFromCell(cell)
			bitset ^= bit
		}
		upCell.hkl = 0
		upCell.apl = 0
		upCell.spl = 0
		upCell.hl = 32
		// TODO insert proper hash calculation
	}
	hph.activeRows--
	if hph.currentKeyLen > 0 {
		hph.currentKeyLen--
	}
	if branchNodeUpdate != nil {
		fmt.Printf("fold: update key: %x\n", updateKey)
	}
	return branchNodeUpdate, updateKey, nil
}

// emptyTip return true if the next nibble of the key is "pointing" to an empty cell
func (hph HexPatriciaHashed) emptyTip(key []byte) bool {
	var cell *Cell
	if hph.activeRows == 0 {
		cell = &hph.root
	} else {
		cell = &hph.grid[hph.activeRows-1][key[hph.activeRows-1]]
	}
	fmt.Printf("emptyTip %t: activeRows %d, currentKeyLen %d\n", cell.hkl == 0 && cell.hl == 0, hph.activeRows, hph.currentKeyLen)
	return cell.hkl == 0 && cell.hl == 0
}

func (hph HexPatriciaHashed) matchingTip(key []byte) bool {
	var mt bool
	if hph.activeRows == 0 {
		mt = bytes.HasPrefix(key, hph.root.hk[:hph.root.hkl])
	} else {
		fmt.Printf("matching tip cell (%d, %x)\n", hph.activeRows-1, key[hph.activeRows-1])
		cell := &hph.grid[hph.activeRows-1][key[hph.activeRows-1]]
		mt = bytes.HasPrefix(key, hph.currentKey[:hph.currentKeyLen]) &&
			len(key) >= hph.activeRows &&
			bytes.HasPrefix(key[hph.activeRows:], cell.hk[:cell.hkl])
	}
	fmt.Printf("matchingTip %t: activeRows %d\n", mt, hph.activeRows)
	return mt
}

func (hph *HexPatriciaHashed) deleteCell(hashedKey []byte) {
	fmt.Printf("deleteCell, activeRows = %d\n", hph.activeRows)
	var row, col int
	var cell *Cell
	if hph.activeRows == 0 {
		// Remove the root
		cell = &hph.root
		hph.rootDel = true
	} else {
		row = hph.activeRows - 1
		col = int(hashedKey[row])
		cell = &hph.grid[row][col]
		hph.delBitmap[row] |= (uint16(1) << col)
		fmt.Printf("deleteCell setting (%d, %x)\n", row, col)
	}
	cell.fillEmpty()
}

func (hph *HexPatriciaHashed) updateAccount(plainKey, hashedKey []byte) *AccountDecorator {
	var row, col int
	var cell *Cell
	if hph.activeRows == 0 {
		// Update the root
		row = -1
		cell = &hph.root
		hph.rootMod = true
	} else {
		row = hph.activeRows - 1
		col = int(hashedKey[row])
		cell = &hph.grid[row][col]
		hph.modBitmap[row] |= (uint16(1) << col)
		fmt.Printf("updateAccount setting (%d, %x)\n", row, col)
	}
	copy(cell.hk[:], hashedKey[row+1:])
	cell.hkl = len(hashedKey) - row - 1
	cell.apl = len(plainKey)
	copy(cell.apk[:], plainKey)
	return &hph.accounts[col]
}

func (hph *HexPatriciaHashed) updateBalance(plainKey, hashedKey []byte, balance *uint256.Int) {
	fmt.Printf("updateBalance, activeRows = %d\n", hph.activeRows)
	account := hph.updateAccount(plainKey, hashedKey)
	account.Balance.Set(balance)
}

func (hph *HexPatriciaHashed) updateCode(plainKey, hashedKey []byte, codeHash []byte) {
	fmt.Printf("updateCode, activeRows = %d\n", hph.activeRows)
	account := hph.updateAccount(plainKey, hashedKey)
	copy(account.CodeHash[:], codeHash)
}

func (hph *HexPatriciaHashed) updateNonce(plainKey, hashedKey []byte, nonce uint64) {
	fmt.Printf("updateNonce, activeRows = %d\n", hph.activeRows)
	account := hph.updateAccount(plainKey, hashedKey)
	account.Nonce = nonce
}

// updateStorage assumes that value is 32 byte slice
func (hph *HexPatriciaHashed) updateStorage(plainKey []byte, hashedKey []byte, accountKeyLen int, value []byte) {
	fmt.Printf("updateStorage, activeRows = %d\n", hph.activeRows)
	var row, col int
	var cell *Cell
	if hph.activeRows == 0 {
		// Update the root
		row = -1
		cell = &hph.root
	} else {
		row = hph.activeRows - 1
		col = int(hashedKey[row])
		cell = &hph.grid[row][col]
		hph.modBitmap[row] |= (uint16(1) << col)
		fmt.Printf("updateStorage setting (%d, %x)\n", row, col)
	}
	copy(cell.hk[:], hashedKey[row+1:])
	cell.hkl = len(hashedKey) - row - 1
	copy(cell.spk[:], plainKey)
	cell.spl = len(plainKey)
	copy(hph.storages[col][:], value)
	cell.accountKeyLen = accountKeyLen
}

type UpdateFlags uint8

const (
	DELETE_UPDATE  UpdateFlags = 0
	BALANCE_UPDATE UpdateFlags = 1
	NONCE_UPDATE   UpdateFlags = 2
	CODE_UPDATE    UpdateFlags = 4
	STORAGE_UPDATE UpdateFlags = 8
)

func (uf UpdateFlags) String() string {
	var sb strings.Builder
	if uf == DELETE_UPDATE {
		sb.WriteString("Delete")
	} else {
		if uf&BALANCE_UPDATE != 0 {
			sb.WriteString("+Balance")
		}
		if uf&NONCE_UPDATE != 0 {
			sb.WriteString("+Nonce")
		}
		if uf&CODE_UPDATE != 0 {
			sb.WriteString("+Code")
		}
		if uf&STORAGE_UPDATE != 0 {
			sb.WriteString("+Storage")
		}
	}
	return sb.String()
}

type Update struct {
	flags             UpdateFlags
	balance           uint256.Int
	nonce             uint64
	codeHashOrStorage [32]byte
}

func (u Update) encode(buf []byte, numBuf []byte) []byte {
	buf = append(buf, byte(u.flags))
	if u.flags&BALANCE_UPDATE != 0 {
		buf = append(buf, byte(u.balance.ByteLen()))
		buf = append(buf, u.balance.Bytes()...)
	}
	if u.flags&NONCE_UPDATE != 0 {
		n := binary.PutUvarint(numBuf, u.nonce)
		buf = append(buf, numBuf[:n]...)
	}
	if u.flags&CODE_UPDATE != 0 {
		buf = append(buf, u.codeHashOrStorage[:]...)
	}
	if u.flags&STORAGE_UPDATE != 0 {
		buf = append(buf, u.codeHashOrStorage[:]...)
	}
	return buf
}

func (u *Update) decode(buf []byte, pos int) (int, error) {
	if len(buf) < pos+1 {
		return 0, fmt.Errorf("decode Update: buffer too small for flags")
	}
	u.flags = UpdateFlags(buf[pos])
	pos++
	if u.flags&BALANCE_UPDATE != 0 {
		if len(buf) < pos+1 {
			return 0, fmt.Errorf("decode Update: buffer too small for balance len")
		}
		balanceLen := int(buf[pos])
		pos++
		if len(buf) < pos+balanceLen {
			return 0, fmt.Errorf("decode Update: buffer too small for balance")
		}
		u.balance.SetBytes(buf[pos : pos+balanceLen])
		pos += balanceLen
	}
	if u.flags&NONCE_UPDATE != 0 {
		var n int
		u.nonce, n = binary.Uvarint(buf[pos:])
		if n == 0 {
			return 0, fmt.Errorf("decode Update: buffer too small for nonce")
		}
		if n < 0 {
			return 0, fmt.Errorf("decode Update: nonce overflow")
		}
		pos += n
	}
	if u.flags&CODE_UPDATE != 0 {
		if len(buf) < pos+32 {
			return 0, fmt.Errorf("decode Update: buffer too small for codeHash")
		}
		copy(u.codeHashOrStorage[:], buf[pos:pos+32])
		pos += 32
	}
	if u.flags&STORAGE_UPDATE != 0 {
		if len(buf) < pos+32 {
			return 0, fmt.Errorf("decode Update: buffer too small for storage")
		}
		copy(u.codeHashOrStorage[:], buf[pos:pos+32])
		pos += 32
	}
	return pos, nil
}

func (u Update) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Flags: [%s]", u.flags))
	if u.flags&BALANCE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", Balance: [%d]", &u.balance))
	}
	if u.flags&NONCE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", Nonce: [%d]", u.nonce))
	}
	if u.flags&CODE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", CodeHash: [%x]", u.codeHashOrStorage))
	}
	if u.flags&STORAGE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", Storage: [%x]", u.codeHashOrStorage))
	}
	return sb.String()
}

func (hph *HexPatriciaHashed) processUpdates(plainKeys, hashedKeys [][]byte, updates []Update, accountKeyLen int) (map[string]*BranchNodeUpdate, error) {
	branchNodeUpdates := make(map[string]*BranchNodeUpdate)
	for i, hashedKey := range hashedKeys {
		plainKey := plainKeys[i]
		update := updates[i]
		fmt.Printf("plainKey=[%x], hashedKey=[%x], update=%s\n", plainKey, hashedKey, update)
		// Keep folding until the currentKey is the prefix of the key we modify
		fmt.Printf("currentKey=[%x]\n", hph.currentKey[:hph.currentKeyLen])
		for hph.currentKeyLen > 0 && !bytes.HasPrefix(hashedKey, hph.currentKey[:hph.currentKeyLen]) {
			if branchNodeUpdate, updateKey, err := hph.fold(); err != nil {
				return nil, fmt.Errorf("fold: %w", err)
			} else if branchNodeUpdate != nil {
				branchNodeUpdates[string(updateKey)] = branchNodeUpdate
			}
		}
		// Now unfold until we step on an empty cell
		for !hph.emptyTip(hashedKey) && !hph.matchingTip(hashedKey) {
			if err := hph.unfold(hashedKey); err != nil {
				return nil, fmt.Errorf("unfold: %w", err)
			}
		}
		// Update the cell
		if update.flags == DELETE_UPDATE {
			hph.deleteCell(hashedKey)
		} else {
			if update.flags&BALANCE_UPDATE != 0 {
				hph.updateBalance(plainKey, hashedKey, &update.balance)
			}
			if update.flags&NONCE_UPDATE != 0 {
				hph.updateNonce(plainKey, hashedKey, update.nonce)
			}
			if update.flags&CODE_UPDATE != 0 {
				hph.updateCode(plainKey, hashedKey, update.codeHashOrStorage[:])
			}
			if update.flags&STORAGE_UPDATE != 0 {
				hph.updateStorage(plainKey, hashedKey, accountKeyLen, update.codeHashOrStorage[:])
			}
		}
	}
	// Folding everything up to the root
	if hph.activeRows == 0 {
		if branchNodeUpdate, err := hph.foldRoot(); err != nil {
			return nil, fmt.Errorf("foldRoot: %w", err)
		} else {
			branchNodeUpdates[""] = branchNodeUpdate
		}
	} else {
		for hph.activeRows > 0 {
			if branchNodeUpdate, updateKey, err := hph.fold(); err != nil {
				return nil, fmt.Errorf("final fold: %w", err)
			} else if branchNodeUpdate != nil {
				branchNodeUpdates[string(updateKey)] = branchNodeUpdate
			}
		}
	}
	return branchNodeUpdates, nil
}
