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
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
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
	activeRows     int
	selectedPrefix [128]byte // Key used to load the last active row
	// Length of the key that reflects current positioning of the grid. It maybe larger than number of active rows,
	// if a account leaf cell represents multiple nibbles in the key
	currentKeyLen int
	currentKey    [128]byte   // For each row indicates which column is currently selected
	nonEmptyCols  [128]uint16 // For each row, bitmap of non-empty columns
	// Function used to load branch node and fill up the cells
	// For each cell, it sets the cell type, clears the modified flag, fills the hash,
	// and for the extension, account, and leaf type, the `l` and `k`
	branchFn func(prefix []byte, row []Cell) error
	// Function used to fetch account with given plain key. It loads
	accountFn func(plainKey []byte, account *AccountDecorator) error
	// Function used to fetch account with given plain key
	storageFn func(plainKey []byte, storage []byte) error
}

type CellType byte

const (
	EMPTY_CELL CellType = iota
	BRANCH_CELL
	ACCOUNT_CELL
	ACC_INTER_CELL // Intermediate cells that are part of a account leaf
	EXTENSION_CELL
	EXT_INTER_CELL // Intermediate cells that are part of extension
	STORAGE_CELL
	STR_INTER_CELL // Intermediate cells that are part of a storage leaf
	ACC_STR_CELL   // Cell with both account and storage attached to it
	EMBEDDED_CELL  // Embedded leaf (RLP representation is less than 32 bytes)
)

type CellMode byte

const (
	NONE_MODE CellMode = iota
	MODIFIED_MODE
	DELETED_MODE
	NEW_MODE
)

type Cell struct {
	h  [32]byte  // cell hash
	hk [128]byte // part of the hashed key (for the extension and leaf nodes), one byte contains one nibble (hex digit 0-15)
	hl byte      // length (for the hashed key part)
	pk [52]byte  // plain key
	pl byte      // length (for the plain key)
	t  CellType  // cell type
	m  CellMode  // mode - whether cell is modified or deleted or new
}

type AccountDecorator struct {
	Nonce    uint64
	Balance  uint256.Int
	CodeHash [32]byte // hash of the bytecode
}

type BranchNodePart struct {
	hashedKey       []byte // Key (composed of nibbles), excluding the first nibble (except for the root node)
	accountPlainKey []byte // Plain key to retrieve account if this entry is going to or through an account
	storagePlainKey []byte // Plain key to retrieve storage item if this entry is storage leaf
	// Hash of the node below if it is either:
	//     branch node (hashKey is empty) or
	//     extension node (hashedKey is not empty, but accountPlainKey and storagePlainKey are empty)
	hash []byte
}

// BranchNodeUpdate describes an update to branch node (or root node, which can thought as a degenerate case of a branch node)
type BranchNodeUpdate struct {
	partMask uint16 // Zero if this is a deletion, otherwise contains bit 1 for every part
	parts    []BranchNodePart
}

func (hph *HexPatriciaHashed) unfoldCell(c *Cell) error {
	fmt.Printf("unfoldCell: activeRows: %d\n", hph.activeRows)
	row := hph.activeRows
	var err error
	switch c.t {
	case EMPTY_CELL:
		for i := 0; i < 16; i++ {
			c1 := &hph.grid[row][i]
			c1.t = EMPTY_CELL
			c1.m = NONE_MODE
		}
	case BRANCH_CELL:
		if err = hph.branchFn(hph.selectedPrefix[:row], hph.grid[row][:]); err != nil {
			return err
		}
	case ACCOUNT_CELL, ACC_INTER_CELL:
		nibble := c.hk[0]
		if c.hl == 1 {
			// If there is only one nibble left in the key, unfold into account
			if err = hph.accountFn(c.pk[:], &hph.accounts[nibble]); err != nil {
				return err
			}
		} else {
			for i := byte(0); i < 16; i++ {
				c1 := &hph.grid[row][i]
				if i == nibble {
					c1.t = ACC_INTER_CELL
					copy(c1.pk[:], c.pk[:])
					c1.pl = c.pl
					copy(c1.hk[:], c.hk[1:])
					c1.hl = c.hl - 1
				} else {
					c1.t = EMPTY_CELL
				}
				c1.m = NONE_MODE
			}
		}
	case EXTENSION_CELL, EXT_INTER_CELL:
		nibble := c.hk[0]
		for i := byte(0); i < 16; i++ {
			c1 := &hph.grid[row][i]
			if i == nibble {
				if c.hl == 1 {
					c1.t = BRANCH_CELL
				} else {
					c1.t = EXT_INTER_CELL
					copy(c1.hk[:], c.hk[1:])
					c1.hl = c.hl - 1
				}
				copy(c1.h[:], c.h[:])
			} else {
				c1.t = EMPTY_CELL
			}
			c1.m = NONE_MODE
		}
	case STORAGE_CELL, STR_INTER_CELL, EMBEDDED_CELL:
		nibble := c.hk[0]
		if c.hl == 1 {
			// If there is only one nibble left in the key, unfold into storage
			if err = hph.storageFn(c.pk[:], hph.storages[nibble][:]); err != nil {
				return err
			}
		} else {
			for i := byte(0); i < 16; i++ {
				c1 := &hph.grid[row][i]
				if i == nibble {
					c1.t = STR_INTER_CELL
					copy(c1.pk[:], c.pk[:])
					c1.pl = c.pl
					copy(c1.hk[:], c.hk[1:])
					c1.hl = c.hl - 1
				} else {
					c1.t = EMPTY_CELL
				}
				c1.m = NONE_MODE
			}
		}
	}
	hph.activeRows++
	return nil
}

func (hph *HexPatriciaHashed) fold() (*BranchNodeUpdate, error) {
	fmt.Printf("fold: activeRows: %d\n", hph.activeRows)
	if hph.activeRows == 0 {
		//TODO - need to return BranchNodeUpdate for the root
		return nil, fmt.Errorf("cannot fold - no active rows")
	}
	// Move information to the row above
	row := hph.activeRows - 1
	var upCell *Cell
	if row == 0 {
		upCell = &hph.root
	} else {
		upCell = &hph.grid[row-1][hph.selectedPrefix[row-1]]
	}
	// Examine the last active row and construct a branchNode update
	// We need to establish the following things:
	// 1. Number of non-empty cells that are not NEW_MODE. If > 1 then the branch node existed
	// 2. Number of non-empty cells that are not DELETED_MODE. If > 1 then branch node will exist aftee this update
	// If both numbers above are <= 1 no update is produced
	// If number of non new cells > 1 and number of non deleted > 1, then update is produced
	// If number of non new cells > 1 and number of non deleted <= 1, then delete is produced
	// If number of non new cells <=1 and number of non deleted > 1, then insert is produced
	var branchNodeUpdate BranchNodeUpdate
	var nonNewCount, nonDelCount int
	var modified bool
	colMask := uint16(1)
	for col := 0; col < 16; col++ {
		if hph.nonEmptyCols[row]&colMask != 0 {
			cell := &hph.grid[row][col]
			if cell.m != NEW_MODE {
				nonNewCount++
			}
			if cell.m != DELETED_MODE {
				nonDelCount++
			}
			if cell.m == MODIFIED_MODE || cell.m == NEW_MODE {
				modified = true
			}
		}
		colMask <<= 1
	}
	if nonNewCount == 0 {
		if nonDelCount == 0 {
			// Should not really happen, it probably means that duplicate keys were used
			panic("")
		} else {
			// All new
			upCell.m = NEW_MODE
		}
	} else {
		if nonDelCount == 0 {
			upCell.m = DELETED_MODE
		} else {
			if modified {
				upCell.m = MODIFIED_MODE
			} else {
				upCell.m = NONE_MODE
			}
		}
	}
	if nonDelCount > 1 {
		upCell.t = BRANCH_CELL
	}
	colMask = uint16(1)
	for col := 0; col < 16; col++ {
		if hph.nonEmptyCols[row]&colMask != 0 {
			cell := &hph.grid[row][col]
			if cell.m != DELETED_MODE {
				n := len(branchNodeUpdate.parts)
				branchNodeUpdate.parts = append(branchNodeUpdate.parts, BranchNodePart{})
				part := &branchNodeUpdate.parts[n]
				switch cell.t {
				case BRANCH_CELL:
					part.hash = common.Copy(cell.h[:])
					switch nonDelCount {
					case 0:
					case 1:
						upCell.t = EXTENSION_CELL
						upCell.hk[0] = byte(col)
						upCell.hl = 1
						copy(upCell.h[:], cell.h[:])
					default:
						// TODO add hash of this cell to the RLP of the upCell
					}
				case ACCOUNT_CELL, ACC_INTER_CELL:
					part.hashedKey = common.Copy(cell.hk[:cell.hl])
					part.accountPlainKey = common.Copy(cell.pk[:20])
					switch nonDelCount {
					case 0:
					case 1:
						upCell.t = ACC_INTER_CELL
						upCell.hk[0] = byte(col)
						copy(upCell.hk[1:], cell.hk[:cell.hl])
						upCell.hl = cell.hl + 1
					default:
						// TODO add hash of this cell to the RLP of the upCell
					}
				case EXTENSION_CELL, EXT_INTER_CELL:
					part.hashedKey = common.Copy(cell.hk[:cell.hl])
					part.hash = common.Copy(cell.h[:])
					switch nonDelCount {
					case 0:
					case 1:
						upCell.t = EXT_INTER_CELL
						upCell.hk[0] = byte(col)
						copy(upCell.hk[1:], cell.hk[:cell.hl])
						upCell.hl = cell.hl + 1
					default:
						// TODO add hash of this cell to the RLP of the upCell
					}
				case STORAGE_CELL, STR_INTER_CELL, EMBEDDED_CELL:
					part.hashedKey = common.Copy(cell.hk[:cell.hl])
					part.storagePlainKey = common.Copy(cell.pk[:])
					switch nonDelCount {
					case 0:
					case 1:
						// TODO: how do we detect folding via account
						upCell.t = STR_INTER_CELL
						upCell.hk[0] = byte(col)
						copy(upCell.hk[1:], cell.hk[:cell.hl])
						upCell.hl = cell.hl + 1
					default:
						// TODO add hash of this cell to the RLP of the upCell
					}
				case ACC_STR_CELL:
					part.hashedKey = common.Copy(cell.hk[:cell.hl])
					part.accountPlainKey = common.Copy(cell.pk[:20])
					part.storagePlainKey = common.Copy(cell.pk[:])
					switch nonDelCount {
					case 0:
					case 1:
						upCell.t = ACC_STR_CELL
						upCell.hk[0] = byte(col)
						copy(upCell.hk[1:], cell.hk[:cell.hl])
						upCell.hl = cell.hl + 1
					default:
						// TODO add hash of this cell to the RLP of the upCell
					}
				}
			}
		}
		colMask <<= 1
	}
	if nonNewCount <= 1 && nonDelCount <= 1 {
		// No update
		return nil, nil
	}
	return &branchNodeUpdate, nil
}

// emptyTip return true if the next nibble of the key is "pointing" to an empty cell
func (hph HexPatriciaHashed) emptyTip(key []byte) bool {
	fmt.Printf("emptyTip: activeRows %d, currentKeyLen %d\n", hph.activeRows, hph.currentKeyLen)
	if hph.activeRows == 0 {
		return hph.root.t == EMPTY_CELL
	}
	return hph.grid[hph.activeRows-1][key[hph.currentKeyLen-1]].t == EMPTY_CELL
}

func (hph *HexPatriciaHashed) deleteCell(k []byte) {
}

func (hph *HexPatriciaHashed) updateBalance(k []byte, balance *uint256.Int) {

}

func (hph *HexPatriciaHashed) updateCode(k []byte, codeHash []byte) {

}

func (hph *HexPatriciaHashed) updateNonce(k []byte, nonce uint64) {

}

func (hph *HexPatriciaHashed) updateStorage(k []byte, value []byte) {

}
