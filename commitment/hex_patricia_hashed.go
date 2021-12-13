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

func (hph *HexPatriciaHashed) fold() error {
	fmt.Printf("fold: activeRows: %d\n", hph.activeRows)
	if hph.activeRows == 0 {
		return fmt.Errorf("cannot fold - no active rows")
	}
	return nil
}

func (hph HexPatriciaHashed) emptyTip() bool {
	fmt.Printf("emptyTip: activeRows %d, currentKeyLen %d\n", hph.activeRows, hph.currentKeyLen)
	if hph.activeRows == 0 {
		return hph.root.t == EMPTY_CELL
	}
	return hph.grid[hph.activeRows-1][hph.currentKey[hph.currentKeyLen-1]].t == EMPTY_CELL
}

func (hph *HexPatriciaHashed) Apply(k, v []byte) {

}
