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

// HexPatriciaHashed implements commitment based on patricia merkle tree with radix 16,
// with keys pre-hashed by keccak256
type HexPatriciaHashed struct {
	root Cell // Root cell of the tree
	// Rows of the grid correspond to the level of depth in the patricia tree
	// Columns of the grid correspond to pointers to the nodes further from the root
	grid [128][16]Cell
	// How many rows (starting from row 0) are currently active and have corresponding selected columns
	// Last active row does not have selected column
	activeRows   int
	selectedCols [128]int // For each row indicates which column is currently selected
}

type CellType int

const (
	EMPTY_CELL CellType = iota
	BRANCH_CELL
)

type Cell struct {
	t CellType // cell type
	h [32]byte // cell hash
	m bool     // modified
}

/*
Initial state - no active rows
From initial state, one can activate the row 0 by either initialising it with empty cells (for empty tree), or by loading root node.
Below is the function `initEmpty` that initialises row 0 for the empty tree
*/

func (hph *HexPatriciaHashed) initEmpty() {
	if hph.activeRows != 0 {
		return
	}
	hph.activeRows = 1
	for i := 0; i < 16; i++ {
		hph.grid[0][i].t = EMPTY_CELL
	}
}

/*
After that, a column in the last active row can be selected and the next row activated. If the selected cell is empty,
then the next row is also empty. If however it is not empty, corresponding node in the tree is loaded into the next row
*/
