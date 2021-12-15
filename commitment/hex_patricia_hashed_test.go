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
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
)

// In memory commitment and state to use with the tests
type MockState struct {
}

func (ms MockState) branchFn(prefix []byte, row []Cell) error {
	return nil
}

func (ms MockState) accountFn(plainKey []byte, account *AccountDecorator) error {
	return nil
}

func (ms MockState) storageFn(plainKey []byte, storage []byte) error {
	return nil
}

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

// UpdateBuilder collects updates to the state
// and provides them in properly sorted form
type UpdateBuilder struct {
	balances   map[string]*uint256.Int
	nonces     map[string]uint64
	codeHashes map[string][32]byte
	storages   map[string][]byte
	deletes    map[string]struct{}
	keyset     map[string]struct{}
}

func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{
		balances:   make(map[string]*uint256.Int),
		nonces:     make(map[string]uint64),
		codeHashes: make(map[string][32]byte),
		storages:   make(map[string][]byte),
		deletes:    make(map[string]struct{}),
		keyset:     make(map[string]struct{}),
	}
}

func (ub *UpdateBuilder) Balance(addrHash string, balance uint64) *UpdateBuilder {
	sk := string(decodeHex(addrHash))
	delete(ub.deletes, sk)
	ub.balances[sk] = uint256.NewInt(balance)
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Nonce(addrHash string, nonce uint64) *UpdateBuilder {
	sk := string(decodeHex(addrHash))
	delete(ub.deletes, sk)
	ub.nonces[sk] = nonce
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) CodeHash(addrHash string, hash [32]byte) *UpdateBuilder {
	sk := string(decodeHex(addrHash))
	delete(ub.deletes, sk)
	ub.codeHashes[sk] = hash
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Storage(key string, value []byte) *UpdateBuilder {
	sk := string(decodeHex(key))
	delete(ub.deletes, sk)
	ub.storages[sk] = common.Copy(value)
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Delete(key string) *UpdateBuilder {
	sk := string(decodeHex(key))
	delete(ub.balances, sk)
	delete(ub.nonces, sk)
	delete(ub.codeHashes, sk)
	delete(ub.storages, sk)
	ub.deletes[sk] = struct{}{}
	ub.keyset[sk] = struct{}{}
	return ub
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

// Build
func (ub *UpdateBuilder) Build() ([][]byte, []Update) {
	var keys []string
	for key := range ub.keyset {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	returnKeys := make([][]byte, len(keys))
	returnUpdates := make([]Update, len(keys))
	for i, key := range keys {
		returnKeys[i] = []byte(key)
		u := &returnUpdates[i]
		if balance, ok := ub.balances[key]; ok {
			u.flags |= BALANCE_UPDATE
			u.balance.Set(balance)
		}
		if nonce, ok := ub.nonces[key]; ok {
			u.flags |= NONCE_UPDATE
			u.nonce = nonce
		}
		if codeHash, ok := ub.codeHashes[key]; ok {
			u.flags |= CODE_UPDATE
			copy(u.codeHashOrStorage[:], codeHash[:])
		}
		if storage, ok := ub.storages[key]; ok {
			u.flags |= STORAGE_UPDATE
			u.codeHashOrStorage = [32]byte{}
			copy(u.codeHashOrStorage[32-len(storage):], storage)
		}
	}
	return returnKeys, returnUpdates
}

func TestEmptyState(t *testing.T) {
	var ms MockState
	hph := &HexPatriciaHashed{
		branchFn:  ms.branchFn,
		accountFn: ms.accountFn,
		storageFn: ms.storageFn,
	}
	// addrHashes are 4 digits long
	keys, updates := NewUpdateBuilder().
		Balance("00000000", 4).
		Build()
	// Unfold the root onto the row 0 if it is not empty
	if hph.root.t != EMPTY_CELL {
		if err := hph.unfoldCell(&hph.root); err != nil {
			t.Error(err)
		}
	}
	var branchNodeUpdates []*BranchNodeUpdate
	for i, key := range keys {
		update := updates[i]
		fmt.Printf("key = [%x], update = %s\n", key, update)
		// Keep folding until the currentKey is the prefix of the key we modify
		for hph.currentKeyLen > 0 && !bytes.HasPrefix(key, hph.currentKey[:hph.currentKeyLen]) {
			if branchNodeUpdate, err := hph.fold(); err != nil {
				t.Error(err)
			} else if branchNodeUpdate != nil {
				branchNodeUpdates = append(branchNodeUpdates, branchNodeUpdate)
			}
		}
		// Now unfold until we step on an empty cell
		for !hph.emptyTip(key) && hph.currentKeyLen < len(key) {
			if err := hph.unfoldCell(&hph.grid[hph.activeRows-1][key[hph.currentKeyLen]]); err != nil {
				t.Error(err)
			}
		}
		// Update the cell
		if update.flags == DELETE_UPDATE {
			hph.deleteCell(key)
		} else {
			if update.flags&BALANCE_UPDATE != 0 {
				hph.updateBalance(key, &update.balance)
			}
			if update.flags&NONCE_UPDATE != 0 {
				hph.updateNonce(key, update.nonce)
			}
			if update.flags&CODE_UPDATE != 0 {
				hph.updateCode(key, update.codeHashOrStorage[:])
			}
			if update.flags&STORAGE_UPDATE != 0 {
				hph.updateStorage(key, update.codeHashOrStorage[:])
			}
		}
	}
	// Folding everything up to the root
	for hph.activeRows > 0 {
		if branchNodeUpdate, err := hph.fold(); err != nil {
			t.Error(err)
		} else {
			branchNodeUpdates = append(branchNodeUpdates, branchNodeUpdate)
		}
	}
}
