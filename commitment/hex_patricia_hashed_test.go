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
	"sort"
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

func (ub *UpdateBuilder) Balance(addr []byte, balance *uint256.Int) {
	sk := string(common.Copy(addr))
	delete(ub.deletes, sk)
	ub.balances[sk] = balance.Clone()
	ub.keyset[sk] = struct{}{}
}

func (ub *UpdateBuilder) Nonce(addr []byte, nonce uint64) {
	sk := string(common.Copy(addr))
	delete(ub.deletes, sk)
	ub.nonces[sk] = nonce
	ub.keyset[sk] = struct{}{}
}

func (ub *UpdateBuilder) CodeHash(addr []byte, hash [32]byte) {
	sk := string(common.Copy(addr))
	delete(ub.deletes, sk)
	ub.codeHashes[sk] = hash
	ub.keyset[sk] = struct{}{}
}

func (ub *UpdateBuilder) Storage(key []byte, value []byte) {
	sk := string(common.Copy(key))
	delete(ub.deletes, sk)
	ub.storages[sk] = common.Copy(value)
	ub.keyset[sk] = struct{}{}
}

func (ub *UpdateBuilder) Delete(key []byte) {
	sk := string(common.Copy(key))
	delete(ub.balances, sk)
	delete(ub.nonces, sk)
	delete(ub.codeHashes, sk)
	delete(ub.storages, sk)
	ub.deletes[sk] = struct{}{}
	ub.keyset[sk] = struct{}{}
}

type UpdateFlags uint8

const (
	BALANCE_UPDATE UpdateFlags = 1
	NONCE_UPDATE   UpdateFlags = 2
	CODE_UPDATE    UpdateFlags = 4
	STORAGE_UPDATE UpdateFlags = 8
)

type Update struct {
	flags             UpdateFlags
	balance           uint256.Int
	nonce             uint64
	codeHashOrStorage [32]byte
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
	if err := hph.unfoldCell(&hph.root, 0); err != nil {
		t.Error(err)
	}
}
