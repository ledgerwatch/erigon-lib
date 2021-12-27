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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"golang.org/x/crypto/sha3"
)

// In memory commitment and state to use with the tests
type MockState struct {
	numBuf [binary.MaxVarintLen64]byte
	sm     map[string][]byte // backbone of the state
	cm     map[string][]byte // backbone of the commitments
}

func NewMockState() *MockState {
	return &MockState{
		sm: make(map[string][]byte),
		cm: make(map[string][]byte),
	}
}

func (ms MockState) branchFn(prefix []byte) (*BranchNodeUpdate, error) {
	if exBytes, ok := ms.cm[string(prefix)]; ok {
		var ex BranchNodeUpdate
		pos, err := ex.decode(exBytes, 0)
		if err != nil {
			return nil, fmt.Errorf("branchFn decode existing [%x], bytes: [%x]: %w", prefix, exBytes, err)
		}
		if pos != len(exBytes) {
			return nil, fmt.Errorf("branchFn key [%x] leftover bytes in [%x], comsumed %x", prefix, exBytes, pos)
		}
		return &ex, nil
	}
	return nil, nil
}

func (ms MockState) accountFn(plainKey []byte, account *AccountDecorator) error {
	exBytes, ok := ms.sm[string(plainKey)]
	if !ok {
		return fmt.Errorf("accountFn not found key [%x]", plainKey)
	}
	var ex Update
	pos, err := ex.decode(exBytes, 0)
	if err != nil {
		return fmt.Errorf("accountFn decode existing [%x], bytes: [%x]: %w", plainKey, exBytes, err)
	}
	if pos != len(exBytes) {
		return fmt.Errorf("accountFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
	}
	if ex.flags&STORAGE_UPDATE != 0 {
		return fmt.Errorf("accountFn reading storage item for key [%x]", plainKey)
	}
	if ex.flags&DELETE_UPDATE != 0 {
		return fmt.Errorf("accountFn reading deleted account for key [%x]", plainKey)
	}
	if ex.flags&BALANCE_UPDATE != 0 {
		account.Balance.Set(&ex.balance)
	} else {
		account.Balance.Clear()
	}
	if ex.flags&NONCE_UPDATE != 0 {
		account.Nonce = ex.nonce
	} else {
		account.Nonce = 0
	}
	if ex.flags&CODE_UPDATE != 0 {
		copy(account.CodeHash[:], ex.codeHashOrStorage[:])
	} else {
		account.CodeHash = [32]byte{}
	}
	return nil
}

func (ms MockState) storageFn(plainKey []byte, storage []byte) error {
	exBytes, ok := ms.sm[string(plainKey)]
	if !ok {
		return fmt.Errorf("storageFn not found key [%x]", plainKey)
	}
	var ex Update
	pos, err := ex.decode(exBytes, 0)
	if err != nil {
		return fmt.Errorf("storageFn decode existing [%x], bytes: [%x]: %w", plainKey, exBytes, err)
	}
	if pos != len(exBytes) {
		return fmt.Errorf("storageFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
	}
	if ex.flags&BALANCE_UPDATE != 0 {
		return fmt.Errorf("storageFn reading balance for key [%x]", plainKey)
	}
	if ex.flags&NONCE_UPDATE != 0 {
		return fmt.Errorf("storageFn reading nonce for key [%x]", plainKey)
	}
	if ex.flags&CODE_UPDATE != 0 {
		return fmt.Errorf("storageFn reading codeHash for key [%x]", plainKey)
	}
	if ex.flags&DELETE_UPDATE != 0 {
		return fmt.Errorf("storageFn reading deleted item for key [%x]", plainKey)
	}
	if ex.flags&STORAGE_UPDATE != 0 {
		copy(storage, ex.codeHashOrStorage[:])
	} else {
		for i := 0; i < 32; i++ {
			storage[i] = 0
		}
	}
	return nil
}

func (ms *MockState) applyPlainUpdates(plainKeys [][]byte, updates []Update) error {
	for i, key := range plainKeys {
		update := updates[i]
		if update.flags&DELETE_UPDATE != 0 {
			delete(ms.sm, string(key))
		} else {
			if exBytes, ok := ms.sm[string(key)]; ok {
				var ex Update
				pos, err := ex.decode(exBytes, 0)
				if err != nil {
					return fmt.Errorf("applyPlainUpdates decode existing [%x], bytes: [%x]: %w", key, exBytes, err)
				}
				if pos != len(exBytes) {
					return fmt.Errorf("applyPlainUpdates key [%x] leftover bytes in [%x], comsumed %x", key, exBytes, pos)
				}
				if update.flags&BALANCE_UPDATE != 0 {
					ex.flags |= BALANCE_UPDATE
					ex.balance.Set(&update.balance)
				}
				if update.flags&NONCE_UPDATE != 0 {
					ex.flags |= NONCE_UPDATE
					ex.nonce = update.nonce
				}
				if update.flags&CODE_UPDATE != 0 {
					ex.flags |= CODE_UPDATE
					copy(ex.codeHashOrStorage[:], update.codeHashOrStorage[:])
				}
				if update.flags&STORAGE_UPDATE != 0 {
					ex.flags |= STORAGE_UPDATE
					copy(ex.codeHashOrStorage[:], update.codeHashOrStorage[:])
				}
				ms.sm[string(key)] = ex.encode(nil, ms.numBuf[:])
			} else {
				ms.sm[string(key)] = update.encode(nil, ms.numBuf[:])
			}
		}
	}
	return nil
}

func (ms *MockState) applyBranchNodeUpdates(updates map[string]*BranchNodeUpdate) error {
	for key, update := range updates {
		if exBytes, ok := ms.cm[key]; ok {
			var ex BranchNodeUpdate
			pos, err := ex.decode(exBytes, 0)
			if err != nil {
				return fmt.Errorf("applyBranchNodeUpdates decode existing [%x], bytes: [%x]: %w", key, exBytes, err)
			}
			if pos != len(exBytes) {
				return fmt.Errorf("applyBranchNodeUpdates key [%x] leftover bytes in [%x], comsumed %x", key, exBytes, pos)
			}
			bitmap := (ex.modMask | update.modMask) ^ update.delMask
			if bitmap == 0 {
				delete(ms.cm, key)
			} else {
				var new BranchNodeUpdate
				new.modMask = bitmap
				new.mods = make([]BranchNodePart, bits.OnesCount16(bitmap))
				var exJ, upJ int
				for bitset, j := bitmap, 0; bitset != 0; j++ {
					bit := bitset & -bitset
					if update.modMask&bit == 0 {
						new.mods[j] = ex.mods[exJ]
					} else {
						new.mods[j] = update.mods[upJ]
						upJ++
					}
					if ex.modMask&bit != 0 {
						exJ++
					}
					bitset ^= bit
				}
				ms.cm[key] = new.encode(nil, ms.numBuf[:])
			}
		} else {
			ms.cm[key] = update.encode(nil, ms.numBuf[:])
		}
	}
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
	storages   map[string]map[string][]byte
	deletes    map[string]struct{}
	deletes2   map[string]map[string]struct{}
	keyset     map[string]struct{}
	keyset2    map[string]map[string]struct{}
}

func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{
		balances:   make(map[string]*uint256.Int),
		nonces:     make(map[string]uint64),
		codeHashes: make(map[string][32]byte),
		storages:   make(map[string]map[string][]byte),
		deletes:    make(map[string]struct{}),
		deletes2:   make(map[string]map[string]struct{}),
		keyset:     make(map[string]struct{}),
		keyset2:    make(map[string]map[string]struct{}),
	}
}

func (ub *UpdateBuilder) Balance(addr string, balance uint64) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	ub.balances[sk] = uint256.NewInt(balance)
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Nonce(addr string, nonce uint64) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	ub.nonces[sk] = nonce
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) CodeHash(addr string, hash [32]byte) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	ub.codeHashes[sk] = hash
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Storage(addr string, loc string, value string) *UpdateBuilder {
	sk1 := string(decodeHex(addr))
	sk2 := string(decodeHex(loc))
	v := decodeHex(value)
	if d, ok := ub.deletes2[sk1]; ok {
		delete(d, sk2)
		if len(d) == 0 {
			delete(ub.deletes2, sk1)
		}
	}
	if k, ok := ub.keyset2[sk1]; ok {
		k[sk2] = struct{}{}
	} else {
		ub.keyset2[sk1] = make(map[string]struct{})
		ub.keyset2[sk1][sk2] = struct{}{}
	}
	if s, ok := ub.storages[sk1]; ok {
		s[sk2] = v
	} else {
		ub.storages[sk1] = make(map[string][]byte)
		ub.storages[sk1][sk2] = v
	}
	return ub
}

func (ub *UpdateBuilder) Delete(addr string) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.balances, sk)
	delete(ub.nonces, sk)
	delete(ub.codeHashes, sk)
	delete(ub.storages, sk)
	ub.deletes[sk] = struct{}{}
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) DeleteStorage(addr string, loc string) *UpdateBuilder {
	sk1 := string(decodeHex(addr))
	sk2 := string(decodeHex(loc))
	if s, ok := ub.storages[sk1]; ok {
		delete(s, sk2)
		if len(s) == 0 {
			delete(ub.storages, sk1)
		}
	}
	if k, ok := ub.keyset2[sk1]; ok {
		k[sk2] = struct{}{}
	} else {
		ub.keyset2[sk1] = make(map[string]struct{})
		ub.keyset2[sk1][sk2] = struct{}{}
	}
	if d, ok := ub.deletes2[sk1]; ok {
		d[sk2] = struct{}{}
	} else {
		ub.deletes2[sk1] = make(map[string]struct{})
		ub.deletes2[sk1][sk2] = struct{}{}
	}
	return ub
}

// Build returns three slices (in the order sorted by the hashed keys)
// 1. Plain keys
// 2. Corresponding hashed keys
// 3. Corresponding updates
func (ub *UpdateBuilder) Build() (plainKeys, hashedKeys [][]byte, updates []Update) {
	var hashed []string
	preimages := make(map[string][]byte)
	preimages2 := make(map[string][]byte)
	keccak := sha3.NewLegacyKeccak256()
	for key := range ub.keyset {
		keccak.Reset()
		keccak.Write([]byte(key))
		h := keccak.Sum(nil)
		hashedKey := make([]byte, len(h)*2)
		for i, c := range h {
			hashedKey[i*2] = (c >> 4) & 0xf
			hashedKey[i*2+1] = c & 0xf
		}
		hashed = append(hashed, string(hashedKey))
		preimages[string(hashedKey)] = []byte(key)
	}
	hashedKey := make([]byte, 128)
	for sk1, k := range ub.keyset2 {
		keccak.Reset()
		keccak.Write([]byte(sk1))
		h := keccak.Sum(nil)
		for i, c := range h {
			hashedKey[i*2] = (c >> 4) & 0xf
			hashedKey[i*2+1] = c & 0xf
		}
		for sk2 := range k {
			keccak.Reset()
			keccak.Write([]byte(sk2))
			h2 := keccak.Sum(nil)
			for i, c := range h2 {
				hashedKey[64+i*2] = (c >> 4) & 0xf
				hashedKey[64+i*2+1] = c & 0xf
			}
			hs := string(common.Copy(hashedKey))
			hashed = append(hashed, hs)
			preimages[hs] = []byte(sk1)
			preimages2[hs] = []byte(sk2)
		}

	}
	sort.Strings(hashed)
	plainKeys = make([][]byte, len(hashed))
	hashedKeys = make([][]byte, len(hashed))
	updates = make([]Update, len(hashed))
	for i, hashedKey := range hashed {
		hashedKeys[i] = []byte(hashedKey)
		key := preimages[hashedKey]
		key2 := preimages2[hashedKey]
		plainKey := make([]byte, len(key)+len(key2))
		copy(plainKey[:], []byte(key))
		if key2 != nil {
			copy(plainKey[len(key):], []byte(key2))
		}
		plainKeys[i] = plainKey
		u := &updates[i]
		if key2 == nil {
			if balance, ok := ub.balances[string(key)]; ok {
				u.flags |= BALANCE_UPDATE
				u.balance.Set(balance)
			}
			if nonce, ok := ub.nonces[string(key)]; ok {
				u.flags |= NONCE_UPDATE
				u.nonce = nonce
			}
			if codeHash, ok := ub.codeHashes[string(key)]; ok {
				u.flags |= CODE_UPDATE
				copy(u.codeHashOrStorage[:], codeHash[:])
			}
		} else {
			if sm, ok1 := ub.storages[string(key)]; ok1 {
				if storage, ok2 := sm[string(key2)]; ok2 {
					u.flags |= STORAGE_UPDATE
					u.codeHashOrStorage = [32]byte{}
					copy(u.codeHashOrStorage[32-len(storage):], storage)
				}
			}
		}
	}
	return
}

func TestEmptyState(t *testing.T) {
	ms := NewMockState()
	hph := &HexPatriciaHashed{
		branchFn:   ms.branchFn,
		accountFn:  ms.accountFn,
		storageFn:  ms.storageFn,
		rootBefore: false, // Start from empty root
		keccak:     sha3.NewLegacyKeccak256(),
	}
	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Balance("04", 8).
		Storage("04", "01", "0401").
		Build()
	if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
		t.Fatal(err)
	}
	branchNodeUpdates, err := hph.processUpdates(plainKeys, hashedKeys, updates, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = ms.applyBranchNodeUpdates(branchNodeUpdates); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Generated updates\n")
	var keys []string
	for key := range branchNodeUpdates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		branchNodeUpdate := branchNodeUpdates[key]
		fmt.Printf("%x => %+v\n", key, branchNodeUpdate)
	}
}
