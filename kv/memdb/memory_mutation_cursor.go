/*
   Copyright 2023 Erigon contributors
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

package memdb

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type NextType int

const (
	Normal NextType = iota
	Dup
	NoDup
)

type CurrentType int

const (
	NoCurrent CurrentType = iota
	OverCurrent
	OverOnly
	UnderCurrent
	UnderOnly
	BothCurrent
)

type DirectionType int

const (
	NoDirection DirectionType = iota
	ForwardDirection
	BackwardDirection
)

// cursor
type memoryMutationCursor struct {
	// entry history
	cursor    kv.Cursor
	memCursor kv.RwCursor
	// we keep the mining mutation so that we can insert new elements in db
	mutation      *MemoryMutation
	table         string
	bucketCfg     kv.TableCfgItem
	current       CurrentType
	direction     DirectionType
	preemptedNext bool
	trace         bool
}

func (m *memoryMutationCursor) isTableCleared() bool {
	return m.mutation.isTableCleared(m.table)
}

func (m *memoryMutationCursor) isEntryDeleted(key []byte) bool {
	return m.mutation.isEntryDeleted(m.table, key)
}

func (m *memoryMutationCursor) selectEntry(direction DirectionType, memKey, memValue, dbKey, dbValue []byte) ([]byte, []byte, error) {
	if dbKey == nil {
		if memKey == nil {
			m.current = NoCurrent
			return nil, nil, nil
		} else {
			m.current = OverOnly
			return memKey, memValue, nil
		}
	} else if memKey == nil {
		m.current = UnderOnly
		return dbKey, dbValue, nil
	}
	c := bytes.Compare(dbKey, memKey)
	if (direction == ForwardDirection && c < 0) || (direction == BackwardDirection && c > 0) {
		m.current = UnderCurrent
		return dbKey, dbValue, nil
	}
	if c == 0 {
		m.current = BothCurrent
	} else {
		m.current = OverCurrent
	}
	return memKey, memValue, nil
}

// First move cursor to first position and return key and value accordingly.
func (m *memoryMutationCursor) First() ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	if memKey, memValue, err = m.memCursor.First(); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		m.current = OverOnly
		if m.trace {
			fmt.Printf("[%s] First()=>[%x;%x]\n", m.table, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.First(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Next() {
	}
	if err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("[%s] First()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// Current return the current key and values the cursor is on.
func (m *memoryMutationCursor) Current() ([]byte, []byte, error) {
	var k, v []byte
	var e error
	switch m.current {
	case OverCurrent, OverOnly, BothCurrent:
		k, v, e = m.memCursor.Current()
	case UnderCurrent, UnderOnly:
		k, v, e = m.cursor.Current()
	}
	if m.trace {
		fmt.Printf("[%s] Current()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// Next returns the next element of the mutation.
func (m *memoryMutationCursor) Next() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = ForwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.Next()
		if m.trace {
			fmt.Printf("[%s] Next()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == BackwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.Next(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbKey, err = m.cursor.Next() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.Next(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if !preemptedNext {
		if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
			if memKey, memValue, err = m.memCursor.Next(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
		if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
			for dbKey, dbValue, err = m.cursor.Next(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Next() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		}
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("[%s] Next()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursor) Seek(seek []byte) ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	if memKey, memValue, err = m.memCursor.Seek(seek); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		if m.trace {
			fmt.Printf("[%s] Seek(%x)=>[%x;%x]\n", m.table, seek, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	for dbKey, dbValue, err = m.cursor.Seek(seek); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Next() {
	}
	if err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("[%s] Seek(%x)=>[%x;%x]\n", m.table, seek, k, v)
	}
	return k, v, e
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursor) SeekExact(seek []byte) ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	if memKey, memValue, err = m.memCursor.SeekExact(seek); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		if m.trace {
			fmt.Printf("[%s] SeekExact(%x)=>[%x;%x]\n", m.table, seek, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	if !m.isEntryDeleted(seek) {
		if dbKey, dbValue, err = m.cursor.SeekExact(seek); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("[%s] SeekExact(%x)=>[%x;%x]\n", m.table, seek, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursor) Put(k, v []byte) error {
	if m.trace {
		fmt.Printf("[%s] Put(%x;%x)\n", m.table, k, v)
	}
	return m.memCursor.Put(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursor) Append(k []byte, v []byte) error {
	if m.trace {
		fmt.Printf("[%s] Append(%x;%x)\n", m.table, k, v)
	}
	return m.memCursor.Append(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursor) Delete(k []byte) error {
	if m.trace {
		fmt.Printf("[%s] Delete(%x)\n", m.table, k)
	}
	if _, ok := m.mutation.deletedEntries[m.table]; !ok {
		m.mutation.deletedEntries[m.table] = map[string]struct{}{}
	}
	m.mutation.deletedEntries[m.table][string(k)] = struct{}{}
	return m.memCursor.Delete(k)
}

func (m *memoryMutationCursor) DeleteCurrent() error {
	if m.trace {
		fmt.Printf("[%s] DeleteCurrent()\n", m.table)
	}
	m.direction = ForwardDirection
	m.preemptedNext = false
	if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
		var err error
		if err = m.memCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if m.isTableCleared() {
		return nil
	}
	if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
		if _, ok := m.mutation.deletedEntries[m.table]; !ok {
			m.mutation.deletedEntries[m.table] = map[string]struct{}{}
		}
		dbKey, _, err := m.cursor.Current()
		if err != nil {
			return err
		}
		m.mutation.deletedEntries[m.table][string(dbKey)] = struct{}{}
		for dbKey, _, err = m.cursor.Next(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, _, err = m.cursor.Next() {
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *memoryMutationCursor) Last() ([]byte, []byte, error) {
	m.direction = BackwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	if memKey, memValue, err = m.memCursor.Last(); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		m.current = OverOnly
		if m.trace {
			fmt.Printf("[%s] Last()=>[%x;%x]\n", m.table, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.Last(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Prev() {
	}
	if err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("[%s] Last()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursor) Prev() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = BackwardDirection
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.Prev()
		if m.trace {
			fmt.Printf("[%s] Prev()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == ForwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.Prev(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbKey, err = m.cursor.Prev() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.Prev(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
		if memKey, memValue, err = m.memCursor.Prev(); err != nil {
			return nil, nil, err
		}
		memKeySet = true
	}
	if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
		for dbKey, dbValue, err = m.cursor.Prev(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Prev() {
		}
		if err != nil {
			return nil, nil, err
		}
		dbKeySet = true
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("[%s] Prev()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursor) Close() {
	if m.cursor != nil {
		m.cursor.Close()
	}
	if m.memCursor != nil {
		m.memCursor.Close()
	}
}

func (m *memoryMutationCursor) Count() (uint64, error) {
	panic("Not implemented")
}

// cursor
type memoryMutationCursorAuto struct {
	// entry history
	cursor    kv.Cursor
	memCursor kv.RwCursor
	// we keep the mining mutation so that we can insert new elements in db
	mutation      *MemoryMutation
	table         string
	bucketCfg     kv.TableCfgItem
	current       CurrentType
	direction     DirectionType
	preemptedNext bool
	trace         bool
}

func (m *memoryMutationCursorAuto) isTableCleared() bool {
	return m.mutation.isTableCleared(m.table)
}

func (m *memoryMutationCursorAuto) isEntryDeleted(key []byte) bool {
	return m.mutation.isEntryDeleted(m.table, key)
}

func (m *memoryMutationCursorAuto) selectEntry(direction DirectionType, memKey, memValue, dbKey, dbValue []byte) ([]byte, []byte, error) {
	if dbKey == nil {
		if memKey == nil {
			m.current = NoCurrent
			return nil, nil, nil
		} else {
			m.current = OverOnly
			return memKey, memValue, nil
		}
	} else if memKey == nil {
		m.current = UnderOnly
		return dbKey, dbValue, nil
	}
	c := bytes.Compare(dbKey, memKey)
	if (direction == ForwardDirection && c < 0) || (direction == BackwardDirection && c > 0) {
		m.current = UnderCurrent
		return dbKey, dbValue, nil
	}
	if c == 0 {
		m.current = BothCurrent
	} else {
		m.current = OverCurrent
	}
	return memKey, memValue, nil
}

// First move cursor to first position and return key and value accordingly.
func (m *memoryMutationCursorAuto) First() ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	if memKey, memValue, err = m.memCursor.First(); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		m.current = OverOnly
		if m.trace {
			fmt.Printf("Auto [%s] First()=>[%x;%x]\n", m.table, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.First(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Next() {
	}
	if err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("Auto [%s] First()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// Current return the current key and values the cursor is on.
func (m *memoryMutationCursorAuto) Current() ([]byte, []byte, error) {
	var k, v []byte
	var e error
	switch m.current {
	case OverCurrent, OverOnly, BothCurrent:
		k, v, e = m.memCursor.Current()
	case UnderCurrent, UnderOnly:
		k, v, e = m.cursor.Current()
	}
	if m.trace {
		fmt.Printf("Auto [%s] Current()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// Next returns the next element of the mutation.
func (m *memoryMutationCursorAuto) Next() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = ForwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.Next()
		if m.trace {
			fmt.Printf("Auto [%s] Next()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == BackwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.Next(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbKey, err = m.cursor.Next() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.Next(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if !preemptedNext {
		if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
			if memKey, memValue, err = m.memCursor.Next(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
		if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
			for dbKey, dbValue, err = m.cursor.Next(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Next() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		}
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("Auto [%s] Next()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursorAuto) Seek(seek []byte) ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	if memKey, memValue, err = m.memCursor.Seek(seek); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		if m.trace {
			fmt.Printf("Auto [%s] Seek(%x)=>[%x;%x]\n", m.table, seek, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	for dbKey, dbValue, err = m.cursor.Seek(seek); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Next() {
	}
	if err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("Auto [%s] Seek(%x)=>[%x;%x]\n", m.table, seek, k, v)
	}
	return k, v, e
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursorAuto) SeekExact(seek []byte) ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	if memKey, memValue, err = m.memCursor.SeekExact(seek); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		if m.trace {
			fmt.Printf("Auto [%s] SeekExact(%x)=>[%x;%x]\n", m.table, seek, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	if !m.isEntryDeleted(seek) {
		if dbKey, dbValue, err = m.cursor.SeekExact(seek); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("Auto [%s] SeekExact(%x)=>[%x;%x]\n", m.table, seek, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursorAuto) Put(k, v []byte) error {
	if m.trace {
		fmt.Printf("Auto [%s] Put(%x;%x)\n", m.table, k, v)
	}
	return m.memCursor.Put(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursorAuto) Append(k []byte, v []byte) error {
	if m.trace {
		fmt.Printf("Auto [%s] Append(%x;%x)\n", m.table, k, v)
	}
	return m.memCursor.Append(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursorAuto) Delete(k []byte) error {
	if m.trace {
		fmt.Printf("Auto [%s] Delete(%x)\n", m.table, k)
	}
	if _, ok := m.mutation.deletedEntries[m.table]; !ok {
		m.mutation.deletedEntries[m.table] = map[string]struct{}{}
	}
	m.mutation.deletedEntries[m.table][string(k)] = struct{}{}
	return m.memCursor.Delete(k)
}

func (m *memoryMutationCursorAuto) DeleteCurrent() error {
	if m.trace {
		fmt.Printf("Auto [%s] DeleteCurrent()\n", m.table)
	}
	m.direction = ForwardDirection
	m.preemptedNext = false
	if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
		var err error
		if err = m.memCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if m.isTableCleared() {
		return nil
	}
	if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
		if _, ok := m.mutation.deletedEntries[m.table]; !ok {
			m.mutation.deletedEntries[m.table] = map[string]struct{}{}
		}
		dbKey, _, err := m.cursor.Current()
		if err != nil {
			return err
		}
		m.mutation.deletedEntries[m.table][string(dbKey)] = struct{}{}
		for dbKey, _, err = m.cursor.Next(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, _, err = m.cursor.Next() {
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *memoryMutationCursorAuto) Last() ([]byte, []byte, error) {
	m.direction = BackwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	if memKey, memValue, err = m.memCursor.Last(); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		m.current = OverOnly
		if m.trace {
			fmt.Printf("Auto [%s] Last()=>[%x;%x]\n", m.table, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.Last(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Prev() {
	}
	if err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("Auto [%s] Last()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursorAuto) Prev() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = BackwardDirection
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.Prev()
		if m.trace {
			fmt.Printf("Auto [%s] Prev()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == ForwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.Prev(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbKey, err = m.cursor.Prev() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.Prev(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
		if memKey, memValue, err = m.memCursor.Prev(); err != nil {
			return nil, nil, err
		}
		memKeySet = true
	}
	if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
		for dbKey, dbValue, err = m.cursor.Prev(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Prev() {
		}
		if err != nil {
			return nil, nil, err
		}
		dbKeySet = true
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("Auto [%s] Prev()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursorAuto) Close() {
	if m.cursor != nil {
		m.cursor.Close()
	}
	if m.memCursor != nil {
		m.memCursor.Close()
	}
}

func (m *memoryMutationCursorAuto) Count() (uint64, error) {
	panic("Not implemented")
}

// cursor
type memoryMutationCursorDupSort struct {
	// entry history
	cursor    kv.CursorDupSort
	memCursor kv.RwCursorDupSort
	// we keep the mining mutation so that we can insert new elements in db
	mutation      *MemoryMutation
	table         string
	bucketCfg     kv.TableCfgItem
	current       CurrentType
	direction     DirectionType
	preemptedNext bool
	trace         bool
}

func (m *memoryMutationCursorDupSort) isTableCleared() bool {
	return m.mutation.isTableCleared(m.table)
}

func (m *memoryMutationCursorDupSort) isEntryDeleted(key []byte, value []byte) bool {
	return m.mutation.isDupsortEntryDeleted(m.table, key, value)
}

func (m *memoryMutationCursorDupSort) selectEntry(direction DirectionType, memKey, memValue, dbKey, dbValue []byte) ([]byte, []byte, error) {
	if m.trace {
		fmt.Printf("selectEntry %v: %x %x %x %x\n", direction, memKey, memValue, dbKey, dbValue)
	}
	if dbKey == nil {
		if memKey == nil {
			m.current = NoCurrent
			if m.trace {
				fmt.Printf("return nil nil\n")
			}
			return nil, nil, nil
		} else {
			m.current = OverOnly
			if m.trace {
				fmt.Printf("return OverOnly %x %x\n", memKey, memValue)
			}
			return memKey, memValue, nil
		}
	} else if memKey == nil {
		m.current = UnderOnly
		if m.trace {
			fmt.Printf("return UnderOnly %x %x\n", dbKey, dbValue)
		}
		return dbKey, dbValue, nil
	}
	c := bytes.Compare(dbKey, memKey)
	if c == 0 {
		if m.bucketCfg.AutoDupSortKeysConversion {
			var memValueTrunc, dbValueTrunc []byte // Truncated values
			if len(memKey) == m.bucketCfg.DupToLen {
				memValueTrunc = memValue[:m.bucketCfg.DupFromLen-m.bucketCfg.DupToLen]
			} else {
				memValueTrunc = memValue
			}
			if len(dbKey) == m.bucketCfg.DupToLen {
				dbValueTrunc = dbValue[:m.bucketCfg.DupFromLen-m.bucketCfg.DupToLen]
			} else {
				dbValueTrunc = dbValue
			}
			c = bytes.Compare(dbValueTrunc, memValueTrunc)
		} else {
			c = bytes.Compare(dbValue, memValue)
		}
	}
	if (direction == ForwardDirection && c < 0) || (direction == BackwardDirection && c > 0) {
		m.current = UnderCurrent
		if m.trace {
			fmt.Printf("return UnderCurrent %x %x\n", dbKey, dbValue)
		}
		return dbKey, dbValue, nil
	}
	if c == 0 {
		m.current = BothCurrent
		if m.trace {
			fmt.Printf("return BothCurrent %x %x\n", memKey, memValue)
		}
	} else {
		m.current = OverCurrent
		if m.trace {
			fmt.Printf("return OverCurrent %x %x\n", memKey, memValue)
		}
	}
	return memKey, memValue, nil
}

// First move cursor to first position and return key and value accordingly.
func (m *memoryMutationCursorDupSort) First() ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	if memKey, memValue, err = m.memCursor.First(); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		m.current = OverOnly
		if m.trace {
			fmt.Printf("DupSort [%s] First()=>[%x;%x]\n", m.table, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.First(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.NextDup() {
	}
	if err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] First()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// Current return the current key and values the cursor is on.
func (m *memoryMutationCursorDupSort) Current() ([]byte, []byte, error) {
	var k, v []byte
	var e error
	switch m.current {
	case OverCurrent, OverOnly, BothCurrent:
		k, v, e = m.memCursor.Current()
	case UnderCurrent, UnderOnly:
		k, v, e = m.cursor.Current()
	}
	if m.trace {
		fmt.Printf("DupSort [%s] Current()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// Next returns the next element of the mutation.
func (m *memoryMutationCursorDupSort) Next() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = ForwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.Next()
		if m.trace {
			fmt.Printf("DupSort [%s] Next()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == BackwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.Next(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbKey, err = m.cursor.NextDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.Next(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if !preemptedNext {
		if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
			if memKey, memValue, err = m.memCursor.Next(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
		if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
			for dbKey, dbValue, err = m.cursor.Next(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.NextDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		}
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] Next()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// NextDup returns the next element of the mutation.
func (m *memoryMutationCursorDupSort) NextDup() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = ForwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.NextDup()
		if m.trace {
			fmt.Printf("DupSort [%s] NextDup()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == BackwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.NextDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbKey, err = m.cursor.NextDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.NextDup(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if !preemptedNext {
		if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
			if memKey, memValue, err = m.memCursor.NextDup(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
		if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
			for dbKey, dbValue, err = m.cursor.NextDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.NextDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		}
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] NextDup()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursorDupSort) Seek(seek []byte) ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	if memKey, memValue, err = m.memCursor.Seek(seek); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		if m.trace {
			fmt.Printf("DupSort [%s] Seek(%x)=>[%x;%x]\n", m.table, seek, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	for dbKey, dbValue, err = m.cursor.Seek(seek); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.NextDup() {
	}
	if err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] Seek(%x)=>[%x;%x]\n", m.table, seek, k, v)
	}
	return k, v, e
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursorDupSort) SeekExact(seek []byte) ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	if memKey, memValue, err = m.memCursor.SeekExact(seek); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		if m.trace {
			fmt.Printf("DupSort [%s] SeekExact(%x)=>[%x;%x]\n", m.table, seek, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	if !m.mutation.isEntryDeleted(m.table, seek) {
		if dbKey, dbValue, err = m.cursor.SeekExact(seek); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] SeekExact(%x)=>[%x;%x]\n", m.table, seek, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursorDupSort) Put(k, v []byte) error {
	if m.trace {
		fmt.Printf("DupSort [%s] Put(%x;%x)\n", m.table, k, v)
	}
	return m.memCursor.Put(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursorDupSort) Append(k []byte, v []byte) error {
	if m.trace {
		fmt.Printf("DupSort [%s] Append(%x;%x)\n", m.table, k, v)
	}
	return m.memCursor.Append(common.Copy(k), common.Copy(v))

}

func (m *memoryMutationCursorDupSort) AppendDup(k []byte, v []byte) error {
	if m.trace {
		fmt.Printf("DupSort [%s] AppendDup(%x;%x)\n", m.table, k, v)
	}
	return m.memCursor.AppendDup(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursorDupSort) PutNoDupData(k, v []byte) error {
	if m.trace {
		fmt.Printf("DupSort [%s] PutNoDupData(%x;%x)\n", m.table, k, v)
	}
	return m.memCursor.PutNoDupData(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursorDupSort) Delete(k []byte) error {
	if m.trace {
		fmt.Printf("DupSort [%s] Delete(%x)\n", m.table, k)
	}
	foundK, _, err := m.cursor.SeekExact(k)
	if err != nil {
		return err
	}
	if err = m.memCursor.Delete(k); err != nil {
		return err
	}
	if foundK == nil {
		// No need to delete
		return nil
	}
	if _, ok := m.mutation.deletedEntries[m.table]; !ok {
		m.mutation.deletedEntries[m.table] = map[string]struct{}{}
	}
	m.mutation.deletedEntries[m.table][string(k)] = struct{}{}
	return nil
}

func (m *memoryMutationCursorDupSort) DeleteCurrent() error {
	if m.trace {
		fmt.Printf("DupSort [%s] DeleteCurrent()\n", m.table)
	}
	m.direction = ForwardDirection
	m.preemptedNext = false
	if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
		if err := m.memCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if m.isTableCleared() {
		return nil
	}
	if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
		if _, ok := m.mutation.deletedEntries[m.table]; !ok {
			m.mutation.deletedEntries[m.table] = map[string]struct{}{}
		}
		dbKey, dbValue, err := m.cursor.Current()
		if err != nil {
			return err
		}
		if _, ok := m.mutation.deletedDupSortEntries[m.table]; !ok {
			m.mutation.deletedDupSortEntries[m.table] = map[string]map[string]struct{}{}
		}
		if _, ok := m.mutation.deletedDupSortEntries[m.table][string(dbKey)]; !ok {
			m.mutation.deletedDupSortEntries[m.table][string(dbKey)] = map[string]struct{}{}
		}
		m.mutation.deletedDupSortEntries[m.table][string(dbKey)][string(dbValue)] = struct{}{}
		for dbKey, dbValue, err = m.cursor.NextDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.NextDup() {
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (m *memoryMutationCursorDupSort) DeleteExact(k1, k2 []byte) error {
	if m.trace {
		fmt.Printf("DupSort [%s] DeleteExact(%x;%x)\n", m.table, k1, k2)
	}
	m.direction = ForwardDirection
	foundK, foundV, err := m.cursor.SeekBothExact(k1, k2)
	if err != nil {
		return err
	}
	if err = m.memCursor.DeleteExact(k1, k2); err != nil {
		return err
	}
	if foundK == nil && foundV == nil {
		// No need to delete
		return nil
	}
	if _, ok := m.mutation.deletedDupSortEntries[m.table]; !ok {
		m.mutation.deletedDupSortEntries[m.table] = map[string]map[string]struct{}{}
	}
	if _, ok := m.mutation.deletedDupSortEntries[m.table][string(k1)]; !ok {
		m.mutation.deletedDupSortEntries[m.table][string(k1)] = map[string]struct{}{}
	}
	m.mutation.deletedDupSortEntries[m.table][string(k1)][string(k2)] = struct{}{}
	return nil
}

func (m *memoryMutationCursorDupSort) DeleteCurrentDuplicates() error {
	if m.trace {
		fmt.Printf("DupSort [%s] DeleteCurrentDuplicates()\n", m.table)
	}
	m.direction = ForwardDirection
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		return nil
	}
	currKey, _, err := m.cursor.Current()
	if err != nil {
		return err
	}
	if currKey == nil {
		return nil
	}
	if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
		if err := m.memCursor.DeleteCurrentDuplicates(); err != nil {
			return err
		}
	} else if m.current != UnderOnly && m.current != NoCurrent {
		currMemKey, _, err := m.memCursor.Current()
		if err != nil {
			return err
		}
		if bytes.Equal(currKey, currMemKey) {
			if err := m.memCursor.DeleteCurrentDuplicates(); err != nil {
				return err
			}
		}
	}
	if _, ok := m.mutation.deletedDupSortEntries[m.table]; !ok {
		m.mutation.deletedDupSortEntries[m.table] = map[string]map[string]struct{}{}
	}
	if _, ok := m.mutation.deletedDupSortEntries[m.table][string(currKey)]; !ok {
		m.mutation.deletedDupSortEntries[m.table][string(currKey)] = map[string]struct{}{}
	}
	var dbKey, dbValue []byte
	dbKey = currKey
	for dbValue, err = m.cursor.FirstDup(); err == nil && dbValue != nil && bytes.Equal(currKey, dbKey); dbKey, dbValue, err = m.cursor.NextDup() {
		m.mutation.deletedDupSortEntries[m.table][string(currKey)][string(dbValue)] = struct{}{}
	}
	if err != nil {
		return err
	}
	return nil
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursorDupSort) SeekBothRange(key, value []byte) ([]byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memValue []byte
	var dbValue []byte
	if memValue, err = m.memCursor.SeekBothRange(key, value); err != nil {
		return nil, err
	}
	var memKey = key
	if memValue == nil {
		memKey = nil
	}
	if m.isTableCleared() {
		m.current = OverOnly
		if m.trace {
			fmt.Printf("DupSort [%s] SeekBothRange(%x;%x)=>[%x]\n", m.table, key, value, memValue)
		}
		return memValue, nil
	}
	var dbKey = key
	for dbValue, err = m.cursor.SeekBothRange(key, value); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue) && bytes.Equal(key, dbKey); dbKey, dbValue, err = m.cursor.NextDup() {
	}
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(key, dbKey) || dbValue == nil {
		dbKey = nil
		dbValue = nil
	}
	_, val, err := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] SeekBothRange(%x;%x)=>[%x]\n", m.table, key, value, val)
	}
	return val, err
}

func (m *memoryMutationCursorDupSort) Last() ([]byte, []byte, error) {
	m.direction = BackwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	if memKey, memValue, err = m.memCursor.Last(); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		m.current = OverOnly
		if m.trace {
			fmt.Printf("DupSort [%s] Last()=>[%x;%x]\n", m.table, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.Last(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.PrevDup() {
	}
	if err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] Last()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursorDupSort) Prev() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = BackwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.Prev()
		if m.trace {
			fmt.Printf("DupSort [%s] Prev()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == ForwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.Prev(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbKey, err = m.cursor.PrevDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.Prev(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if !preemptedNext {
		if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
			if memKey, memValue, err = m.memCursor.Prev(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
		if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
			for dbKey, dbValue, err = m.cursor.Prev(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.PrevDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		}
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] Prev()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}
func (m *memoryMutationCursorDupSort) PrevDup() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = BackwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.PrevDup()
		if m.trace {
			fmt.Printf("DupSort [%s] PrevDup()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == ForwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.PrevDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbKey, err = m.cursor.PrevDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.PrevDup(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if !preemptedNext {
		if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
			if memKey, memValue, err = m.memCursor.PrevDup(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
		if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
			for dbKey, dbValue, err = m.cursor.PrevDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.PrevDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		}
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] PrevDup()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}
func (m *memoryMutationCursorDupSort) PrevNoDup() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = BackwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.PrevNoDup()
		if m.trace {
			fmt.Printf("DupSort [%s] PrevNoDup()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == ForwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.PrevNoDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbKey, err = m.cursor.PrevDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.PrevNoDup(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if !preemptedNext {
		if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
			if memKey, memValue, err = m.memCursor.PrevNoDup(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
		if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
			for dbKey, dbValue, err = m.cursor.PrevNoDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.PrevDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		}
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] PrevNoDup()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursorDupSort) Close() {
	if m.cursor != nil {
		m.cursor.Close()
	}
	if m.memCursor != nil {
		m.memCursor.Close()
	}
}

func (m *memoryMutationCursorDupSort) Count() (uint64, error) {
	panic("Not implemented")
}

func (m *memoryMutationCursorDupSort) FirstDup() ([]byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	if memKey, _, err = m.memCursor.Current(); err != nil {
		return nil, err
	}
	if memValue, err = m.memCursor.FirstDup(); err != nil {
		return nil, err
	}
	if m.isTableCleared() {
		m.current = OverOnly
		return memValue, nil
	}
	var dbKey, dbValue []byte
	if dbKey, _, err = m.cursor.Current(); err != nil {
		return nil, err
	}
	for dbValue, err = m.cursor.FirstDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.NextDup() {
	}
	if err != nil {
		return nil, err
	}
	_, value, err := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (m *memoryMutationCursorDupSort) NextNoDup() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = ForwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		k, v, e := m.memCursor.NextNoDup()
		if m.trace {
			fmt.Printf("DupSort [%s] NextNoDup()=>[%x;%x]\n", m.table, k, v)
		}
		return k, v, e
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == BackwardDirection {
		switch m.current {
		case OverCurrent:
			for dbKey, dbValue, err = m.cursor.NextNoDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbKey, err = m.cursor.NextDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		case UnderCurrent:
			if memKey, memValue, err = m.memCursor.NextNoDup(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
	}
	if !preemptedNext {
		if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
			if memKey, memValue, err = m.memCursor.NextNoDup(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
		if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
			for dbKey, dbValue, err = m.cursor.NextNoDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.NextDup() {
			}
			if err != nil {
				return nil, nil, err
			}
			dbKeySet = true
		}
	}
	if !memKeySet && m.current != UnderOnly && m.current != NoCurrent {
		if memKey, memValue, err = m.memCursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	if !dbKeySet && m.current != OverOnly && m.current != NoCurrent {
		if dbKey, dbValue, err = m.cursor.Current(); err != nil {
			return nil, nil, err
		}
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] NextNoDup()=>[%x;%x]\n", m.table, k, v)
	}
	return k, v, e
}

func (m *memoryMutationCursorDupSort) LastDup() ([]byte, error) {
	m.direction = BackwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	if memKey, _, err = m.memCursor.Current(); err != nil {
		return nil, err
	}
	if memValue, err = m.memCursor.LastDup(); err != nil {
		return nil, err
	}
	if m.isTableCleared() {
		m.current = OverOnly
		if m.trace {
			fmt.Printf("DupSort [%s] LastDup()=>[%x]\n", m.table, memValue)
		}
		return memValue, nil
	}
	var dbKey, dbValue []byte
	if dbKey, _, err = m.cursor.Current(); err != nil {
		return nil, err
	}
	for dbValue, err = m.cursor.LastDup(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.PrevDup() {
	}
	if err != nil {
		return nil, err
	}
	_, value, err := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if err != nil {
		return nil, err
	}
	if m.trace {
		fmt.Printf("DupSort [%s] LastDup()=>[%x]\n", m.table, value)
	}
	return value, nil
}

func (m *memoryMutationCursorDupSort) CountDuplicates() (uint64, error) {
	panic("Not implemented")
}

func (m *memoryMutationCursorDupSort) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	if memKey, memValue, err = m.memCursor.SeekBothExact(key, value); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		m.current = OverOnly
		if m.trace {
			fmt.Printf("DupSort [%s] SeekBothExact(%x;%x)=>[%x;%x]\n", m.table, key, value, memKey, memValue)
		}
		return memKey, memValue, nil
	}
	if m.isEntryDeleted(key, value) {
		if m.trace {
			fmt.Printf("DupSort [%s] SeekBothExact(%x;%x)=>[nil;nil]\n", m.table, key, value)
		}
		return nil, nil, nil
	} else if dbKey, dbValue, err = m.cursor.SeekBothExact(key, value); err != nil {
		return nil, nil, err
	}
	k, v, e := m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
	if m.trace {
		fmt.Printf("DupSort [%s] SeekBothExact(%x;%x)=>[%x;%x]\n", m.table, key, value, k, v)
	}
	return k, v, e
}
