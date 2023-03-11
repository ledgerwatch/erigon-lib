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

// entry for the cursor
type cursorEntry struct {
	key   []byte
	value []byte
}

// cursor
type memoryMutationCursor struct {
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
}

func (m *memoryMutationCursor) isTableCleared() bool {
	return m.mutation.isTableCleared(m.table)
}

func (m *memoryMutationCursor) isEntryDeleted(key []byte) bool {
	return m.mutation.isEntryDeleted(m.table, key)
}

func (m *memoryMutationCursor) selectEntry(direction DirectionType, memKey, memValue, dbKey, dbValue []byte) ([]byte, []byte, error) {
	//fmt.Printf("selectEntry direction %v, mem: %s=>%s, db: %s=>%s\n", direction, memKey, memValue, dbKey, dbValue)
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
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.First(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Next() {
	}
	if err != nil {
		return nil, nil, err
	}
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
}

// Current return the current key and values the cursor is on.
func (m *memoryMutationCursor) Current() ([]byte, []byte, error) {
	switch m.current {
	case OverCurrent, OverOnly, BothCurrent:
		return m.memCursor.Current()
	case UnderCurrent, UnderOnly:
		return m.cursor.Current()
	default:
		return nil, nil, nil
	}
}

// Next returns the next element of the mutation.
func (m *memoryMutationCursor) Next() ([]byte, []byte, error) {
	//fmt.Printf("Next\n")
	direction := m.direction
	m.direction = ForwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		return m.memCursor.Next()
	}
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	var memKeySet, dbKeySet bool
	// Reverse direction if needed
	if direction == BackwardDirection {
		//fmt.Printf("Reverse direction\n")
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
		//fmt.Printf("not preemptedNext, m.current = %v\n", m.current)
		if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
			if memKey, memValue, err = m.memCursor.Next(); err != nil {
				return nil, nil, err
			}
			memKeySet = true
		}
		if m.current == UnderCurrent || m.current == UnderOnly || m.current == BothCurrent {
			//fmt.Printf("UnderCurrent\n")
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
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursor) Seek(seek []byte) ([]byte, []byte, error) {
	//fmt.Printf("Seek %s\n", seek)
	m.direction = ForwardDirection
	m.preemptedNext = false
	var err error
	var memKey, memValue []byte
	var dbKey, dbValue []byte
	if memKey, memValue, err = m.memCursor.Seek(seek); err != nil {
		return nil, nil, err
	}
	if m.isTableCleared() {
		return memKey, memValue, nil
	}
	for dbKey, dbValue, err = m.cursor.Seek(seek); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Next() {
	}
	if err != nil {
		return nil, nil, err
	}
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
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
		return memKey, memValue, nil
	}
	if m.isEntryDeleted(seek) {
		return nil, nil, nil
	} else if dbKey, dbValue, err = m.cursor.SeekExact(seek); err != nil {
		return nil, nil, err
	}
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
}

func (m *memoryMutationCursor) Put(k, v []byte) error {
	return m.memCursor.Put(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursor) Append(k []byte, v []byte) error {
	return m.memCursor.Append(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursor) Delete(k []byte) error {
	if _, ok := m.mutation.deletedEntries[m.table]; !ok {
		m.mutation.deletedEntries[m.table] = map[string]struct{}{}
	}
	m.mutation.deletedEntries[m.table][string(k)] = struct{}{}
	return m.memCursor.Delete(k)
}

func (m *memoryMutationCursor) DeleteCurrent() error {
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
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.Last(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey); dbKey, dbValue, err = m.cursor.Prev() {
	}
	if err != nil {
		return nil, nil, err
	}
	return m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
}

func (m *memoryMutationCursor) Prev() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = BackwardDirection
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		return m.memCursor.Prev()
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
	return m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
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
}

func (m *memoryMutationCursorDupSort) isTableCleared() bool {
	return m.mutation.isTableCleared(m.table)
}

func (m *memoryMutationCursorDupSort) isEntryDeleted(key []byte, value []byte) bool {
	return m.mutation.isDupsortEntryDeleted(m.table, key, value)
}

func (m *memoryMutationCursorDupSort) selectEntry(direction DirectionType, memKey, memValue, dbKey, dbValue []byte) ([]byte, []byte, error) {
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
	if c == 0 {
		c = bytes.Compare(dbValue, memValue)
	}
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
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.First(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.NextDup() {
	}
	if err != nil {
		return nil, nil, err
	}
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
}

// Current return the current key and values the cursor is on.
func (m *memoryMutationCursorDupSort) Current() ([]byte, []byte, error) {
	switch m.current {
	case OverCurrent, OverOnly, BothCurrent:
		return m.memCursor.Current()
	case UnderCurrent, UnderOnly:
		return m.cursor.Current()
	default:
		return nil, nil, nil
	}
}

// Next returns the next element of the mutation.
func (m *memoryMutationCursorDupSort) Next() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = ForwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		return m.memCursor.Next()
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
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
}

// NextDup returns the next element of the mutation.
func (m *memoryMutationCursorDupSort) NextDup() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = ForwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		return m.memCursor.NextDup()
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
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
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
		return memKey, memValue, nil
	}
	for dbKey, dbValue, err = m.cursor.Seek(seek); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.NextDup() {
	}
	if err != nil {
		return nil, nil, err
	}
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
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
		return memKey, memValue, nil
	}
	if m.mutation.isEntryDeleted(m.table, seek) {
		return nil, nil, nil
	} else if dbKey, dbValue, err = m.cursor.SeekExact(seek); err != nil {
		return nil, nil, err
	}
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
}

func (m *memoryMutationCursorDupSort) Put(k, v []byte) error {
	return m.memCursor.Put(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursorDupSort) Append(k []byte, v []byte) error {
	return m.memCursor.Append(common.Copy(k), common.Copy(v))

}

func (m *memoryMutationCursorDupSort) AppendDup(k []byte, v []byte) error {
	return m.memCursor.AppendDup(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursorDupSort) PutNoDupData(key, value []byte) error {
	return m.memCursor.PutNoDupData(common.Copy(key), common.Copy(value))
}

func (m *memoryMutationCursorDupSort) Delete(k []byte) error {
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
	m.direction = ForwardDirection
	m.preemptedNext = false
	fmt.Printf("current = %v\n", m.current)
	if m.current == OverCurrent || m.current == OverOnly || m.current == BothCurrent {
		if err := m.memCursor.DeleteCurrentDuplicates(); err != nil {
			return err
		}
	}
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
	fmt.Printf("deletes: %+v\n", m.mutation.deletedDupSortEntries)
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
		return memKey, memValue, nil
	}
	var dbKey, dbValue []byte
	for dbKey, dbValue, err = m.cursor.Last(); err == nil && dbKey != nil && m.isEntryDeleted(dbKey, dbValue); dbKey, dbValue, err = m.cursor.PrevDup() {
	}
	if err != nil {
		return nil, nil, err
	}
	return m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
}

func (m *memoryMutationCursorDupSort) Prev() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = BackwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		return m.memCursor.Prev()
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
	return m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
}
func (m *memoryMutationCursorDupSort) PrevDup() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = BackwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		return m.memCursor.PrevDup()
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
	return m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)
}
func (m *memoryMutationCursorDupSort) PrevNoDup() ([]byte, []byte, error) {
	direction := m.direction
	m.direction = BackwardDirection
	preemptedNext := m.preemptedNext
	m.preemptedNext = false
	if m.isTableCleared() {
		m.current = OverOnly
		return m.memCursor.PrevNoDup()
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
	return m.selectEntry(BackwardDirection, memKey, memValue, dbKey, dbValue)

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
		return m.memCursor.NextNoDup()
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
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
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
		return memKey, memValue, nil
	}
	if m.isEntryDeleted(key, value) {
		return nil, nil, nil
	} else if dbKey, dbValue, err = m.cursor.SeekBothExact(key, value); err != nil {
		return nil, nil, err
	}
	return m.selectEntry(ForwardDirection, memKey, memValue, dbKey, dbValue)
}
