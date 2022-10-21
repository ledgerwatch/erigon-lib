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

package etl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

const (
	//SliceBuffer - just simple slice w
	SortableSliceBuffer = iota
	//SortableAppendBuffer - map[k] [v1 v2 v3]
	SortableAppendBuffer
	// SortableOldestAppearedBuffer - buffer that keeps only the oldest entries.
	// if first v1 was added under key K, then v2; only v1 will stay
	SortableOldestAppearedBuffer

	//BufIOSize - 128 pages | default is 1 page | increasing over `64 * 4096` doesn't show speedup on SSD/NVMe, but show speedup in cloud drives
	BufIOSize = 128 * 4096
)

var BufferOptimalSize = 256 * datasize.MB /*  var because we want to sometimes change it from tests or command-line flags */

type Buffer interface {
	Put(k, v []byte)
	Get(i int, keyBuf, valBuf []byte) ([]byte, []byte)
	Len() int
	Reset()
	Write(io.Writer) error
	Sort()
	CheckFlushSize() bool
	SetComparator(cmp kv.CmpFunc)
}

type sortableBufferEntry struct {
	key   []byte
	value []byte
}

var (
	_ Buffer = &sortableBuffer{}
	_ Buffer = &appendSortableBuffer{}
	_ Buffer = &oldestEntrySortableBuffer{}
)

func NewSortableBuffer(bufferOptimalSize datasize.ByteSize) *sortableBuffer {
	return &sortableBuffer{
		optimalSize: int(bufferOptimalSize.Bytes()),
	}
}

type sortableBuffer struct {
	comparator  kv.CmpFunc
	offsets     []int
	lens        []int
	data        []byte
	optimalSize int
}

// Put adds key and value to the buffer. These slices will not be accessed later,
// so no copying is necessary
func (b *sortableBuffer) Put(k, v []byte) {
	b.offsets = append(b.offsets, len(b.data))
	b.lens = append(b.lens, len(k))
	if len(k) > 0 {
		b.data = append(b.data, k...)
	}
	b.offsets = append(b.offsets, len(b.data))
	b.lens = append(b.lens, len(v))
	if len(v) > 0 {
		b.data = append(b.data, v...)
	}
}

func (b *sortableBuffer) Size() int {
	return len(b.data) + 8*len(b.offsets) + 8*len(b.lens)
}

func (b *sortableBuffer) Len() int {
	return len(b.offsets) / 2
}

func (b *sortableBuffer) SetComparator(cmp kv.CmpFunc) {
	b.comparator = cmp
}

func (b *sortableBuffer) Less(i, j int) bool {
	i2, j2 := i*2, j*2
	ki := b.data[b.offsets[i2] : b.offsets[i2]+b.lens[i2]]
	kj := b.data[b.offsets[j2] : b.offsets[j2]+b.lens[j2]]
	if b.comparator != nil {
		vi := b.data[b.offsets[i2+1] : b.offsets[i2+1]+b.lens[i2+1]]
		vj := b.data[b.offsets[j2+1] : b.offsets[j2+1]+b.lens[j2+1]]
		return b.comparator(ki, kj, vi, vj) < 0
	}
	return bytes.Compare(ki, kj) < 0
}

func (b *sortableBuffer) Swap(i, j int) {
	i2, j2 := i*2, j*2
	b.offsets[i2], b.offsets[j2] = b.offsets[j2], b.offsets[i2]
	b.offsets[i2+1], b.offsets[j2+1] = b.offsets[j2+1], b.offsets[i2+1]
	b.lens[i2], b.lens[j2] = b.lens[j2], b.lens[i2]
	b.lens[i2+1], b.lens[j2+1] = b.lens[j2+1], b.lens[i2+1]
}

func (b *sortableBuffer) Get(i int, keyBuf, valBuf []byte) ([]byte, []byte) {
	i2 := i * 2
	keyOffset, valOffset := b.offsets[i2], b.offsets[i2+1]
	keyLen, valLen := b.lens[i2], b.lens[i2+1]
	if keyLen > 0 {
		keyBuf = append(keyBuf, b.data[keyOffset:keyOffset+keyLen]...)
	}
	if valLen > 0 {
		valBuf = append(valBuf, b.data[valOffset:valOffset+valLen]...)
	}
	return keyBuf, valBuf
}

func (b *sortableBuffer) Reset() {
	b.offsets = b.offsets[:0]
	b.lens = b.lens[:0]
	b.data = b.data[:0]
}
func (b *sortableBuffer) Sort() {
	if sort.IsSorted(b) {
		return
	}
	sort.Stable(b)
}

func (b *sortableBuffer) CheckFlushSize() bool {
	return b.Size() >= b.optimalSize
}

func (b *sortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	for i, offset := range b.offsets {
		l := b.lens[i]
		n := binary.PutUvarint(numBuf[:], uint64(l))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err := w.Write(b.data[offset : offset+l]); err != nil {
			return err
		}
	}
	return nil
}

func NewAppendBuffer(bufferOptimalSize datasize.ByteSize) *appendSortableBuffer {
	return &appendSortableBuffer{
		entries:     make(map[string][]byte),
		size:        0,
		optimalSize: int(bufferOptimalSize.Bytes()),
	}
}

type appendSortableBuffer struct {
	entries     map[string][]byte
	comparator  kv.CmpFunc
	sortedBuf   []sortableBufferEntry
	size        int
	optimalSize int
}

func (b *appendSortableBuffer) Put(k, v []byte) {
	stored, ok := b.entries[string(k)]
	if !ok {
		b.size += len(k)
	}
	b.size += len(v)
	stored = append(stored, v...)
	b.entries[string(k)] = stored
}

func (b *appendSortableBuffer) SetComparator(cmp kv.CmpFunc) {
	b.comparator = cmp
}

func (b *appendSortableBuffer) Size() int {
	return b.size
}

func (b *appendSortableBuffer) Len() int {
	return len(b.entries)
}
func (b *appendSortableBuffer) Sort() {
	for i := range b.entries {
		b.sortedBuf = append(b.sortedBuf, sortableBufferEntry{key: []byte(i), value: b.entries[i]})
	}
	sort.Stable(b)
}

func (b *appendSortableBuffer) Less(i, j int) bool {
	if b.comparator != nil {
		return b.comparator(b.sortedBuf[i].key, b.sortedBuf[j].key, b.sortedBuf[i].value, b.sortedBuf[j].value) < 0
	}
	return bytes.Compare(b.sortedBuf[i].key, b.sortedBuf[j].key) < 0
}

func (b *appendSortableBuffer) Swap(i, j int) {
	b.sortedBuf[i], b.sortedBuf[j] = b.sortedBuf[j], b.sortedBuf[i]
}

func (b *appendSortableBuffer) Get(i int, keyBuf, valBuf []byte) ([]byte, []byte) {
	keyBuf = append(keyBuf, b.sortedBuf[i].key...)
	valBuf = append(valBuf, b.sortedBuf[i].value...)
	return keyBuf, valBuf
}
func (b *appendSortableBuffer) Reset() {
	b.sortedBuf = nil
	b.entries = make(map[string][]byte)
	b.size = 0
}

func (b *appendSortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	entries := b.sortedBuf
	for _, entry := range entries {
		n := binary.PutUvarint(numBuf[:], uint64(len(entry.key)))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err := w.Write(entry.key); err != nil {
			return err
		}
		n = binary.PutUvarint(numBuf[:], uint64(len(entry.value)))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err := w.Write(entry.value); err != nil {
			return err
		}
	}
	return nil
}

func (b *appendSortableBuffer) CheckFlushSize() bool {
	return b.size >= b.optimalSize
}

func NewOldestEntryBuffer(bufferOptimalSize datasize.ByteSize) *oldestEntrySortableBuffer {
	return &oldestEntrySortableBuffer{
		entries:     make(map[string][]byte),
		size:        0,
		optimalSize: int(bufferOptimalSize.Bytes()),
	}
}

type oldestEntrySortableBuffer struct {
	entries     map[string][]byte
	comparator  kv.CmpFunc
	sortedBuf   []sortableBufferEntry
	size        int
	optimalSize int
}

func (b *oldestEntrySortableBuffer) SetComparator(cmp kv.CmpFunc) {
	b.comparator = cmp
}

func (b *oldestEntrySortableBuffer) Put(k, v []byte) {
	_, ok := b.entries[string(k)]
	if ok {
		// if we already had this entry, we are going to keep it and ignore new value
		return
	}

	b.size += len(k)*2 + len(v)
	b.entries[string(k)] = common.Copy(v)
}

func (b *oldestEntrySortableBuffer) Size() int {
	return b.size
}

func (b *oldestEntrySortableBuffer) Len() int {
	return len(b.entries)
}

func (b *oldestEntrySortableBuffer) Sort() {
	for k, v := range b.entries {
		b.sortedBuf = append(b.sortedBuf, sortableBufferEntry{key: []byte(k), value: v})
	}
	sort.Stable(b)
}

func (b *oldestEntrySortableBuffer) Less(i, j int) bool {
	if b.comparator != nil {
		return b.comparator(b.sortedBuf[i].key, b.sortedBuf[j].key, b.sortedBuf[i].value, b.sortedBuf[j].value) < 0
	}
	return bytes.Compare(b.sortedBuf[i].key, b.sortedBuf[j].key) < 0
}

func (b *oldestEntrySortableBuffer) Swap(i, j int) {
	b.sortedBuf[i], b.sortedBuf[j] = b.sortedBuf[j], b.sortedBuf[i]
}

func (b *oldestEntrySortableBuffer) Get(i int, keyBuf, valBuf []byte) ([]byte, []byte) {
	keyBuf = append(keyBuf, b.sortedBuf[i].key...)
	valBuf = append(valBuf, b.sortedBuf[i].value...)
	return keyBuf, valBuf
}
func (b *oldestEntrySortableBuffer) Reset() {
	b.sortedBuf = nil
	b.entries = make(map[string][]byte)
	b.size = 0
}

func (b *oldestEntrySortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	entries := b.sortedBuf
	for _, entry := range entries {
		n := binary.PutUvarint(numBuf[:], uint64(len(entry.key)))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err := w.Write(entry.key); err != nil {
			return err
		}
		n = binary.PutUvarint(numBuf[:], uint64(len(entry.value)))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err := w.Write(entry.value); err != nil {
			return err
		}
	}
	return nil
}
func (b *oldestEntrySortableBuffer) CheckFlushSize() bool {
	return b.size >= b.optimalSize
}

func getBufferByType(tp int, size datasize.ByteSize) Buffer {
	switch tp {
	case SortableSliceBuffer:
		return NewSortableBuffer(size)
	case SortableAppendBuffer:
		return NewAppendBuffer(size)
	case SortableOldestAppearedBuffer:
		return NewOldestEntryBuffer(size)
	default:
		panic("unknown buffer type " + strconv.Itoa(tp))
	}
}

func getTypeByBuffer(b Buffer) int {
	switch b.(type) {
	case *sortableBuffer:
		return SortableSliceBuffer
	case *appendSortableBuffer:
		return SortableAppendBuffer
	case *oldestEntrySortableBuffer:
		return SortableOldestAppearedBuffer
	default:
		panic(fmt.Sprintf("unknown buffer type: %T ", b))
	}
}
