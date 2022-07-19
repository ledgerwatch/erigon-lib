/*
   Copyright 2022 Erigon contributors

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

package state

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"

	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

// Algorithms for reconstituting the state from state history

// DomainContext allows accesing the same domain from multiple go-routines
type DomainContext struct {
	d     *Domain
	files [NumberOfTypes]*btree.BTree
}

func (d *Domain) MakeContext() *DomainContext {
	dc := &DomainContext{d: d}
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		bt := btree.New(32)
		dc.files[fType] = bt
		d.files[fType].Ascend(func(item *filesItem) bool {
			bt.ReplaceOrInsert(&filesItem{
				startTxNum:   item.startTxNum,
				endTxNum:     item.endTxNum,
				decompressor: item.decompressor,
				index:        item.index,
				getter:       item.decompressor.MakeGetter(),
				indexReader:  recsplit.NewIndexReader(item.index),
			})
			return true
		})
	}
	return dc
}

func (dc *DomainContext) GetNoState(key []byte, txNum uint64) ([]byte, bool, uint64, error) {
	var foundTxNum uint64
	var foundEndTxNum uint64
	var foundStartTxNum uint64
	var found bool
	var anyItem bool
	var maxTxNum uint64
	dc.files[EfHistory].Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		if item.index.Empty() {
			return true
		}
		offset := item.indexReader.Lookup(key)
		g := item.getter
		g.Reset(offset)
		if k, _ := g.NextUncompressed(); bytes.Equal(k, key) {
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			if n, ok := ef.Search(txNum); ok {
				foundTxNum = n
				foundEndTxNum = item.endTxNum
				foundStartTxNum = item.startTxNum
				found = true
				return false
			} else {
				maxTxNum = ef.Max()
			}
			anyItem = true
		}
		return true
	})
	if found {
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], foundTxNum)
		var historyItem *filesItem
		var search filesItem
		search.startTxNum = foundStartTxNum
		search.endTxNum = foundEndTxNum
		if i := dc.files[History].Get(&search); i != nil {
			historyItem = i.(*filesItem)
		} else {
			return nil, false, 0, fmt.Errorf("no %s file found for [%x]", dc.d.filenameBase, key)
		}
		offset := historyItem.indexReader.Lookup2(txKey[:], key)
		g := historyItem.getter
		g.Reset(offset)
		if dc.d.compressVals {
			v, _ := g.Next(nil)
			return v, true, 0, nil
		}
		v, _ := g.NextUncompressed()
		return v, true, 0, nil
	} else if anyItem {
		return nil, false, maxTxNum, nil
	} else {
		return nil, true, 0, nil
	}
}

func (dc *DomainContext) MaxTxNum(key []byte) (bool, uint64) {
	var found bool
	var foundTxNum uint64
	dc.files[EfHistory].Descend(func(i btree.Item) bool {
		item := i.(*filesItem)
		if item.index.Empty() {
			return true
		}
		offset := item.indexReader.Lookup(key)
		g := item.getter
		g.Reset(offset)
		if k, _ := g.NextUncompressed(); bytes.Equal(k, key) {
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			found = true
			foundTxNum = ef.Max()
			return false
		}
		return true
	})
	if !found {
		return false, 0
	}
	return true, foundTxNum
}

type ReconItem struct {
	key         []byte
	txNum       uint64
	startTxNum  uint64
	endTxNum    uint64
	g           *compress.Getter
	startOffset uint64
	lastOffset  uint64
}

type ReconHeap []*ReconItem

func (rh ReconHeap) Len() int {
	return len(rh)
}

// Less (part of heap.Interface) compares two links. For persisted links, those with the lower block heights get evicted first. This means that more recently persisted links are preferred.
// For non-persisted links, those with the highest block heights get evicted first. This is to prevent "holes" in the block heights that may cause inability to
// insert headers in the ascending order of their block heights.
func (rh ReconHeap) Less(i, j int) bool {
	c := bytes.Compare(rh[i].key, rh[j].key)
	if c == 0 {
		return rh[i].txNum < rh[j].txNum
	}
	return c < 0
}

// Swap (part of heap.Interface) moves two links in the queue into each other's places. Note that each link has idx attribute that is getting adjusted during
// the swap. The idx attribute allows the removal of links from the middle of the queue (in case if links are getting invalidated due to
// failed verification of unavailability of parent headers)
func (rh ReconHeap) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
}

// Push (part of heap.Interface) places a new link onto the end of queue. Note that idx attribute is set to the correct position of the new link
func (rh *ReconHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	l := x.(*ReconItem)
	*rh = append(*rh, l)
}

// Pop (part of heap.Interface) removes the first link from the queue
func (rh *ReconHeap) Pop() interface{} {
	old := *rh
	n := len(old)
	x := old[n-1]
	*rh = old[0 : n-1]
	return x
}

type ScanIterator struct {
	dc             *DomainContext
	h              ReconHeap
	uptoTxNum      uint64
	hasNext        bool
	nextTxNum      uint64
	fromKey, toKey []byte
	key            []byte
	progress       uint64
	total          uint64
}

func (si *ScanIterator) advance() {
	for si.h.Len() > 0 {
		top := heap.Pop(&si.h).(*ReconItem)
		key := top.key
		val, offset := top.g.NextUncompressed()
		si.progress += offset - top.lastOffset
		top.lastOffset = offset
		if top.g.HasNext() {
			top.key, _ = top.g.NextUncompressed()
			if si.toKey == nil || bytes.Compare(top.key, si.toKey) <= 0 {
				heap.Push(&si.h, top)
			}
		}
		if !bytes.Equal(key, si.key) {
			si.key = key
			ef, _ := eliasfano32.ReadEliasFano(val)
			max := ef.Max()
			if max < si.uptoTxNum {
				si.nextTxNum = max
				si.hasNext = true
				return
			}
		}
	}
	si.hasNext = false
}

func (si *ScanIterator) HasNext() bool {
	return si.hasNext
}

func (si *ScanIterator) Next() (uint64, uint64) {
	n, p := si.nextTxNum, si.progress
	si.advance()
	return n, p
}

func (si *ScanIterator) Total() uint64 {
	return si.total
}

func (dc *DomainContext) iterateReconTxs(fromKey, toKey []byte, uptoTxNum uint64) *ScanIterator {
	var si ScanIterator
	dc.files[EfHistory].Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		g := item.getter
		for g.HasNext() {
			key, offset := g.NextUncompressed()
			if fromKey == nil || bytes.Compare(key, fromKey) > 0 {
				heap.Push(&si.h, &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: g, txNum: ^item.endTxNum, key: key, startOffset: offset, lastOffset: offset})
				break
			} else {
				g.SkipUncompressed()
			}
		}
		si.total += uint64(item.decompressor.Size())
		return true
	})
	si.dc = dc
	si.fromKey = fromKey
	si.toKey = toKey
	si.uptoTxNum = uptoTxNum
	si.advance()
	return &si
}

type HistoryIterator struct {
	dc             *DomainContext
	h              ReconHeap
	txNum          uint64
	key, val       []byte
	hasNext        bool
	compressVals   bool
	fromKey, toKey []byte
	progress       uint64
	total          uint64
}

func (hi *HistoryIterator) advance() {
	for hi.h.Len() > 0 {
		top := heap.Pop(&hi.h).(*ReconItem)
		key := top.key
		val, offset := top.g.NextUncompressed()
		hi.progress += offset - top.lastOffset
		top.lastOffset = offset
		if top.g.HasNext() {
			top.key, _ = top.g.NextUncompressed()
			if hi.toKey == nil || bytes.Compare(top.key, hi.toKey) <= 0 {
				heap.Push(&hi.h, top)
			}
		}
		if !bytes.Equal(hi.key, key) {
			ef, _ := eliasfano32.ReadEliasFano(val)
			if n, ok := ef.Search(hi.txNum); ok {
				hi.key = key
				var txKey [8]byte
				binary.BigEndian.PutUint64(txKey[:], n)
				var historyItem *filesItem
				var search filesItem
				search.startTxNum = top.startTxNum
				search.endTxNum = top.endTxNum
				if i := hi.dc.files[History].Get(&search); i != nil {
					historyItem = i.(*filesItem)
				} else {
					panic(fmt.Errorf("no %s file found for [%x]", hi.dc.d.filenameBase, hi.key))
				}
				offset := historyItem.indexReader.Lookup2(txKey[:], hi.key)
				g := historyItem.getter
				g.Reset(offset)
				if hi.compressVals {
					hi.val, _ = g.Next(nil)
				} else {
					hi.val, _ = g.NextUncompressed()
				}
				hi.hasNext = true
				return
			}
		}
	}
	hi.hasNext = false
}

func (hi *HistoryIterator) HasNext() bool {
	return hi.hasNext
}

func (hi *HistoryIterator) Next() ([]byte, []byte, uint64) {
	k, v, p := hi.key, hi.val, hi.progress
	hi.advance()
	return k, v, p
}

func (hi *HistoryIterator) Total() uint64 {
	return hi.total
}

// Creates iterator that provides history values for the state just before transaction txNum
func (dc *DomainContext) iterateHistoryBeforeTxNum(fromKey, toKey []byte, txNum uint64) *HistoryIterator {
	var hi HistoryIterator
	heap.Init(&hi.h)
	dc.files[EfHistory].Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		g := item.getter
		g.Reset(0)
		for g.HasNext() {
			key, offset := g.NextUncompressed()
			if fromKey == nil || bytes.Compare(key, fromKey) > 0 {
				heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
				break
			} else {
				g.SkipUncompressed()
			}
		}
		hi.total += uint64(item.decompressor.Size())
		return true
	})
	hi.dc = dc
	hi.compressVals = dc.d.compressVals
	hi.txNum = txNum
	hi.fromKey = fromKey
	hi.toKey = toKey
	hi.advance()
	return &hi
}
