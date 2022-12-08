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
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

// Algorithms for reconstituting the state from state history

type ReconItem struct {
	g           *compress.Getter
	key         []byte
	txNum       uint64
	startTxNum  uint64
	endTxNum    uint64
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
	old[n-1] = nil
	*rh = old[0 : n-1]
	return x
}

type ScanIteratorInc struct {
	g         *compress.Getter
	nextKey   []byte
	key       []byte
	uptoTxNum uint64
	nextTxNum uint64
	nextDel   bool
	total     uint64
	hasNext   bool
}

func (sii *ScanIteratorInc) advance() {
	if !sii.hasNext {
		return
	}
	if sii.key == nil {
		sii.hasNext = false
		return
	}
	val, _ := sii.g.NextUncompressed()
	max := eliasfano32.Max(val)
	sii.nextKey = sii.key
	sii.nextTxNum = max
	if max < sii.uptoTxNum {
		sii.nextDel = false
	} else {
		sii.nextDel = true
	}
	if sii.g.HasNext() {
		sii.key, _ = sii.g.NextUncompressed()
	} else {
		sii.key = nil
	}
}

func (sii *ScanIteratorInc) HasNext() bool {
	return sii.hasNext
}

func (si *ScanIteratorInc) Next() ([]byte, uint64, bool) {
	k, n, d := si.nextKey, si.nextTxNum, si.nextDel
	si.advance()
	return k, n, d
}

func (sii *ScanIteratorInc) Total() uint64 {
	return sii.total
}

func (hs *HistoryStep) iterateTxs(uptoTxNum uint64) *ScanIteratorInc {
	var sii ScanIteratorInc
	sii.g = hs.indexFile.getter
	sii.g.Reset(0)
	if sii.g.HasNext() {
		sii.key, _ = sii.g.NextUncompressed()
		sii.total = uint64(hs.indexFile.getter.Size())
		sii.uptoTxNum = uptoTxNum
		sii.hasNext = true
	} else {
		sii.hasNext = false
	}
	sii.advance()
	return &sii
}

type HistoryIteratorInc struct {
	uptoTxNum    uint64
	indexG       *compress.Getter
	historyG     *compress.Getter
	r            *recsplit.IndexReader
	key          []byte
	nextKey      []byte
	nextVal      []byte
	nextTxNum    uint64
	hasNext      bool
	compressVals bool
}

func (hs *HistoryStep) interateHistoryBeforeTxNum(txNum uint64) *HistoryIteratorInc {
	var hii HistoryIteratorInc
	hii.indexG = hs.indexFile.getter
	hii.historyG = hs.historyFile.getter
	hii.r = hs.historyFile.reader
	hii.compressVals = hs.compressVals
	hii.indexG.Reset(0)
	if hii.indexG.HasNext() {
		hii.key, _ = hii.indexG.NextUncompressed()
		hii.uptoTxNum = txNum
		hii.hasNext = true
	} else {
		hii.hasNext = false
	}
	hii.advance()
	return &hii
}

func (hii *HistoryIteratorInc) advance() {
	if !hii.hasNext {
		return
	}
	for hii.key != nil {
		val, _ := hii.indexG.NextUncompressed()
		hii.nextKey = hii.key
		key := hii.key
		if hii.indexG.HasNext() {
			hii.key, _ = hii.indexG.NextUncompressed()
		} else {
			hii.key = nil
		}
		ef, _ := eliasfano32.ReadEliasFano(val)
		n, ok := ef.Search(hii.uptoTxNum)
		if ok {
			var txKey [8]byte
			binary.BigEndian.PutUint64(txKey[:], n)
			offset := hii.r.Lookup2(txKey[:], key)
			hii.historyG.Reset(offset)
			if hii.compressVals {
				hii.nextVal, _ = hii.historyG.Next(nil)
			} else {
				hii.nextVal, _ = hii.historyG.NextUncompressed()
			}
			hii.nextTxNum = n
			break
		}
	}
	if hii.key == nil {
		hii.hasNext = false
	}
}

func (hii *HistoryIteratorInc) HasNext() bool {
	return hii.hasNext
}

func (hii *HistoryIteratorInc) Next() ([]byte, []byte, uint64) {
	k, v, t := hii.nextKey, hii.nextVal, hii.nextTxNum
	hii.advance()
	return k, v, t
}
