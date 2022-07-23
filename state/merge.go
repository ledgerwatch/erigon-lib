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
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
)

func (d *Domain) endTxNumMinimax() uint64 {
	var minimax uint64
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		if max, ok := d.files[fType].Max(); ok {
			endTxNum := max.endTxNum
			if minimax == 0 || endTxNum < minimax {
				minimax = endTxNum
			}
		}
	}
	return minimax
}

func (ii *InvertedIndex) endTxNumMinimax() uint64 {
	var minimax uint64
	if max, ok := ii.files.Max(); ok {
		endTxNum := max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}

func (h *History) endTxNumMinimax() uint64 {
	minimax := h.InvertedIndex.endTxNumMinimax()
	if max, ok := h.files.Max(); ok {
		endTxNum := max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}

type DomainRanges struct {
	valuesStartTxNum  uint64
	valuesEndTxNum    uint64
	values            bool
	historyStartTxNum uint64
	historyEndTxNum   uint64
	history           bool
}

func (r DomainRanges) any() bool {
	return r.values || r.history
}

// findMergeRange assumes that all fTypes in d.files have items at least as far as maxEndTxNum
// That is why only Values type is inspected
func (d *Domain) findMergeRange(maxEndTxNum, maxSpan uint64) DomainRanges {
	var r DomainRanges
	d.files[Values].Ascend(func(item *filesItem) bool {
		if item.endTxNum > maxEndTxNum {
			return false
		}
		endStep := item.endTxNum / d.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * d.aggregationStep
		start := item.endTxNum - span
		if start < item.startTxNum {
			if !r.values || start < r.valuesStartTxNum {
				r.values = true
				r.valuesStartTxNum = start
				r.valuesEndTxNum = item.endTxNum
			}
		}
		return true
	})
	d.files[History1].Ascend(func(item *filesItem) bool {
		if item.endTxNum > maxEndTxNum {
			return false
		}
		endStep := item.endTxNum / d.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * d.aggregationStep
		if span > maxSpan {
			span = maxSpan
		}
		start := item.endTxNum - span
		if start < item.startTxNum {
			if !r.history || start < r.historyStartTxNum {
				r.history = true
				r.historyStartTxNum = start
				r.historyEndTxNum = item.endTxNum
			}
		}
		return true
	})
	return r
}

func (ii *InvertedIndex) findMergeRange(maxEndTxNum, maxSpan uint64) (bool, uint64, uint64) {
	var minFound bool
	var startTxNum, endTxNum uint64
	ii.files.Ascend(func(item *filesItem) bool {
		if item.endTxNum > maxEndTxNum {
			return false
		}
		endStep := item.endTxNum / ii.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * ii.aggregationStep
		if span > maxSpan {
			span = maxSpan
		}
		start := item.endTxNum - span
		if start < item.startTxNum {
			if !minFound || start < startTxNum {
				minFound = true
				startTxNum = start
				endTxNum = item.endTxNum
			}
		}
		return true
	})
	return minFound, startTxNum, endTxNum
}

type HistoryRanges struct {
	historyStartTxNum uint64
	historyEndTxNum   uint64
	history           bool
	indexStartTxNum   uint64
	indexEndTxNum     uint64
	index             bool
}

func (r HistoryRanges) any() bool {
	return r.history || r.index
}

func (h *History) findMergeRange(maxEndTxNum, maxSpan uint64) HistoryRanges {
	var r HistoryRanges
	r.index, r.indexStartTxNum, r.indexEndTxNum = h.InvertedIndex.findMergeRange(maxEndTxNum, maxSpan)
	h.files.Ascend(func(item *filesItem) bool {
		if item.endTxNum > maxEndTxNum {
			return false
		}
		endStep := item.endTxNum / h.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * h.aggregationStep
		if span > maxSpan {
			span = maxSpan
		}
		start := item.endTxNum - span
		if start < item.startTxNum {
			if !r.history || start < r.historyStartTxNum {
				r.history = true
				r.historyStartTxNum = start
				r.historyEndTxNum = item.endTxNum
			}
		}
		return true
	})
	return r
}

// staticFilesInRange returns list of static files with txNum in specified range [startTxNum; endTxNum)
// files are in the descending order of endTxNum
func (d *Domain) staticFilesInRange(r DomainRanges) ([][NumberOfTypes]*filesItem, int) {
	var files [][NumberOfTypes]*filesItem
	var startJ int
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		var startTxNum, endTxNum uint64
		if fType == Values {
			if !r.values {
				continue
			}
			startTxNum = r.valuesStartTxNum
			endTxNum = r.valuesEndTxNum
		} else {
			if !r.history {
				continue
			}
			startTxNum = r.historyStartTxNum
			endTxNum = r.historyEndTxNum
		}
		startJ = 0
		j := 0
		d.files[fType].Ascend(func(item *filesItem) bool {
			if item.startTxNum < startTxNum {
				startJ++
				return true
			}
			if item.endTxNum > endTxNum {
				return false
			}
			for j >= len(files) {
				files = append(files, [NumberOfTypes]*filesItem{})
			}
			files[j][fType] = item
			j++
			return true
		})
	}
	return files, startJ
}

func (ii *InvertedIndex) staticFilesInRange(startTxNum, endTxNum uint64) ([]*filesItem, int) {
	var files []*filesItem
	var startJ int
	ii.files.Ascend(func(item *filesItem) bool {
		if item.startTxNum < startTxNum {
			startJ++
			return true
		}
		if item.endTxNum > endTxNum {
			return false
		}
		files = append(files, item)
		return true
	})
	return files, startJ
}

func (h *History) staticFilesInRange(r HistoryRanges) (indexFiles []*filesItem, historyFiles []*filesItem, startJ int) {
	if r.index {
		indexFiles, startJ = h.InvertedIndex.staticFilesInRange(r.indexStartTxNum, r.indexEndTxNum)
	}
	if r.history {
		startJ = 0
		h.files.Ascend(func(item *filesItem) bool {
			if item.startTxNum < r.historyStartTxNum {
				startJ++
				return true
			}
			if item.endTxNum > r.historyEndTxNum {
				return false
			}
			historyFiles = append(historyFiles, item)
			return true
		})
	}
	return
}

func mergeEfs(preval, val, buf []byte) ([]byte, error) {
	preef, _ := eliasfano32.ReadEliasFano(preval)
	ef, _ := eliasfano32.ReadEliasFano(val)
	preIt := preef.Iterator()
	efIt := ef.Iterator()
	newEf := eliasfano32.NewEliasFano(preef.Count()+ef.Count(), ef.Max())
	for preIt.HasNext() {
		newEf.AddOffset(preIt.Next())
	}
	for efIt.HasNext() {
		newEf.AddOffset(efIt.Next())
	}
	newEf.Build()
	return newEf.AppendBytes(buf), nil
}

func (d *Domain) mergeFiles(files [][NumberOfTypes]*filesItem, r DomainRanges, maxSpan uint64) ([NumberOfTypes]*filesItem, error) {
	var outItems [NumberOfTypes]*filesItem
	var comp *compress.Compressor
	var decomp *compress.Decompressor
	var err error
	var closeItem bool = true
	defer func() {
		if closeItem {
			if comp != nil {
				comp.Close()
			}
			if decomp != nil {
				decomp.Close()
			}
			for fType := FileType(0); fType < NumberOfTypes; fType++ {
				outItem := outItems[fType]
				if outItem != nil {
					if outItem.decompressor != nil {
						outItem.decompressor.Close()
					}
					if outItem.index != nil {
						outItem.index.Close()
					}
					outItems[fType] = nil
				}
			}
		}
	}()
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		compressVal := d.compressVals && fType != EfHistory
		var startTxNum, endTxNum uint64
		if fType == Values {
			if !r.values {
				continue
			}
			startTxNum = r.valuesStartTxNum
			endTxNum = r.valuesEndTxNum
		} else {
			if !r.history {
				continue
			}
			startTxNum = r.historyStartTxNum
			endTxNum = r.historyEndTxNum
		}
		removeVals := fType == History1 && (endTxNum-startTxNum) == maxSpan
		tmpPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.tmp", d.filenameBase, fType.String(), startTxNum/d.aggregationStep, endTxNum/d.aggregationStep))
		datPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.dat", d.filenameBase, fType.String(), startTxNum/d.aggregationStep, endTxNum/d.aggregationStep))
		if removeVals {
			if comp, err = compress.NewCompressor(context.Background(), "merge", tmpPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
				return outItems, fmt.Errorf("merge %s history compressor: %w", d.filenameBase, err)
			}
		} else {
			if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
				return outItems, fmt.Errorf("merge %s history compressor: %w", d.filenameBase, err)
			}
		}
		var cp CursorHeap
		heap.Init(&cp)
		for _, filesByType := range files {
			item := filesByType[fType]
			if item == nil {
				continue
			}
			g := item.decompressor.MakeGetter()
			g.Reset(0)
			if g.HasNext() {
				key, _ := g.NextUncompressed()
				var val []byte
				if d.compressVals {
					val, _ = g.Next(nil)
				} else {
					val, _ = g.NextUncompressed()
				}
				heap.Push(&cp, &CursorItem{
					t:        FILE_CURSOR,
					dg:       g,
					key:      key,
					val:      val,
					endTxNum: item.endTxNum,
					reverse:  true,
				})
			}
		}
		count := 0
		// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
		// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
		// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
		// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
		// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
		var keyBuf, valBuf []byte
		for cp.Len() > 0 {
			lastKey := common.Copy(cp[0].key)
			lastVal := common.Copy(cp[0].val)
			var mergedOnce bool
			// Advance all the items that have this key (including the top)
			for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
				ci1 := cp[0]
				if mergedOnce && fType == EfHistory {
					if lastVal, err = mergeEfs(ci1.val, lastVal, nil); err != nil {
						return outItems, fmt.Errorf("merge %s efhistory: %w", d.filenameBase, err)
					}
				} else {
					mergedOnce = true
				}
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.NextUncompressed()
					if d.compressVals {
						ci1.val, _ = ci1.dg.Next(ci1.val[:0])
					} else {
						ci1.val, _ = ci1.dg.NextUncompressed()
					}
					heap.Fix(&cp, 0)
				} else {
					heap.Pop(&cp)
				}
			}
			var skip bool
			if fType == Values {
				if d.prefixLen > 0 {
					skip = startTxNum == 0 && len(lastVal) == 0 && len(lastKey) != d.prefixLen
				} else {
					// For the rest of types, empty value means deletion
					skip = startTxNum == 0 && len(lastVal) == 0
				}
			}
			if !skip {
				if keyBuf != nil && (d.prefixLen == 0 || len(keyBuf) != d.prefixLen || bytes.HasPrefix(lastKey, keyBuf)) {
					if err = comp.AddUncompressedWord(keyBuf); err != nil {
						return outItems, err
					}
					count++ // Only counting keys, not values
					if compressVal {
						if err = comp.AddWord(valBuf); err != nil {
							return outItems, err
						}
					} else {
						if err = comp.AddUncompressedWord(valBuf); err != nil {
							return outItems, err
						}
					}
				}
				keyBuf = append(keyBuf[:0], lastKey...)
				valBuf = append(valBuf[:0], lastVal...)
			}
		}
		if keyBuf != nil {
			if err = comp.AddUncompressedWord(keyBuf); err != nil {
				return outItems, err
			}
			count++ // Only counting keys, not values
			if compressVal {
				if err = comp.AddWord(valBuf); err != nil {
					return outItems, err
				}
			} else {
				if err = comp.AddUncompressedWord(valBuf); err != nil {
					return outItems, err
				}
			}
		}
		if err = comp.Compress(); err != nil {
			return outItems, err
		}
		comp.Close()
		comp = nil
		idxPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.idx", d.filenameBase, fType.String(), startTxNum/d.aggregationStep, endTxNum/d.aggregationStep))
		outItem := &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		outItems[fType] = outItem
		if removeVals {
			if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
				return outItems, fmt.Errorf("merge %s remove vals compressor: %w", d.filenameBase, err)
			}
			if decomp, err = compress.NewDecompressor(tmpPath); err != nil {
				return outItems, fmt.Errorf("merge %s remove vals decompressor %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			g := decomp.MakeGetter()
			var val []byte
			var count int
			g.Reset(0)
			for g.HasNext() {
				g.SkipUncompressed() // Skip key on on the first pass
				if compressVal {
					val, _ = g.Next(val[:0])
					if err = comp.AddWord(val); err != nil {
						return outItems, fmt.Errorf("merge %s remove vals add val %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
					}
				} else {
					val, _ = g.NextUncompressed()
					if err = comp.AddUncompressedWord(val); err != nil {
						return outItems, fmt.Errorf("merge %s remove vals add val %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
					}
				}
				count++
			}
			if err = comp.Compress(); err != nil {
				return outItems, err
			}
			comp.Close()
			comp = nil
			if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				return outItems, fmt.Errorf("merge %s remove vals decompressor(no val) %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			var rs *recsplit.RecSplit
			if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
				KeyCount:   count,
				Enums:      false,
				BucketSize: 2000,
				LeafSize:   8,
				TmpDir:     d.dir,
				StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
					0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
					0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
				IndexFile: idxPath,
			}); err != nil {
				return outItems, fmt.Errorf("merge %s remove vals recsplit %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			g1 := outItem.decompressor.MakeGetter()
			var key []byte
			for {
				g.Reset(0)
				g1.Reset(0)
				var lastOffset uint64
				for g.HasNext() {
					key, _ = g.NextUncompressed()
					g.Skip() // Skip value
					_, pos := g1.Next(nil)
					if err = rs.AddKey(key, lastOffset); err != nil {
						return outItems, fmt.Errorf("merge %s remove vals recsplit add key %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
					}
					lastOffset = pos
				}
				if err = rs.Build(); err != nil {
					if rs.Collision() {
						log.Info("Building reduceHistoryFiles. Collision happened. It's ok. Restarting...")
						rs.ResetNextSalt()
					} else {
						return outItems, fmt.Errorf("merge %s remove vals recsplit build %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
					}
				} else {
					break
				}
			}
			decomp.Close()
			decomp = nil
			if err = os.Remove(tmpPath); err != nil {
				return outItems, fmt.Errorf("merge %s remove tmp file %s: %w", d.filenameBase, tmpPath, err)
			}
			if outItem.index, err = recsplit.OpenIndex(idxPath); err != nil {
				return outItems, fmt.Errorf("merge %s open index %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
		} else {
			if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				return outItems, fmt.Errorf("merge %s decompressor %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			if outItem.index, err = buildIndex(outItem.decompressor, idxPath, d.dir, count, fType == History1 /* values */); err != nil {
				return outItems, fmt.Errorf("merge %s buildIndex %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
		}
	}
	closeItem = false
	return outItems, nil
}

func (ii *InvertedIndex) mergeFiles(files []*filesItem, startTxNum, endTxNum uint64, maxSpan uint64) (*filesItem, error) {
	var outItem *filesItem
	var comp *compress.Compressor
	var decomp *compress.Decompressor
	var err error
	var closeItem bool = true
	defer func() {
		if closeItem {
			if comp != nil {
				comp.Close()
			}
			if decomp != nil {
				decomp.Close()
			}
			if outItem != nil {
				if outItem.decompressor != nil {
					outItem.decompressor.Close()
				}
				if outItem.index != nil {
					outItem.index.Close()
				}
				outItem = nil
			}
		}
	}()
	datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep))
	if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, ii.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
		return nil, fmt.Errorf("merge %s inverted index compressor: %w", ii.filenameBase, err)
	}
	var cp CursorHeap
	heap.Init(&cp)
	for _, item := range files {
		g := item.decompressor.MakeGetter()
		g.Reset(0)
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
			heap.Push(&cp, &CursorItem{
				t:        FILE_CURSOR,
				dg:       g,
				key:      key,
				val:      val,
				endTxNum: item.endTxNum,
				reverse:  true,
			})
		}
	}
	count := 0
	// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
	// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
	// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
	// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
	// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
	var keyBuf, valBuf []byte
	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		var mergedOnce bool
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := cp[0]
			if mergedOnce {
				if lastVal, err = mergeEfs(ci1.val, lastVal, nil); err != nil {
					return nil, fmt.Errorf("merge %s inverted index: %w", ii.filenameBase, err)
				}
			} else {
				mergedOnce = true
			}
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.NextUncompressed()
				ci1.val, _ = ci1.dg.NextUncompressed()
				heap.Fix(&cp, 0)
			} else {
				heap.Pop(&cp)
			}
		}
		if keyBuf != nil {
			if err = comp.AddUncompressedWord(keyBuf); err != nil {
				return nil, err
			}
			count++ // Only counting keys, not values
			if err = comp.AddUncompressedWord(valBuf); err != nil {
				return nil, err
			}
		}
		keyBuf = append(keyBuf[:0], lastKey...)
		valBuf = append(valBuf[:0], lastVal...)
	}
	if keyBuf != nil {
		if err = comp.AddUncompressedWord(keyBuf); err != nil {
			return nil, err
		}
		count++ // Only counting keys, not values
		if err = comp.AddUncompressedWord(valBuf); err != nil {
			return nil, err
		}
	}
	if err = comp.Compress(); err != nil {
		return nil, err
	}
	comp.Close()
	comp = nil
	idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep))
	outItem = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
	if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
		return nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
	}
	if outItem.index, err = buildIndex(outItem.decompressor, idxPath, ii.dir, count, false /* values */); err != nil {
		return nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
	}
	closeItem = false
	return outItem, nil
}

func (h *History) mergeFiles(indexFiles, historyFiles []*filesItem, r HistoryRanges, maxSpan uint64) (indexIn, historyIn *filesItem, err error) {
	if !r.any() {
		return nil, nil, nil
	}
	var closeIndex = true
	defer func() {
		if closeIndex {
			if indexIn != nil {
				indexIn.decompressor.Close()
				indexIn.index.Close()
			}
		}
	}()
	if indexIn, err = h.InvertedIndex.mergeFiles(indexFiles, r.indexStartTxNum, r.indexEndTxNum, maxSpan); err != nil {
		return nil, nil, err
	}
	var comp *compress.Compressor
	var decomp *compress.Decompressor
	var rs *recsplit.RecSplit
	var index *recsplit.Index
	var closeItem bool = true
	defer func() {
		if closeItem {
			if comp != nil {
				comp.Close()
			}
			if decomp != nil {
				decomp.Close()
			}
			if rs != nil {
				rs.Close()
			}
			if index != nil {
				index.Close()
			}
			if historyIn != nil {
				if historyIn.decompressor != nil {
					historyIn.decompressor.Close()
				}
				if historyIn.index != nil {
					historyIn.index.Close()
				}
			}
		}
	}()
	datPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, r.historyStartTxNum/h.aggregationStep, r.historyEndTxNum/h.aggregationStep))
	idxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, r.historyStartTxNum/h.aggregationStep, r.historyEndTxNum/h.aggregationStep))
	if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, h.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
		return nil, nil, fmt.Errorf("merge %s history compressor: %w", h.filenameBase, err)
	}
	var cp CursorHeap
	heap.Init(&cp)
	for i, item := range indexFiles {
		g := item.decompressor.MakeGetter()
		g.Reset(0)
		if g.HasNext() {
			g2 := historyFiles[i].decompressor.MakeGetter()
			key, _ := g.NextUncompressed()
			val, _ := g.NextUncompressed()
			heap.Push(&cp, &CursorItem{
				t:        FILE_CURSOR,
				dg:       g,
				dg2:      g2,
				key:      key,
				val:      val,
				endTxNum: item.endTxNum,
				reverse:  false,
			})
		}
	}
	count := 0
	// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
	// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
	// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
	// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
	// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
	var valBuf []byte
	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := cp[0]
			ef, _ := eliasfano32.ReadEliasFano(ci1.val)
			for i := uint64(0); i < ef.Count(); i++ {
				if h.compressVals {
					valBuf, _ = ci1.dg2.Next(valBuf[:0])
					if err = comp.AddWord(valBuf); err != nil {
						return nil, nil, err
					}
				} else {
					valBuf, _ = ci1.dg2.NextUncompressed()
					if err = comp.AddUncompressedWord(valBuf); err != nil {
						return nil, nil, err
					}
				}
			}
			count += int(ef.Count())
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.NextUncompressed()
				ci1.val, _ = ci1.dg.NextUncompressed()
				heap.Fix(&cp, 0)
			} else {
				heap.Remove(&cp, 0)
			}
		}
	}
	if err = comp.Compress(); err != nil {
		return nil, nil, err
	}
	comp.Close()
	comp = nil
	if decomp, err = compress.NewDecompressor(datPath); err != nil {
		return nil, nil, err
	}
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.dir,
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		IndexFile: idxPath,
	}); err != nil {
		return nil, nil, fmt.Errorf("create recsplit: %w", err)
	}
	var historyKey []byte
	var txKey [8]byte
	var valOffset uint64
	g := indexIn.decompressor.MakeGetter()
	g2 := decomp.MakeGetter()
	var keyBuf []byte
	for {
		g.Reset(0)
		g2.Reset(0)
		valOffset = 0
		for g.HasNext() {
			keyBuf, _ = g.NextUncompressed()
			valBuf, _ = g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(valBuf)
			efIt := ef.Iterator()
			for efIt.HasNext() {
				txNum := efIt.Next()
				binary.BigEndian.PutUint64(txKey[:], txNum)
				historyKey = append(append(historyKey[:0], txKey[:]...), keyBuf...)
				if err = rs.AddKey(historyKey, valOffset); err != nil {
					return nil, nil, err
				}
				if h.compressVals {
					valOffset = g2.Skip()
				} else {
					valOffset = g2.SkipUncompressed()
				}
			}
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return nil, nil, fmt.Errorf("build %s idx: %w", h.filenameBase, err)
			}
		} else {
			break
		}
	}
	rs.Close()
	rs = nil
	if index, err = recsplit.OpenIndex(idxPath); err != nil {
		return nil, nil, fmt.Errorf("open %s idx: %w", h.filenameBase, err)
	}
	historyIn = &filesItem{startTxNum: r.historyStartTxNum, endTxNum: r.historyEndTxNum, decompressor: decomp, index: index}
	closeItem = false
	closeIndex = false
	return
}

func (d *Domain) integrateMergedFiles(outs [][NumberOfTypes]*filesItem, in [NumberOfTypes]*filesItem) {
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		d.files[fType].ReplaceOrInsert(in[fType])
		for _, out := range outs {
			if out[fType] == nil {
				continue
			}
			d.files[fType].Delete(out[fType])
			out[fType].decompressor.Close()
			out[fType].index.Close()
		}
	}
}

func (ii *InvertedIndex) integrateMergedFiles(outs []*filesItem, in *filesItem) {
	ii.files.ReplaceOrInsert(in)
	for _, out := range outs {
		ii.files.Delete(out)
		out.decompressor.Close()
		fmt.Printf("Closed decomp1 %d-%d\n", out.startTxNum, out.endTxNum)
		out.index.Close()
		fmt.Printf("Closed index1 %d-%d\n", out.startTxNum, out.endTxNum)
	}
}

func (h *History) integrateMergedFiles(indexOuts, historyOuts []*filesItem, indexIn, historyIn *filesItem) {
	h.InvertedIndex.integrateMergedFiles(indexOuts, indexIn)
	h.files.ReplaceOrInsert(historyIn)
	for _, out := range historyOuts {
		h.files.Delete(out)
		out.decompressor.Close()
		out.index.Close()
	}
}

func (d *Domain) deleteFiles(outs [][NumberOfTypes]*filesItem) error {
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		for _, out := range outs {
			if out[fType] == nil {
				continue
			}
			datPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.dat", d.filenameBase, fType.String(), out[fType].startTxNum/d.aggregationStep, out[fType].endTxNum/d.aggregationStep))
			if err := os.Remove(datPath); err != nil {
				return err
			}
			idxPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.idx", d.filenameBase, fType.String(), out[fType].startTxNum/d.aggregationStep, out[fType].endTxNum/d.aggregationStep))
			if err := os.Remove(idxPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ii *InvertedIndex) deleteFiles(outs []*filesItem) error {
	for _, out := range outs {
		datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, out.startTxNum/ii.aggregationStep, out.endTxNum/ii.aggregationStep))
		if err := os.Remove(datPath); err != nil {
			return err
		}
		idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, out.startTxNum/ii.aggregationStep, out.endTxNum/ii.aggregationStep))
		if err := os.Remove(idxPath); err != nil {
			return err
		}
	}
	return nil
}

func (h *History) deleteFiles(indexOuts, historyOuts []*filesItem) error {
	if err := h.InvertedIndex.deleteFiles(indexOuts); err != nil {
		return err
	}
	for _, out := range historyOuts {
		datPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, out.startTxNum/h.aggregationStep, out.endTxNum/h.aggregationStep))
		if err := os.Remove(datPath); err != nil {
			return err
		}
		idxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, out.startTxNum/h.aggregationStep, out.endTxNum/h.aggregationStep))
		if err := os.Remove(idxPath); err != nil {
			return err
		}
	}
	return nil
}
