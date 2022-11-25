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
	"path/filepath"
	"time"

	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
)

// LocalityIndex - has info in which .ef files exists given key
// Format: key -> bitmap(step_number_list)
// step_number_list is list of .ef files where exists given key
type LocalityIndex struct {
	//file         *filesItem
	//filenameBase string
	//dir          string // Directory where static files are created
	//tmpdir       string // Directory where static files are created

	files *filesItem
}

func (l *LocalityIndex) BuildMissedIndices(ctx context.Context, h *History) error {
	defer h.EnableMadvNormalReadAhead().DisableReadAhead()
	var toStep uint64
	h.files.Descend(func(item *filesItem) bool {
		if item.endTxNum-item.startTxNum == StepsInBiggestFile*h.aggregationStep {
			toStep = item.endTxNum / h.aggregationStep
			return false
		}
		return true
	})
	if toStep == 0 {
		return nil
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	fromStep := uint64(0)

	count := 0
	it := h.MakeContext().iterateKeysLocality(nil, nil, toStep*h.aggregationStep)
	total := float64(it.Total())
	for it.HasNext() {
		k, _, progress := it.Next()
		count++
		select {
		default:
		case <-logEvery.C:
			log.Info("[LocalityIndex] build step1", "name", h.filenameBase, "k", fmt.Sprintf("%x", k), "progress", fmt.Sprintf("%.2f%%", ((float64(progress)/total)*100)/2))
		}
	}
	log.Info("[LocalityIndex] keys amount", "total", count)

	fName := fmt.Sprintf("%s.%d-%d.li", h.filenameBase, fromStep, toStep)
	idxPath := filepath.Join(h.dir, fName)

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.tmpdir,
		IndexFile:  idxPath,
	})
	if err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	bm := make([]byte, 8)
	total = float64(it.Total())

	for {
		it = h.MakeContext().iterateKeysLocality(nil, nil, toStep*h.aggregationStep)
		for it.HasNext() {
			k, filesBitmap, progress := it.Next()
			binary.BigEndian.PutUint64(bm, filesBitmap)

			//if bytes.Equal(k, hex.MustDecodeString("e0a2bd4258d2768837baa26a28fe71dc079f84c7")) {
			//	fmt.Printf(".l file: %x, %b\n", k, filesBitmap)
			//}
			if err = rs.AddKey(k, filesBitmap); err != nil {
				return err
			}

			select {
			default:
			case <-logEvery.C:
				log.Info("[LocalityIndex] build step2", "name", h.filenameBase, "k", fmt.Sprintf("%x", k), "progress", fmt.Sprintf("%.2f%%", 50+((float64(progress)/total)*100)/2))
			}
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}

	idx, err := recsplit.OpenIndex(idxPath)
	if err != nil {
		return fmt.Errorf("open idx: %w", err)
	}
	l.files = &filesItem{index: idx, startTxNum: fromStep * h.aggregationStep, endTxNum: toStep * h.aggregationStep}
	return nil
}

type LocalityIterator struct {
	hc         *HistoryContext
	h          ReconHeap
	bitmap     uint64
	nextBitmap uint64
	nextKey    []byte
	fromKey    []byte
	key        []byte
	uptoTxNum  uint64
	progress   uint64
	total      uint64
	hasNext    bool
}

func (si *LocalityIterator) advance() {
	for si.h.Len() > 0 {
		top := heap.Pop(&si.h).(*ReconItem)
		key := top.key
		_, offset := top.g.NextUncompressed()
		si.progress += offset - top.lastOffset
		top.lastOffset = offset
		inStep := uint32(top.startTxNum / si.hc.h.aggregationStep)
		if top.g.HasNext() {
			top.key, _ = top.g.NextUncompressed()
			heap.Push(&si.h, top)
		}

		inFile := inStep / StepsInBiggestFile
		if inFile > 64 {
			panic("this index supports only up to 64 files")
		}

		if !bytes.Equal(key, si.key) {
			if si.key == nil {
				si.key = key
				si.bitmap |= 1 << inFile
				//if bytes.HasPrefix(key, hex.MustDecodeString("e0")) {
				//	fmt.Printf("it1: %x, step=%d, file=%d, %b\n", key, inStep, inFile, si.bitmap)
				//}
				continue
			}
			//if bytes.HasPrefix(key, hex.MustDecodeString("e0")) {
			//	fmt.Printf("it2 finish: %x, %b\n", si.key, si.bitmap)
			//}

			si.nextBitmap = si.bitmap
			si.nextKey = si.key
			si.bitmap = 0

			si.bitmap |= 1 << inFile
			si.key = key

			//if bytes.HasPrefix(key, hex.MustDecodeString("e0")) {
			//	fmt.Printf("it2 new: %x, step=%d, file=%d, %b\n", si.key, inStep, inFile, si.bitmap)
			//}

			si.hasNext = true
			return
		}
		si.bitmap |= 1 << inFile

		//if bytes.HasPrefix(key, hex.MustDecodeString("e0")) {
		//	fmt.Printf("it3 add: %x, step=%d, file=%d, %b\n", key, inStep, inFile, si.bitmap)
		//}
	}
	si.nextBitmap, si.bitmap = si.bitmap, si.nextBitmap
	si.nextKey = si.key
	si.hasNext = false
}

func (si *LocalityIterator) HasNext() bool { return si.hasNext }
func (si *LocalityIterator) Total() uint64 { return si.total }

func (si *LocalityIterator) Next() ([]byte, uint64, uint64) {
	si.advance()
	k, n, p := si.nextKey, si.nextBitmap, si.progress
	return k, n, p
}

func (hc *HistoryContext) iterateKeysLocality(fromKey, toKey []byte, uptoTxNum uint64) *LocalityIterator {
	si := &LocalityIterator{hc: hc, fromKey: fromKey, uptoTxNum: uptoTxNum}
	hc.indexFiles.Ascend(func(item ctxItem) bool {
		if (item.endTxNum-item.startTxNum)/hc.h.aggregationStep != StepsInBiggestFile {
			return false
		}
		if item.startTxNum > uptoTxNum {
			return false
		}
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
		si.total += uint64(item.getter.Size())
		return true
	})
	si.advance()
	return si
}
