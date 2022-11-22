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
	"fmt"
	"path/filepath"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
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

func (l *LocalityIndex) BuildMissedIndices(ctx context.Context, toStep uint64, h *History) error {
	defer h.EnableMadvNormalReadAhead().DisableReadAhead()

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
			log.Info("[LocalityIndex] build", "name", h.filenameBase, "k", fmt.Sprintf("%x", k), "progress", fmt.Sprintf("%.2f%%", ((float64(progress)/total)*100)/2))
		}
	}

	lFName := fmt.Sprintf("%s.%d-%d.l", h.filenameBase, fromStep, toStep)
	lFPath := filepath.Join(h.dir, lFName)
	comp, err := compress.NewCompressor(ctx, "", lFPath, h.tmpdir, compress.MinPatternScore, h.workers, log.LvlTrace)
	if err != nil {
		return fmt.Errorf("create %s compressor: %w", h.filenameBase, err)
	}
	defer comp.Close()

	it = h.MakeContext().iterateKeysLocality(nil, nil, toStep*h.aggregationStep)

	keys := etl.NewCollector("", h.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	bm := make([]byte, 4096)
	total = float64(it.Total())
	for it.HasNext() {
		k, steps, progress := it.Next()
		freezeLen, err := steps.FreezeTo(bm)
		if err != nil {
			return err
		}

		if err = comp.AddUncompressedWord(bm[:freezeLen]); err != nil {
			return err
		}

		_ = keys.Collect(k, nil)

		select {
		default:
		case <-logEvery.C:
			log.Info("[LocalityIndex] build", "name", h.filenameBase, "k", fmt.Sprintf("%x", k), "progress", fmt.Sprintf("%.2f%%", 50+((float64(progress)/total)*100)/2))
		}
	}
	if err = comp.Compress(); err != nil {
		return err
	}
	comp.Close()

	dec, err := compress.NewDecompressor(lFPath)
	if err != nil {
		return fmt.Errorf("open idx: %w", err)
	}
	dataGetter := dec.MakeGetter()

	fName := fmt.Sprintf("%s.%d-%d.li", h.filenameBase, fromStep, toStep)
	idxPath := filepath.Join(h.dir, fName)

	var rs *recsplit.RecSplit
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.tmpdir,
		IndexFile:  idxPath,
	}); err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	for {
		var lastKey []byte
		var pos uint64
		_ = keys.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {

			if err = rs.AddKey(k, pos); err != nil {
				return err
			}
			pos = dataGetter.Skip()

			select {
			default:
			case <-logEvery.C:
				log.Info("[LocalityIndex] build .li file", "name", h.filenameBase, "k", fmt.Sprintf("%x", k))
			}
			return nil
		}, etl.TransformArgs{})
		if err = rs.AddKey(lastKey, pos); err != nil {
			return err
		}

		if err = rs.Build(); err != nil {
			if rs.Collision() {
				panic("TODO: need implement graceful retry - collector does delete files after load")
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
	l.files = &filesItem{index: idx, decompressor: dec, startTxNum: fromStep * h.aggregationStep, endTxNum: toStep * h.aggregationStep}
	return nil
}

type LocalityIterator struct {
	hc        *HistoryContext
	h         ReconHeap
	nextKey   []byte
	fromKey   []byte
	key       []byte
	uptoTxNum uint64
	progress  uint64
	total     uint64
	hasNext   bool

	list     *roaring.Bitmap
	nextList *roaring.Bitmap
}

func (si *LocalityIterator) advance() {
	for si.h.Len() > 0 {
		top := heap.Pop(&si.h).(*ReconItem)
		key := top.key
		_, offset := top.g.NextUncompressed()
		si.progress += offset - top.lastOffset
		top.lastOffset = offset
		if top.g.HasNext() {
			top.key, _ = top.g.NextUncompressed()
			heap.Push(&si.h, top)
		}

		inStep := uint32(top.startTxNum / si.hc.h.aggregationStep)
		if !bytes.Equal(key, si.key) {
			if si.key == nil {
				si.key = key
				si.list.Add(inStep)
				//fmt.Printf("it1: %x, %d, %d\n", key, inStep, si.list.ToArray())
				continue
			}
			si.nextKey = si.key
			si.nextList, si.list = si.list, si.nextList

			si.list.Clear()
			si.key = key
			si.list.Add(inStep)
			//fmt.Printf("it2: %x, %d, %d\n", key, inStep, si.list.ToArray())
			//fmt.Printf("it2 next: %x, %d\n", si.nextKey, si.nextList.ToArray())

			si.hasNext = true
			return
		}
		si.list.Add(inStep)
		//fmt.Printf("it3: %x, %d, %d\n", key, inStep, si.list.ToArray())
	}
	si.hasNext = false
}

func (si *LocalityIterator) HasNext() bool { return si.hasNext }
func (si *LocalityIterator) Total() uint64 { return si.total }

func (si *LocalityIterator) Next() ([]byte, *roaring.Bitmap, uint64) {
	k, n, p := si.nextKey, si.nextList, si.progress
	si.advance()
	return k, n, p
}

func (hc *HistoryContext) iterateKeysLocality(fromKey, toKey []byte, uptoTxNum uint64) *LocalityIterator {
	si := &LocalityIterator{hc: hc, fromKey: fromKey, uptoTxNum: uptoTxNum, list: bitmapdb.NewBitmap(), nextList: bitmapdb.NewBitmap()}
	hc.indexFiles.Ascend(func(item ctxItem) bool {
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
