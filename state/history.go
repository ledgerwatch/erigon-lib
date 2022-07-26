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
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

type History struct {
	*InvertedIndex
	valsTable     string
	settingsTable string
	files         *btree.BTreeG[*filesItem]
	compressVals  bool
}

func NewHistory(
	dir string,
	aggregationStep uint64,
	filenameBase string,
	keysTable string,
	indexTable string,
	valsTable string,
	settingsTable string,
	compressVals bool,
) (*History, error) {
	var h History
	var err error
	h.InvertedIndex, err = NewInvertedIndex(dir, aggregationStep, filenameBase, keysTable, indexTable)
	if err != nil {
		return nil, err
	}
	h.valsTable = valsTable
	h.settingsTable = settingsTable
	h.files = btree.NewG[*filesItem](32, filesItemLess)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	h.scanStateFiles(files)
	if err = h.openFiles(); err != nil {
		return nil, err
	}
	h.compressVals = compressVals
	return &h, nil
}

func (h *History) scanStateFiles(files []fs.DirEntry) {
	re := regexp.MustCompile(h.filenameBase + ".([0-9]+)-([0-9]+).(v|vi)")
	var err error
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				log.Warn("File ignored by inverted index scan, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startTxNum, endTxNum uint64
		if startTxNum, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endTxNum, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startTxNum > endTxNum {
			log.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}
		var item = &filesItem{startTxNum: startTxNum * h.aggregationStep, endTxNum: endTxNum * h.aggregationStep}
		var foundI *filesItem
		h.files.AscendGreaterOrEqual(&filesItem{startTxNum: endTxNum * h.aggregationStep, endTxNum: endTxNum * h.aggregationStep}, func(it *filesItem) bool {
			if it.endTxNum == endTxNum {
				foundI = it
			}
			return false
		})
		if foundI == nil || foundI.startTxNum > startTxNum {
			//log.Info("Load state file", "name", name, "startTxNum", startTxNum*ii.aggregationStep, "endTxNum", endTxNum*ii.aggregationStep)
			h.files.ReplaceOrInsert(item)
		}
	}
}

func (h *History) openFiles() error {
	var totalKeys uint64
	var err error
	h.files.Ascend(func(item *filesItem) bool {
		datPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep))
		if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
			return false
		}
		idxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep))
		if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
			return false
		}
		totalKeys += item.index.KeyCount()
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (h *History) closeFiles() {
	h.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		if item.index != nil {
			item.index.Close()
		}
		return true
	})
}

func (h *History) Close() {
	h.InvertedIndex.closeFiles()
	h.closeFiles()
}

func (h *History) AddPrevValue(key, original []byte) error {
	historyKey := make([]byte, len(key)+8)
	copy(historyKey, key)
	if len(original) > 0 {
		val, err := h.tx.GetOne(h.settingsTable, historyValCountKey)
		if err != nil {
			return err
		}
		var valNum uint64
		if len(val) > 0 {
			valNum = binary.BigEndian.Uint64(val)
		}
		valNum++
		binary.BigEndian.PutUint64(historyKey[len(key):], valNum)
		if err = h.tx.Put(h.settingsTable, historyValCountKey, historyKey[len(key):]); err != nil {
			return err
		}
		if err = h.tx.Put(h.valsTable, historyKey[len(key):], original); err != nil {
			return err
		}
	}
	if err := h.add(historyKey, key); err != nil {
		return err
	}
	return nil
}

type HistoryCollation struct {
	historyPath  string
	historyComp  *compress.Compressor
	historyCount int
	indexBitmaps map[string]*roaring64.Bitmap
}

func (c HistoryCollation) Close() {
	if c.historyComp != nil {
		c.Close()
	}
}

func (h *History) collate(step, txFrom, txTo uint64, roTx kv.Tx) (HistoryCollation, error) {
	var historyComp *compress.Compressor
	var err error
	closeComp := true
	defer func() {
		if closeComp {
			if historyComp != nil {
				historyComp.Close()
			}
		}
	}()
	historyPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, step, step+1))
	if historyComp, err = compress.NewCompressor(context.Background(), "collate history", historyPath, h.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history compressor: %w", h.filenameBase, err)
	}
	keysCursor, err := roTx.CursorDupSort(h.keysTable)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer keysCursor.Close()
	indexBitmaps := map[string]*roaring64.Bitmap{}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var val []byte
	var k, v []byte
	for k, v, err = keysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = keysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		var bitmap *roaring64.Bitmap
		var ok bool
		if bitmap, ok = indexBitmaps[string(v[:len(v)-8])]; !ok {
			bitmap = roaring64.New()
			indexBitmaps[string(v[:len(v)-8])] = bitmap
		}
		bitmap.Add(txNum)
	}
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("iterate over %s history cursor: %w", h.filenameBase, err)
	}
	keys := make([]string, 0, len(indexBitmaps))
	for key := range indexBitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	historyCount := 0
	for _, key := range keys {
		bitmap := indexBitmaps[key]
		it := bitmap.Iterator()
		for it.HasNext() {
			txNum := it.Next()
			binary.BigEndian.PutUint64(txKey[:], txNum)
			v, err := keysCursor.SeekBothRange(txKey[:], []byte(key))
			if err != nil {
				return HistoryCollation{}, err
			}
			if bytes.HasPrefix(v, []byte(key)) {
				valNum := binary.BigEndian.Uint64(v[len(v)-8:])
				if valNum == 0 {
					val = nil
				} else {
					if val, err = roTx.GetOne(h.valsTable, v[len(v)-8:]); err != nil {
						return HistoryCollation{}, fmt.Errorf("get %s history val [%x]=>%d: %w", h.filenameBase, k, valNum, err)
					}
				}
				if err = historyComp.AddUncompressedWord(val); err != nil {
					return HistoryCollation{}, fmt.Errorf("add %s history val [%x]=>[%x]: %w", h.filenameBase, k, val, err)
				}
				historyCount++
			}
		}
	}
	closeComp = false
	return HistoryCollation{
		historyPath:  historyPath,
		historyComp:  historyComp,
		historyCount: historyCount,
		indexBitmaps: indexBitmaps,
	}, nil
}

type HistoryFiles struct {
	historyDecomp   *compress.Decompressor
	historyIdx      *recsplit.Index
	efHistoryDecomp *compress.Decompressor
	efHistoryIdx    *recsplit.Index
}

func (sf HistoryFiles) Close() {
	if sf.historyDecomp != nil {
		sf.historyDecomp.Close()
	}
	if sf.historyIdx != nil {
		sf.historyIdx.Close()
	}
	if sf.efHistoryDecomp != nil {
		sf.efHistoryDecomp.Close()
	}
	if sf.efHistoryIdx != nil {
		sf.efHistoryIdx.Close()
	}
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (h *History) buildFiles(step uint64, collation HistoryCollation) (HistoryFiles, error) {
	historyComp := collation.historyComp
	var historyDecomp, efHistoryDecomp *compress.Decompressor
	var historyIdx, efHistoryIdx *recsplit.Index
	var efHistoryComp *compress.Compressor
	var rs *recsplit.RecSplit
	closeComp := true
	defer func() {
		if closeComp {
			if historyComp != nil {
				historyComp.Close()
			}
			if historyDecomp != nil {
				historyDecomp.Close()
			}
			if historyIdx != nil {
				historyIdx.Close()
			}
			if efHistoryComp != nil {
				efHistoryComp.Close()
			}
			if efHistoryDecomp != nil {
				efHistoryDecomp.Close()
			}
			if efHistoryIdx != nil {
				efHistoryIdx.Close()
			}
			if rs != nil {
				rs.Close()
			}
		}
	}()
	historyIdxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, step, step+1))
	if err := historyComp.Compress(); err != nil {
		return HistoryFiles{}, fmt.Errorf("compress %s history: %w", h.filenameBase, err)
	}
	historyComp.Close()
	historyComp = nil
	var err error
	if historyDecomp, err = compress.NewDecompressor(collation.historyPath); err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s history decompressor: %w", h.filenameBase, err)
	}
	// Build history ef
	efHistoryPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.ef", h.filenameBase, step, step+1))
	efHistoryComp, err = compress.NewCompressor(context.Background(), "ef history", efHistoryPath, h.dir, compress.MinPatternScore, 1, log.LvlDebug)
	if err != nil {
		return HistoryFiles{}, fmt.Errorf("create %s ef history compressor: %w", h.filenameBase, err)
	}
	var buf []byte
	keys := make([]string, 0, len(collation.indexBitmaps))
	for key := range collation.indexBitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err = efHistoryComp.AddUncompressedWord([]byte(key)); err != nil {
			return HistoryFiles{}, fmt.Errorf("add %s ef history key [%x]: %w", h.InvertedIndex.filenameBase, key, err)
		}
		bitmap := collation.indexBitmaps[key]
		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			txNum := it.Next()
			ef.AddOffset(txNum)
		}
		ef.Build()
		buf = ef.AppendBytes(buf[:0])
		if err = efHistoryComp.AddUncompressedWord(buf); err != nil {
			return HistoryFiles{}, fmt.Errorf("add %s ef history val: %w", h.filenameBase, err)
		}
	}
	if err = efHistoryComp.Compress(); err != nil {
		return HistoryFiles{}, fmt.Errorf("compress %s ef history: %w", h.filenameBase, err)
	}
	efHistoryComp.Close()
	efHistoryComp = nil
	if efHistoryDecomp, err = compress.NewDecompressor(efHistoryPath); err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s ef history decompressor: %w", h.filenameBase, err)
	}
	efHistoryIdxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.efi", h.filenameBase, step, step+1))
	if efHistoryIdx, err = buildIndex(efHistoryDecomp, efHistoryIdxPath, h.dir, len(keys), false /* values */); err != nil {
		return HistoryFiles{}, fmt.Errorf("build %s ef history idx: %w", h.filenameBase, err)
	}
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   collation.historyCount,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.dir,
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		IndexFile: historyIdxPath,
	}); err != nil {
		return HistoryFiles{}, fmt.Errorf("create recsplit: %w", err)
	}
	var historyKey []byte
	var txKey [8]byte
	var valOffset uint64
	g := historyDecomp.MakeGetter()
	for {
		g.Reset(0)
		valOffset = 0
		for _, key := range keys {
			bitmap := collation.indexBitmaps[key]
			it := bitmap.Iterator()
			for it.HasNext() {
				txNum := it.Next()
				binary.BigEndian.PutUint64(txKey[:], txNum)
				historyKey = append(append(historyKey[:0], txKey[:]...), key...)
				if err = rs.AddKey(historyKey, valOffset); err != nil {
					return HistoryFiles{}, fmt.Errorf("add %s history idx [%x]: %w", h.filenameBase, historyKey, err)
				}
				valOffset = g.Skip()
			}
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return HistoryFiles{}, fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	rs.Close()
	rs = nil
	if historyIdx, err = recsplit.OpenIndex(historyIdxPath); err != nil {
		return HistoryFiles{}, fmt.Errorf("open idx: %w", err)
	}
	closeComp = false
	return HistoryFiles{
		historyDecomp:   historyDecomp,
		historyIdx:      historyIdx,
		efHistoryDecomp: efHistoryDecomp,
		efHistoryIdx:    efHistoryIdx,
	}, nil
}

func (h *History) integrateFiles(sf HistoryFiles, txNumFrom, txNumTo uint64) {
	h.InvertedIndex.integrateFiles(InvertedFiles{
		decomp: sf.efHistoryDecomp,
		index:  sf.efHistoryIdx,
	}, txNumFrom, txNumTo)
	h.files.ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.historyDecomp,
		index:        sf.historyIdx,
	})
}

// [txFrom; txTo)
func (h *History) prune(step uint64, txFrom, txTo uint64) error {
	historyKeysCursor, err := h.tx.RwCursorDupSort(h.keysTable)
	if err != nil {
		return fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var k, v []byte
	idxC, err := h.tx.RwCursorDupSort(h.indexTable)
	if err != nil {
		return err
	}
	defer idxC.Close()
	valsC, err := h.tx.RwCursor(h.valsTable)
	if err != nil {
		return err
	}
	defer valsC.Close()
	for k, v, err = historyKeysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		if err = valsC.Delete(v[len(v)-8:]); err != nil {
			return err
		}
		if err = idxC.DeleteExact(v[:len(v)-8], k); err != nil {
			return err
		}
		// This DeleteCurrent needs to the the last in the loop iteration, because it invalidates k and v
		if err = historyKeysCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s history keys: %w", h.filenameBase, err)
	}
	return nil
}

type HistoryContext struct {
	h                        *History
	indexFiles, historyFiles *btree.BTreeG[*ctxItem]
}

func (h *History) MakeContext() *HistoryContext {
	var hc = HistoryContext{h: h}
	hc.indexFiles = btree.NewG[*ctxItem](32, ctxItemLess)
	h.InvertedIndex.files.Ascend(func(item *filesItem) bool {
		hc.indexFiles.ReplaceOrInsert(&ctxItem{
			startTxNum: item.startTxNum,
			endTxNum:   item.endTxNum,
			getter:     item.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(item.index),
		})
		return true
	})
	hc.historyFiles = btree.NewG[*ctxItem](32, ctxItemLess)
	h.files.Ascend(func(item *filesItem) bool {
		hc.historyFiles.ReplaceOrInsert(&ctxItem{
			startTxNum: item.startTxNum,
			endTxNum:   item.endTxNum,
			getter:     item.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(item.index),
		})
		return true
	})
	return &hc
}

func (hc *HistoryContext) GetNoState(key []byte, txNum uint64) ([]byte, bool, uint64, error) {
	//fmt.Printf("GetNoState [%x] %d\n", key, txNum)
	var foundTxNum uint64
	var foundEndTxNum uint64
	var foundStartTxNum uint64
	var found bool
	var anyItem bool
	var maxTxNum uint64
	hc.indexFiles.Ascend(func(item *ctxItem) bool {
		//fmt.Printf("ef item %d-%d, key %x\n", item.startTxNum, item.endTxNum, key)
		if item.reader.Empty() {
			return true
		}
		offset := item.reader.Lookup(key)
		g := item.getter
		g.Reset(offset)
		if k, _ := g.NextUncompressed(); bytes.Equal(k, key) {
			//fmt.Printf("Found key=%x\n", k)
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			if n, ok := ef.Search(txNum); ok {
				foundTxNum = n
				foundEndTxNum = item.endTxNum
				foundStartTxNum = item.startTxNum
				found = true
				//fmt.Printf("Found n=%d\n", n)
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
		var historyItem *ctxItem
		var ok bool
		var search ctxItem
		search.startTxNum = foundStartTxNum
		search.endTxNum = foundEndTxNum
		if historyItem, ok = hc.historyFiles.Get(&search); !ok {
			return nil, false, 0, fmt.Errorf("no %s file found for [%x]", hc.h.filenameBase, key)
		}
		offset := historyItem.reader.Lookup2(txKey[:], key)
		//fmt.Printf("offset = %d, txKey=[%x], key=[%x]\n", offset, txKey[:], key)
		g := historyItem.getter
		g.Reset(offset)
		if hc.h.compressVals {
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
