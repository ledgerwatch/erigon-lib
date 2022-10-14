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
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

type InvertedIndex struct {
	indexKeysTable string // txnNum_u64 -> key (k+auto_increment)
	indexTable     string // k -> txnNum_u64 , Needs to be table with DupSort

	dir             string // Directory where static files are created
	aggregationStep uint64
	filenameBase    string
	tx              kv.RwTx
	txNum           uint64
	txNumBytes      [8]byte
	files           *btree.BTreeG[*filesItem]

	workers int
	w       *invertedIndexWriter
}

func NewInvertedIndex(
	dir string,
	aggregationStep uint64,
	filenameBase string,
	indexKeysTable string,
	indexTable string,
) (*InvertedIndex, error) {
	ii := InvertedIndex{
		dir:             dir,
		files:           btree.NewG[*filesItem](32, filesItemLess),
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
		indexKeysTable:  indexKeysTable,
		indexTable:      indexTable,
		workers:         1,
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("NewInvertedIndex: %s, %w", filenameBase, err)
	}
	ii.scanStateFiles(files)
	if err = ii.openFiles(); err != nil {
		return nil, fmt.Errorf("NewInvertedIndex: %s, %w", filenameBase, err)
	}
	return &ii, nil
}

func (ii *InvertedIndex) scanStateFiles(files []fs.DirEntry) {
	re := regexp.MustCompile("^" + ii.filenameBase + ".([0-9]+)-([0-9]+).ef$")
	var err error
	var uselessFiles []string
	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}

		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 3 {
			if len(subs) != 0 {
				log.Warn("File ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			log.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*ii.aggregationStep, endStep*ii.aggregationStep
		var item = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		{
			var subSet, superSet *filesItem
			ii.files.DescendLessOrEqual(item, func(it *filesItem) bool {
				if it.isSubsetOf(item) {
					subSet = it
				} else if item.isSubsetOf(it) {
					superSet = it
				}
				return true
			})
			if subSet != nil {
				ii.files.Delete(subSet)
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, subSet.startTxNum/ii.aggregationStep, subSet.endTxNum/ii.aggregationStep),
					fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, subSet.startTxNum/ii.aggregationStep, subSet.endTxNum/ii.aggregationStep),
				)
			}
			if superSet != nil {
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, startStep, endStep),
					fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, startStep, endStep),
				)
				continue
			}
		}
		{
			var subSet, superSet *filesItem
			ii.files.AscendGreaterOrEqual(item, func(it *filesItem) bool {
				if it.isSubsetOf(item) {
					subSet = it
				} else if item.isSubsetOf(it) {
					superSet = it
				}
				return false
			})
			if subSet != nil {
				ii.files.Delete(subSet)
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, subSet.startTxNum/ii.aggregationStep, subSet.endTxNum/ii.aggregationStep),
					fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, subSet.startTxNum/ii.aggregationStep, subSet.endTxNum/ii.aggregationStep),
				)
			}
			if superSet != nil {
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, startStep, endStep),
					fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, startStep, endStep),
				)
				continue
			}
		}
		ii.files.ReplaceOrInsert(item)
	}
	if len(uselessFiles) > 0 {
		log.Info("[snapshots] history can delete", "files", strings.Join(uselessFiles, ","))
	}
}

func (ii *InvertedIndex) missedIdxFiles() (l []*filesItem) {
	ii.files.Ascend(func(item *filesItem) bool { // don't run slow logic while iterating on btree
		fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
		if !dir.FileExist(filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, fromStep, toStep))) {
			l = append(l, item)
		}
		return true
	})
	return l
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (ii *InvertedIndex) BuildMissedIndices() (err error) {
	for _, item := range ii.missedIdxFiles() {
		fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
		idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, fromStep, toStep))
		if _, err := buildIndex(item.decompressor, idxPath, ii.dir, item.decompressor.Count()/2, false /* values */); err != nil {
			return err
		}
	}
	return ii.openFiles()
}

func (ii *InvertedIndex) openFiles() error {
	var err error
	var totalKeys uint64
	var invalidFileItems []*filesItem
	ii.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
		datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, fromStep, toStep))
		if !dir.FileExist(datPath) {
			invalidFileItems = append(invalidFileItems, item)
		}
		if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
			log.Debug("InvertedIndex.openFiles: %w, %s", err, datPath)
			return false
		}

		if item.index == nil {
			idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, fromStep, toStep))
			if dir.FileExist(idxPath) {
				if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
					log.Debug("InvertedIndex.openFiles: %w, %s", err, idxPath)
					return false
				}
				totalKeys += item.index.KeyCount()
			}
		}
		return true
	})
	for _, item := range invalidFileItems {
		ii.files.Delete(item)
	}
	if err != nil {
		return err
	}
	return nil
}

func (ii *InvertedIndex) closeFiles() {
	ii.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		if item.index != nil {
			item.index.Close()
		}
		return true
	})
}

func (ii *InvertedIndex) Close() {
	ii.closeFiles()
}

func (ii *InvertedIndex) Files() (res []string) {
	ii.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			_, fName := filepath.Split(item.decompressor.FilePath())
			res = append(res, filepath.Join("history", fName))
		}
		return true
	})
	return res
}

func (ii *InvertedIndex) SetTx(tx kv.RwTx) {
	ii.tx = tx
}

func (ii *InvertedIndex) SetTxNum(txNum uint64) {
	ii.txNum = txNum
	binary.BigEndian.PutUint64(ii.txNumBytes[:], ii.txNum)
}

func (ii *InvertedIndex) add(key, indexKey []byte) error {
	return ii.w.add(key, indexKey)
}

func (ii *InvertedIndex) Add(key []byte) error {
	return ii.add(key, key)
}

func (ii *InvertedIndex) StartWrites(tmpdir string) { ii.w = ii.newWriter(tmpdir) }
func (ii *InvertedIndex) FinishWrites() {
	ii.w.close()
	ii.w = nil
}

func (ii *InvertedIndex) Flush(tx kv.RwTx) error {
	if err := ii.w.flush(tx); err != nil {
		return err
	}
	ii.w = ii.newWriter(ii.w.tmpdir)
	return nil
}

type invertedIndexWriter struct {
	ii        *InvertedIndex
	index     *etl.Collector
	indexKeys *etl.Collector
	tmpdir    string
}

// loadFunc - is analog of etl.Identity, but it signaling to etl - use .Put instead of .AppendDup - to allow duplicates
// maybe in future we will improve etl, to sort dupSort values in the way that allow use .AppendDup
func loadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	return next(k, k, v)
}

func (ii *invertedIndexWriter) flush(tx kv.RwTx) error {
	if err := ii.index.Load(tx, ii.ii.indexTable, loadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	if err := ii.indexKeys.Load(tx, ii.ii.indexKeysTable, loadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	ii.close()
	return nil
}

func (ii *invertedIndexWriter) close() {
	if ii == nil {
		return
	}
	ii.index.Close()
	ii.indexKeys.Close()
}

func (ii *InvertedIndex) newWriter(tmpdir string) *invertedIndexWriter {
	w := &invertedIndexWriter{ii: ii,
		tmpdir:    tmpdir,
		index:     etl.NewCollector(ii.indexTable, tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize/16)),
		indexKeys: etl.NewCollector(ii.indexKeysTable, tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize/16)),
	}
	w.index.LogLvl(log.LvlDebug)
	w.indexKeys.LogLvl(log.LvlDebug)
	return w
}
func (ii *invertedIndexWriter) add(key, indexKey []byte) error {
	/*
		if err := ii.ii.tx.Put(ii.ii.indexKeysTable, ii.ii.txNumBytes[:], key); err != nil {
			return err
		}
		if err := ii.ii.tx.Put(ii.ii.indexTable, indexKey, ii.ii.txNumBytes[:]); err != nil {
			return err
		}
		return nil
	*/
	if err := ii.indexKeys.Collect(ii.ii.txNumBytes[:], key); err != nil {
		return err
	}
	if err := ii.index.Collect(indexKey, ii.ii.txNumBytes[:]); err != nil {
		return err
	}
	return nil
}

func (ii *InvertedIndex) MakeContext() *InvertedIndexContext {
	var ic = InvertedIndexContext{ii: ii}
	ic.files = btree.NewG[ctxItem](32, ctxItemLess)
	ii.files.Ascend(func(item *filesItem) bool {
		if item.index == nil {
			return false
		}

		ic.files.ReplaceOrInsert(ctxItem{
			startTxNum: item.startTxNum,
			endTxNum:   item.endTxNum,
			getter:     item.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(item.index),
		})
		return true
	})
	return &ic
}

// InvertedIterator allows iteration over range of tx numbers
// Iteration is not implmented via callback function, because there is often
// a requirement for interators to be composable (for example, to implement AND and OR for indices)
// InvertedIterator must be closed after use to prevent leaking of resources like cursor
type InvertedIterator struct {
	key                  []byte
	startTxNum, endTxNum uint64
	stack                []ctxItem
	efIt                 *eliasfano32.EliasFanoIter
	next                 uint64
	hasNextInFiles       bool
	hasNextInDb          bool
	roTx                 kv.Tx
	indexTable           string
	cursor               kv.CursorDupSort
}

func (it *InvertedIterator) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
}

func (it *InvertedIterator) advanceInFiles() {
	for {
		for it.efIt == nil {
			if len(it.stack) == 0 {
				it.hasNextInFiles = false
				return
			}
			item := it.stack[len(it.stack)-1]
			it.stack = it.stack[:len(it.stack)-1]
			offset := item.reader.Lookup(it.key)
			g := item.getter
			g.Reset(offset)
			if k, _ := g.NextUncompressed(); bytes.Equal(k, it.key) {
				eliasVal, _ := g.NextUncompressed()
				ef, _ := eliasfano32.ReadEliasFano(eliasVal)
				it.efIt = ef.Iterator()
			}
		}
		for it.efIt.HasNext() {
			n := it.efIt.Next()
			if n >= it.endTxNum {
				it.hasNextInFiles = false
				return
			}
			if n >= it.startTxNum {
				it.hasNextInFiles = true
				it.next = n
				return
			}
		}
		it.efIt = nil // Exhausted this iterator
	}
}

func (it *InvertedIterator) advanceInDb() {
	var v []byte
	var err error
	if it.cursor == nil {
		if it.cursor, err = it.roTx.CursorDupSort(it.indexTable); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		var k []byte
		if k, v, err = it.cursor.Seek(it.key); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		if !bytes.Equal(k, it.key) {
			it.cursor.Close()
			it.hasNextInDb = false
			return
		}
	} else {
		_, v, err = it.cursor.NextDup()
		if err != nil {
			// TODO pass error properly around
			panic(err)
		}
	}
	for ; err == nil && v != nil; _, v, err = it.cursor.NextDup() {
		n := binary.BigEndian.Uint64(v)
		if n >= it.endTxNum {
			it.cursor.Close()
			it.hasNextInDb = false
			return
		}
		if n >= it.startTxNum {
			it.hasNextInDb = true
			it.next = n
			return
		}
	}
	if err != nil {
		// TODO pass error properly around
		panic(err)
	}
	it.cursor.Close()
	it.hasNextInDb = false
}

func (it *InvertedIterator) advance() {
	if it.hasNextInFiles {
		it.advanceInFiles()
	}
	if it.hasNextInDb && !it.hasNextInFiles {
		it.advanceInDb()
	}
}

func (it *InvertedIterator) HasNext() bool {
	return it.hasNextInFiles || it.hasNextInDb
}

func (it *InvertedIterator) Next() uint64 {
	n := it.next
	it.advance()
	return n
}

type InvertedIndexContext struct {
	ii    *InvertedIndex
	files *btree.BTreeG[ctxItem]
}

// IterateRange is to be used in public API, therefore it relies on read-only transaction
// so that iteration can be done even when the inverted index is being updated.
// [startTxNum; endNumTx)
func (ic *InvertedIndexContext) IterateRange(key []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	it := InvertedIterator{
		key:        key,
		startTxNum: startTxNum,
		endTxNum:   endTxNum,
		indexTable: ic.ii.indexTable,
		roTx:       roTx,
	}
	var search ctxItem
	it.hasNextInDb = true
	search.startTxNum = 0
	search.endTxNum = startTxNum
	ic.files.DescendGreaterThan(search, func(item ctxItem) bool {
		if item.startTxNum < endTxNum {
			it.stack = append(it.stack, item)
			it.hasNextInFiles = true
		}
		if item.endTxNum >= endTxNum {
			it.hasNextInDb = false
		}
		return true
	})
	it.advance()
	return it
}

type InvertedIterator1 struct {
	hasNextInFiles                       bool
	hasNextInDb                          bool
	startTxKey                           [8]byte
	startTxNum                           uint64
	endTxNum                             uint64
	roTx                                 kv.Tx
	cursor                               kv.CursorDupSort
	indexTable                           string
	h                                    ReconHeap
	key, nextKey, nextFileKey, nextDbKey []byte
}

func (it *InvertedIterator1) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
}

func (it *InvertedIterator1) advanceInFiles() {
	for it.h.Len() > 0 {
		top := heap.Pop(&it.h).(*ReconItem)
		key := top.key
		val, _ := top.g.NextUncompressed()
		if top.g.HasNext() {
			top.key, _ = top.g.NextUncompressed()
			heap.Push(&it.h, top)
		}
		if !bytes.Equal(key, it.key) {
			ef, _ := eliasfano32.ReadEliasFano(val)
			min := ef.Get(0)
			max := ef.Max()
			if min < it.endTxNum && max >= it.startTxNum { // Intersection of [min; max) and [it.startTxNum; it.endTxNum)
				it.key = key
				it.nextFileKey = key
				return
			}
		}
	}
	it.hasNextInFiles = false
}

func (it *InvertedIterator1) advanceInDb() {
	var k, v []byte
	var err error
	if it.cursor == nil {
		if it.cursor, err = it.roTx.CursorDupSort(it.indexTable); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		if k, _, err = it.cursor.First(); err != nil {
			// TODO pass error properly around
			panic(err)
		}
	} else {
		if k, _, err = it.cursor.NextNoDup(); err != nil {
			panic(err)
		}
	}
	for k != nil {
		if v, err = it.cursor.SeekBothRange(k, it.startTxKey[:]); err != nil {
			panic(err)
		}
		if v != nil {
			txNum := binary.BigEndian.Uint64(v)
			if txNum < it.endTxNum {
				it.nextDbKey = append(it.nextDbKey[:0], k...)
				return
			}
		}
		if k, _, err = it.cursor.NextNoDup(); err != nil {
			panic(err)
		}
	}
	it.cursor.Close()
	it.cursor = nil
	it.hasNextInDb = false
}

func (it *InvertedIterator1) advance() {
	if it.hasNextInFiles {
		if it.hasNextInDb {
			c := bytes.Compare(it.nextFileKey, it.nextDbKey)
			if c < 0 {
				it.nextKey = append(it.nextKey[:0], it.nextFileKey...)
				it.advanceInFiles()
			} else if c > 0 {
				it.nextKey = append(it.nextKey[:0], it.nextDbKey...)
				it.advanceInDb()
			} else {
				it.nextKey = append(it.nextKey[:0], it.nextFileKey...)
				it.advanceInDb()
				it.advanceInFiles()
			}
		} else {
			it.nextKey = append(it.nextKey[:0], it.nextFileKey...)
			it.advanceInFiles()
		}
	} else if it.hasNextInDb {
		it.nextKey = append(it.nextKey[:0], it.nextDbKey...)
		it.advanceInDb()
	} else {
		it.nextKey = nil
	}
}

func (it *InvertedIterator1) HasNext() bool {
	return it.hasNextInFiles || it.hasNextInDb || it.nextKey != nil
}

func (it *InvertedIterator1) Next(keyBuf []byte) []byte {
	result := append(keyBuf, it.nextKey...)
	it.advance()
	return result
}

func (ic *InvertedIndexContext) IterateChangedKeys(startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator1 {
	var ii1 InvertedIterator1
	ii1.hasNextInDb = true
	ii1.roTx = roTx
	ii1.indexTable = ic.ii.indexTable
	ic.files.AscendGreaterOrEqual(ctxItem{endTxNum: startTxNum}, func(item ctxItem) bool {
		if item.endTxNum >= endTxNum {
			ii1.hasNextInDb = false
		}
		if item.endTxNum <= startTxNum {
			return true
		}
		if item.startTxNum >= endTxNum {
			return false
		}
		g := item.getter
		if g.HasNext() {
			key, _ := g.NextUncompressed()
			heap.Push(&ii1.h, &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: g, txNum: ^item.endTxNum, key: key})
			ii1.hasNextInFiles = true
		}
		return true
	})
	binary.BigEndian.PutUint64(ii1.startTxKey[:], startTxNum)
	ii1.startTxNum = startTxNum
	ii1.endTxNum = endTxNum
	ii1.advanceInDb()
	ii1.advanceInFiles()
	ii1.advance()
	return ii1
}

func (ii *InvertedIndex) collate(txFrom, txTo uint64, roTx kv.Tx) (map[string]*roaring64.Bitmap, error) {
	keysCursor, err := roTx.CursorDupSort(ii.indexKeysTable)
	if err != nil {
		return nil, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()
	indexBitmaps := map[string]*roaring64.Bitmap{}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var k, v []byte
	for k, v, err = keysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = keysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		var bitmap *roaring64.Bitmap
		var ok bool
		if bitmap, ok = indexBitmaps[string(v)]; !ok {
			bitmap = roaring64.New()
			indexBitmaps[string(v)] = bitmap
		}
		bitmap.Add(txNum)
	}
	if err != nil {
		return nil, fmt.Errorf("iterate over %s keys cursor: %w", ii.filenameBase, err)
	}
	return indexBitmaps, nil
}

type InvertedFiles struct {
	decomp *compress.Decompressor
	index  *recsplit.Index
}

func (sf InvertedFiles) Close() {
	if sf.decomp != nil {
		sf.decomp.Close()
	}
	if sf.index != nil {
		sf.index.Close()
	}
}

func (ii *InvertedIndex) buildFiles(step uint64, bitmaps map[string]*roaring64.Bitmap) (InvertedFiles, error) {
	var decomp *compress.Decompressor
	var index *recsplit.Index
	var comp *compress.Compressor
	var err error
	closeComp := true
	defer func() {
		if closeComp {
			if comp != nil {
				comp.Close()
			}
			if decomp != nil {
				decomp.Close()
			}
			if index != nil {
				index.Close()
			}
		}
	}()
	txNumFrom := step * ii.aggregationStep
	txNumTo := (step + 1) * ii.aggregationStep
	datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, txNumFrom/ii.aggregationStep, txNumTo/ii.aggregationStep))
	comp, err = compress.NewCompressor(context.Background(), "ef", datPath, ii.dir, compress.MinPatternScore, ii.workers, log.LvlDebug)
	if err != nil {
		return InvertedFiles{}, fmt.Errorf("create %s compressor: %w", ii.filenameBase, err)
	}
	var buf []byte
	keys := make([]string, 0, len(bitmaps))
	for key := range bitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err = comp.AddUncompressedWord([]byte(key)); err != nil {
			return InvertedFiles{}, fmt.Errorf("add %s key [%x]: %w", ii.filenameBase, key, err)
		}
		bitmap := bitmaps[key]
		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			ef.AddOffset(it.Next())
		}
		ef.Build()
		buf = ef.AppendBytes(buf[:0])
		if err = comp.AddUncompressedWord(buf); err != nil {
			return InvertedFiles{}, fmt.Errorf("add %s val: %w", ii.filenameBase, err)
		}
	}
	if err = comp.Compress(); err != nil {
		return InvertedFiles{}, fmt.Errorf("compress %s: %w", ii.filenameBase, err)
	}
	comp.Close()
	comp = nil
	if decomp, err = compress.NewDecompressor(datPath); err != nil {
		return InvertedFiles{}, fmt.Errorf("open %s decompressor: %w", ii.filenameBase, err)
	}
	idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, txNumFrom/ii.aggregationStep, txNumTo/ii.aggregationStep))
	if index, err = buildIndex(decomp, idxPath, ii.dir, len(keys), false /* values */); err != nil {
		return InvertedFiles{}, fmt.Errorf("build %s efi: %w", ii.filenameBase, err)
	}
	closeComp = false
	return InvertedFiles{decomp: decomp, index: index}, nil
}

func (ii *InvertedIndex) integrateFiles(sf InvertedFiles, txNumFrom, txNumTo uint64) {
	ii.files.ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.decomp,
		index:        sf.index,
	})
}

func (ii *InvertedIndex) warmup(txFrom, limit uint64, tx kv.Tx) error {
	keysCursor, err := tx.CursorDupSort(ii.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var k, v []byte
	idxC, err := tx.CursorDupSort(ii.indexTable)
	if err != nil {
		return err
	}
	defer idxC.Close()
	k, v, err = keysCursor.Seek(txKey[:])
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	txFrom = binary.BigEndian.Uint64(k)
	txTo := txFrom + ii.aggregationStep
	if limit != math.MaxUint64 && limit != 0 {
		txTo = txFrom + limit
	}
	for ; err == nil && k != nil; k, v, err = keysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		_, _ = idxC.SeekBothRange(v, k)
	}
	if err != nil {
		return fmt.Errorf("iterate over %s keys: %w", ii.filenameBase, err)
	}
	return nil
}

// [txFrom; txTo)
func (ii *InvertedIndex) prune(txFrom, txTo, limit uint64) error {
	keysCursor, err := ii.tx.RwCursorDupSort(ii.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	idxC, err := ii.tx.RwCursorDupSort(ii.indexTable)
	if err != nil {
		return err
	}
	defer idxC.Close()
	k, v, err := keysCursor.Seek(txKey[:])
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	txFrom = binary.BigEndian.Uint64(k)
	if limit != math.MaxUint64 && limit != 0 {
		txTo = cmp.Min(txTo, txFrom+limit)
	}
	if txFrom-txTo > 10_000 {
		log.Info("[snapshots] prune old history", "name", ii.filenameBase, "range", fmt.Sprintf("%.1fm-%.1fm", float64(txFrom)/1_000_000, float64(txTo)/1_000_000))
	}
	for ; err == nil && k != nil; k, v, err = keysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		if err = idxC.DeleteExact(v, k); err != nil {
			return err
		}
		// This DeleteCurrent needs to the the last in the loop iteration, because it invalidates k and v
		if err = keysCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s keys: %w", ii.filenameBase, err)
	}
	return nil
}

func (ii *InvertedIndex) DisableReadAhead() {
	ii.files.Ascend(func(item *filesItem) bool {
		item.decompressor.DisableReadAhead()
		if item.index != nil {
			item.index.DisableReadAhead()
		}
		return true
	})
}

func (ii *InvertedIndex) EnableReadAhead() *InvertedIndex {
	ii.files.Ascend(func(item *filesItem) bool {
		item.decompressor.EnableReadAhead()
		if item.index != nil {
			item.index.EnableReadAhead()
		}
		return true
	})
	return ii
}
func (ii *InvertedIndex) EnableMadvWillNeed() *InvertedIndex {
	ii.files.Ascend(func(item *filesItem) bool {
		item.decompressor.EnableWillNeed()
		if item.index != nil {
			item.index.EnableWillNeed()
		}
		return true
	})
	return ii
}
func (ii *InvertedIndex) EnableMadvNormalReadAhead() *InvertedIndex {
	ii.files.Ascend(func(item *filesItem) bool {
		item.decompressor.EnableMadvNormal()
		if item.index != nil {
			item.index.EnableMadvNormal()
		}
		return true
	})
	return ii
}
