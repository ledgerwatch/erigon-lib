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
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	btree2 "github.com/tidwall/btree"
	atomic2 "go.uber.org/atomic"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type InvertedIndex struct {
	files *btree2.BTreeG[*filesItem] // thread-safe, but maybe need 1 RWLock for all trees in AggregatorV3

	// roFiles derivative from field `file`, but without garbage (canDelete=true, overlaps, etc...)
	// MakeContext() using this field in zero-copy way
	roFiles atomic2.Pointer[[]ctxItem]

	indexKeysTable  string // txnNum_u64 -> key (k+auto_increment)
	indexTable      string // k -> txnNum_u64 , Needs to be table with DupSort
	dir, tmpdir     string // Directory where static files are created
	filenameBase    string
	aggregationStep uint64
	compressWorkers int

	integrityFileExtensions []string
	withLocalityIndex       bool
	localityIndex           *LocalityIndex
	tx                      kv.RwTx

	// fields for history write
	txNum      uint64
	txNumBytes [8]byte
	wal        *invertedIndexWAL
	walLock    sync.RWMutex
}

func NewInvertedIndex(
	dir, tmpdir string,
	aggregationStep uint64,
	filenameBase string,
	indexKeysTable string,
	indexTable string,
	withLocalityIndex bool,
	integrityFileExtensions []string,
) (*InvertedIndex, error) {
	ii := InvertedIndex{
		dir:                     dir,
		tmpdir:                  tmpdir,
		files:                   btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		roFiles:                 *atomic2.NewPointer(&[]ctxItem{}),
		aggregationStep:         aggregationStep,
		filenameBase:            filenameBase,
		indexKeysTable:          indexKeysTable,
		indexTable:              indexTable,
		compressWorkers:         1,
		integrityFileExtensions: integrityFileExtensions,
		withLocalityIndex:       withLocalityIndex,
	}
	if ii.withLocalityIndex {
		var err error
		ii.localityIndex, err = NewLocalityIndex(ii.dir, ii.tmpdir, ii.aggregationStep, ii.filenameBase)
		if err != nil {
			return nil, fmt.Errorf("NewHistory: %s, %w", ii.filenameBase, err)
		}
	}
	return &ii, nil
}

func (ii *InvertedIndex) fileNamesOnDisk() ([]string, error) {
	files, err := os.ReadDir(ii.dir)
	if err != nil {
		return nil, err
	}
	filteredFiles := make([]string, 0, len(files))
	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}
		filteredFiles = append(filteredFiles, f.Name())
	}
	return filteredFiles, nil
}

func (ii *InvertedIndex) OpenList(fNames []string) error {
	if err := ii.localityIndex.OpenList(fNames); err != nil {
		return err
	}
	ii.closeWhatNotInList(fNames)
	_ = ii.scanStateFiles(fNames)
	if err := ii.openFiles(); err != nil {
		return fmt.Errorf("NewHistory.openFiles: %s, %w", ii.filenameBase, err)
	}
	return nil
}

func (ii *InvertedIndex) OpenFolder() error {
	files, err := ii.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return ii.OpenList(files)
}

func (ii *InvertedIndex) scanStateFiles(fileNames []string) (uselessFiles []*filesItem) {
	re := regexp.MustCompile("^" + ii.filenameBase + ".([0-9]+)-([0-9]+).ef$")
	var err error
Loop:
	for _, name := range fileNames {
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
		frozen := endStep-startStep == StepsInBiggestFile

		for _, ext := range ii.integrityFileExtensions {
			requiredFile := fmt.Sprintf("%s.%d-%d.%s", ii.filenameBase, startStep, endStep, ext)
			if !dir.FileExist(filepath.Join(ii.dir, requiredFile)) {
				log.Debug(fmt.Sprintf("[snapshots] skip %s because %s doesn't exists", name, requiredFile))
				continue Loop
			}
		}

		var newFile = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum, frozen: frozen}
		if _, has := ii.files.Get(newFile); has {
			continue
		}

		addNewFile := true
		var subSets []*filesItem
		ii.files.Walk(func(items []*filesItem) bool {
			for _, item := range items {
				if item.isSubsetOf(newFile) {
					subSets = append(subSets, item)
					continue
				}

				if newFile.isSubsetOf(item) {
					if item.frozen {
						addNewFile = false
						uselessFiles = append(uselessFiles, newFile)
					}
					continue
				}
			}
			return true
		})
		//for _, subSet := range subSets {
		//	ii.files.Delete(subSet)
		//}
		if addNewFile {
			ii.files.Set(newFile)
		}
	}

	return uselessFiles
}

func (ii *InvertedIndex) reCalcRoFiles() {
	roFiles := make([]ctxItem, 0, ii.files.Len())
	var prevStart uint64
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.canDelete.Load() {
				continue
			}
			//if item.startTxNum > h.endTxNumMinimax() {
			//	continue
			//}
			// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
			// see super-set file, just drop sub-set files from list
			if item.startTxNum < prevStart {
				for len(roFiles) > 0 {
					if roFiles[len(roFiles)-1].startTxNum < item.startTxNum {
						break
					}
					roFiles[len(roFiles)-1].src = nil
					roFiles = roFiles[:len(roFiles)-1]
				}
			}

			roFiles = append(roFiles, ctxItem{
				startTxNum: item.startTxNum,
				endTxNum:   item.endTxNum,
				//getter:     item.decompressor.MakeGetter(),
				//reader:     recsplit.NewIndexReader(item.index),

				i:   len(roFiles),
				src: item,
			})
		}
		return true
	})
	if roFiles == nil {
		roFiles = []ctxItem{}
	}
	ii.roFiles.Store(&roFiles)
}

func (ii *InvertedIndex) missedIdxFiles() (l []*filesItem) {
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
			if !dir.FileExist(filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, fromStep, toStep))) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

func (ii *InvertedIndex) buildEfi(ctx context.Context, item *filesItem) (err error) {
	fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
	fName := fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, fromStep, toStep)
	idxPath := filepath.Join(ii.dir, fName)
	log.Info("[snapshots] build idx", "file", fName)
	return buildIndex(ctx, item.decompressor, idxPath, ii.tmpdir, item.decompressor.Count()/2, false)
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (ii *InvertedIndex) BuildMissedIndices(ctx context.Context, g *errgroup.Group) {
	missedFiles := ii.missedIdxFiles()
	for _, item := range missedFiles {
		item := item
		g.Go(func() error { return ii.buildEfi(ctx, item) })
	}
}

func (ii *InvertedIndex) openFiles() error {
	var err error
	var totalKeys uint64
	var invalidFileItems []*filesItem
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				continue
			}
			fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
			datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, fromStep, toStep))
			if !dir.FileExist(datPath) {
				invalidFileItems = append(invalidFileItems, item)
				continue
			}

			if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				log.Debug("InvertedIndex.openFiles: %w, %s", err, datPath)
				continue
			}

			if item.index != nil {
				continue
			}
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

	ii.reCalcRoFiles()
	return nil
}

func (ii *InvertedIndex) closeWhatNotInList(fNames []string) {
	var toDelete []*filesItem
	ii.files.Walk(func(items []*filesItem) bool {
	Loop1:
		for _, item := range items {
			for _, protectName := range fNames {
				if item.decompressor != nil && item.decompressor.FileName() == protectName {
					continue Loop1
				}
			}
			toDelete = append(toDelete, item)
		}
		return true
	})
	for _, item := range toDelete {
		if item.decompressor != nil {
			if err := item.decompressor.Close(); err != nil {
				log.Trace("close", "err", err, "file", item.index.FileName())
			}
			item.decompressor = nil
		}
		if item.index != nil {
			if err := item.index.Close(); err != nil {
				log.Trace("close", "err", err, "file", item.index.FileName())
			}
			item.index = nil
		}
		ii.files.Delete(item)
	}
}

func (ii *InvertedIndex) Close() {
	ii.localityIndex.Close()
	ii.closeWhatNotInList([]string{})
	ii.reCalcRoFiles()
}

func (ii *InvertedIndex) Files() (res []string) {
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				res = append(res, item.decompressor.FileName())
			}
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

func (ii *InvertedIndex) add(key, indexKey []byte) (err error) {
	ii.walLock.RLock()
	err = ii.wal.add(key, indexKey)
	ii.walLock.RUnlock()
	return err
}

func (ii *InvertedIndex) Add(key []byte) error {
	return ii.add(key, key)
}

func (ii *InvertedIndex) DiscardHistory(tmpdir string) {
	ii.walLock.Lock()
	defer ii.walLock.Unlock()
	ii.wal = ii.newWriter(tmpdir, false, true)
}
func (ii *InvertedIndex) StartWrites() {
	ii.walLock.Lock()
	defer ii.walLock.Unlock()
	ii.wal = ii.newWriter(ii.tmpdir, WALCollectorRam > 0, false)
}
func (ii *InvertedIndex) FinishWrites() {
	ii.walLock.Lock()
	defer ii.walLock.Unlock()
	ii.wal.close()
	ii.wal = nil
}

func (ii *InvertedIndex) Rotate() *invertedIndexWAL {
	ii.walLock.Lock()
	defer ii.walLock.Unlock()
	if ii.wal != nil {
		ii.wal.index, ii.wal.indexFlushing = ii.wal.indexFlushing, ii.wal.index
		ii.wal.indexKeys, ii.wal.indexKeysFlushing = ii.wal.indexKeysFlushing, ii.wal.indexKeys
	}
	return ii.wal
}

type invertedIndexWAL struct {
	ii                           *InvertedIndex
	index, indexFlushing         *etl.Collector
	indexKeys, indexKeysFlushing *etl.Collector
	tmpdir                       string
	buffered                     bool
	discard                      bool
}

// loadFunc - is analog of etl.Identity, but it signaling to etl - use .Put instead of .AppendDup - to allow duplicates
// maybe in future we will improve etl, to sort dupSort values in the way that allow use .AppendDup
func loadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	return next(k, k, v)
}

func (ii *invertedIndexWAL) Flush(ctx context.Context, tx kv.RwTx) error {
	if ii.discard {
		return nil
	}
	if err := ii.indexFlushing.Load(tx, ii.ii.indexTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := ii.indexKeysFlushing.Load(tx, ii.ii.indexKeysTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	return nil
}

func (ii *invertedIndexWAL) close() {
	if ii == nil {
		return
	}
	if ii.index != nil {
		ii.index.Close()
	}
	if ii.indexKeys != nil {
		ii.indexKeys.Close()
	}
}

// 3 history + 4 indices = 10 etl collectors, 10*256Mb/8 = 512mb - for all indices buffers
var WALCollectorRam = 2 * (etl.BufferOptimalSize / 8)

func init() {
	v, _ := os.LookupEnv("ERIGON_WAL_COLLETOR_RAM")
	if v != "" {
		var err error
		WALCollectorRam, err = datasize.ParseString(v)
		if err != nil {
			panic(err)
		}
	}
}

func (ii *InvertedIndex) newWriter(tmpdir string, buffered, discard bool) *invertedIndexWAL {
	w := &invertedIndexWAL{ii: ii,
		buffered: buffered,
		discard:  discard,
		tmpdir:   tmpdir,
	}
	if buffered {
		// etl collector doesn't fsync: means if have enough ram, all files produced by all collectors will be in ram
		w.index = etl.NewCollector(ii.indexTable, tmpdir, etl.NewSortableBuffer(WALCollectorRam))
		w.indexFlushing = etl.NewCollector(ii.indexTable, tmpdir, etl.NewSortableBuffer(WALCollectorRam))
		w.indexKeys = etl.NewCollector(ii.indexKeysTable, tmpdir, etl.NewSortableBuffer(WALCollectorRam))
		w.indexKeysFlushing = etl.NewCollector(ii.indexKeysTable, tmpdir, etl.NewSortableBuffer(WALCollectorRam))
		w.index.LogLvl(log.LvlTrace)
		w.indexFlushing.LogLvl(log.LvlTrace)
		w.indexKeys.LogLvl(log.LvlTrace)
		w.indexKeysFlushing.LogLvl(log.LvlTrace)
	}
	return w
}

func (ii *invertedIndexWAL) add(key, indexKey []byte) error {
	if ii.discard {
		return nil
	}

	if ii.buffered {
		if err := ii.indexKeys.Collect(ii.ii.txNumBytes[:], key); err != nil {
			return err
		}

		if err := ii.index.Collect(indexKey, ii.ii.txNumBytes[:]); err != nil {
			return err
		}
	} else {
		if err := ii.ii.tx.Put(ii.ii.indexKeysTable, ii.ii.txNumBytes[:], key); err != nil {
			return err
		}
		if err := ii.ii.tx.Put(ii.ii.indexTable, indexKey, ii.ii.txNumBytes[:]); err != nil {
			return err
		}
	}
	return nil
}

func (ii *InvertedIndex) MakeContext() *InvertedIndexContext {
	var ic = InvertedIndexContext{
		ii:    ii,
		files: *ii.roFiles.Load(),
		loc:   ii.localityIndex.MakeContext(),
	}
	for _, item := range ic.files {
		if !item.src.frozen {
			item.src.refcount.Inc()
		}
	}
	return &ic
}
func (ic *InvertedIndexContext) Close() {
	for _, item := range ic.files {
		if item.src.frozen {
			continue
		}
		refCnt := item.src.refcount.Dec()
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && item.src.canDelete.Load() {
			item.src.closeFilesAndRemove()
		}
	}

	ic.loc.Close()
}

// InvertedIterator allows iteration over range of tx numbers
// Iteration is not implmented via callback function, because there is often
// a requirement for interators to be composable (for example, to implement AND and OR for indices)
// InvertedIterator must be closed after use to prevent leaking of resources like cursor
type InvertedIterator struct {
	key                  []byte
	startTxNum, endTxNum int
	limit                int
	orderAscend          order.By

	roTx       kv.Tx
	cursor     kv.CursorDupSort
	efIt       iter.Unary[uint64]
	indexTable string
	stack      []ctxItem

	nextN                       uint64
	hasNextInDb, hasNextInFiles bool
	nextErrInDB, nextErrInFile  error

	res []uint64
	bm  *roaring64.Bitmap

	ef *eliasfano32.EliasFano
}

func (it *InvertedIterator) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
	bitmapdb.ReturnToPool64(it.bm)
}

func (it *InvertedIterator) advanceInFiles() {
	for {
		for it.efIt == nil { //TODO: this loop may be optimized by LocalityIndex
			if len(it.stack) == 0 {
				it.hasNextInFiles = false
				return
			}
			item := it.stack[len(it.stack)-1]
			it.stack = it.stack[:len(it.stack)-1]
			offset := item.reader.Lookup(it.key)
			g := item.getter
			g.Reset(offset)
			k, _ := g.NextUncompressed()
			if bytes.Equal(k, it.key) {
				eliasVal, _ := g.NextUncompressed()
				it.ef.Reset(eliasVal)
				if it.orderAscend {
					efiter := it.ef.Iterator()
					if it.startTxNum > 0 {
						efiter.Seek(uint64(it.startTxNum))
					}
					it.efIt = efiter
				} else {
					it.efIt = it.ef.ReverseIterator()
				}
			}
		}

		//TODO: add seek method
		//Asc:  [from, to) AND from > to
		//Desc: [from, to) AND from < to
		if it.orderAscend {
			for it.efIt.HasNext() {
				n, _ := it.efIt.Next()
				if it.endTxNum >= 0 && int(n) >= it.endTxNum {
					it.hasNextInFiles = false
					return
				}
				if int(n) >= it.startTxNum {
					it.hasNextInFiles = true
					it.nextN = n
					return
				}
			}
		} else {
			for it.efIt.HasNext() {
				n, _ := it.efIt.Next()
				if int(n) <= it.endTxNum {
					it.hasNextInFiles = false
					return
				}
				if it.startTxNum >= 0 && int(n) <= it.startTxNum {
					it.hasNextInFiles = true
					it.nextN = n
					return
				}
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
		if k, _, err = it.cursor.SeekExact(it.key); err != nil {
			panic(err)
		}
		if k == nil {
			it.hasNextInDb = false
			return
		}
		//Asc:  [from, to) AND from > to
		//Desc: [from, to) AND from < to
		var keyBytes [8]byte
		if it.startTxNum > 0 {
			binary.BigEndian.PutUint64(keyBytes[:], uint64(it.startTxNum))
		}
		if v, err = it.cursor.SeekBothRange(it.key, keyBytes[:]); err != nil {
			panic(err)
		}
		if v == nil {
			if !it.orderAscend {
				_, v, _ = it.cursor.PrevDup()
				if err != nil {
					panic(err)
				}
			}
			if v == nil {
				it.hasNextInDb = false
				return
			}
		}
	} else {
		if it.orderAscend {
			_, v, err = it.cursor.NextDup()
			if err != nil {
				// TODO pass error properly around
				panic(err)
			}
		} else {
			_, v, err = it.cursor.PrevDup()
			if err != nil {
				panic(err)
			}
		}
	}

	//Asc:  [from, to) AND from > to
	//Desc: [from, to) AND from < to
	if it.orderAscend {
		for ; v != nil; _, v, err = it.cursor.NextDup() {
			if err != nil {
				// TODO pass error properly around
				panic(err)
			}
			n := binary.BigEndian.Uint64(v)
			if it.endTxNum >= 0 && int(n) >= it.endTxNum {
				it.hasNextInDb = false
				return
			}
			if int(n) >= it.startTxNum {
				it.hasNextInDb = true
				it.nextN = n
				return
			}
		}
	} else {
		for ; v != nil; _, v, err = it.cursor.PrevDup() {
			if err != nil {
				// TODO pass error properly around
				panic(err)
			}
			n := binary.BigEndian.Uint64(v)
			if int(n) <= it.endTxNum {
				it.hasNextInDb = false
				return
			}
			if it.startTxNum >= 0 && int(n) <= it.startTxNum {
				it.hasNextInDb = true
				it.nextN = n
				return
			}
		}
	}

	it.hasNextInDb = false
}

func (it *InvertedIterator) advance() {
	if it.orderAscend {
		if it.hasNextInFiles {
			it.advanceInFiles()
		}
		if it.hasNextInDb && !it.hasNextInFiles {
			it.advanceInDb()
		}
	} else {
		if it.hasNextInDb {
			it.advanceInDb()
		}
		if it.hasNextInFiles && !it.hasNextInDb {
			it.advanceInFiles()
		}
	}
}

func (it *InvertedIterator) HasNext() bool {
	if it.nextErrInDB != nil || it.nextErrInFile != nil { // always true, then .Next() call will return this error
		return true
	}
	if it.limit == 0 { // limit reached
		return false
	}
	return it.hasNextInFiles || it.hasNextInDb
}

func (it *InvertedIterator) Next() (uint64, error) { return it.next(), nil }
func (it *InvertedIterator) NextBatch() ([]uint64, error) {
	it.res = append(it.res[:0], it.next())
	for it.HasNext() && len(it.res) < 128 {
		it.res = append(it.res, it.next())
	}
	return it.res, nil
}

func (it *InvertedIterator) next() uint64 {
	it.limit--
	n := it.nextN
	it.advance()
	return n
}
func (it *InvertedIterator) ToArray() (res []uint64) {
	for it.HasNext() {
		res = append(res, it.next())
	}
	return res
}
func (it *InvertedIterator) ToBitmap() (*roaring64.Bitmap, error) {
	it.bm = bitmapdb.NewBitmap64()
	bm := it.bm
	for it.HasNext() {
		bm.Add(it.next())
	}
	return bm, nil
}

type InvertedIndexContext struct {
	ii      *InvertedIndex
	files   []ctxItem // have no garbage (overlaps, etc...)
	getters []*compress.Getter
	readers []*recsplit.IndexReader
	loc     *ctxLocalityIdx
}

func (ic *InvertedIndexContext) statelessGetter(i int) *compress.Getter {
	if ic.getters == nil {
		ic.getters = make([]*compress.Getter, len(ic.files))
	}
	r := ic.getters[i]
	if r == nil {
		r = ic.files[i].src.decompressor.MakeGetter()
		ic.getters[i] = r
	}
	return r
}
func (ic *InvertedIndexContext) statelessIdxReader(i int) *recsplit.IndexReader {
	if ic.readers == nil {
		ic.readers = make([]*recsplit.IndexReader, len(ic.files))
	}
	r := ic.readers[i]
	if r == nil {
		r = recsplit.NewIndexReader(ic.files[i].src.index)
		ic.readers[i] = r
	}
	return r
}

func (ic *InvertedIndexContext) getFile(from, to uint64) (it ctxItem, ok bool) {
	for _, item := range ic.files {
		if item.startTxNum == from && item.endTxNum == to {
			return item, true
		}
	}
	return it, false
}

// IterateRange is to be used in public API, therefore it relies on read-only transaction
// so that iteration can be done even when the inverted index is being updated.
// [startTxNum; endNumTx)
func (ic *InvertedIndexContext) IterateRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (*InvertedIterator, error) {
	if asc && (startTxNum >= 0 && endTxNum >= 0) && startTxNum > endTxNum {
		return nil, fmt.Errorf("startTxNum=%d epected to be lower than endTxNum=%d", startTxNum, endTxNum)
	}
	if !asc && (startTxNum >= 0 && endTxNum >= 0) && startTxNum < endTxNum {
		return nil, fmt.Errorf("startTxNum=%d epected to be bigger than endTxNum=%d", startTxNum, endTxNum)
	}

	it := &InvertedIterator{
		key:         key,
		startTxNum:  startTxNum,
		endTxNum:    endTxNum,
		indexTable:  ic.ii.indexTable,
		roTx:        roTx,
		hasNextInDb: true,
		orderAscend: asc,
		limit:       limit,
		ef:          eliasfano32.NewEliasFano(1, 1),
	}
	if asc {
		for i := len(ic.files) - 1; i >= 0; i-- {
			// [from,to) && from < to
			if endTxNum >= 0 && int(ic.files[i].startTxNum) >= endTxNum {
				continue
			}
			if startTxNum >= 0 && ic.files[i].endTxNum <= uint64(startTxNum) {
				break
			}
			it.stack = append(it.stack, ic.files[i])
			it.stack[len(it.stack)-1].getter = it.stack[len(it.stack)-1].src.decompressor.MakeGetter()
			it.stack[len(it.stack)-1].reader = recsplit.NewIndexReader(it.stack[len(it.stack)-1].src.index)
			it.hasNextInFiles = true
		}
		it.hasNextInDb = len(it.stack) == 0 || endTxNum < 0 || it.stack[0].endTxNum < uint64(endTxNum)
	} else {
		for i := 0; i < len(ic.files); i++ {
			// [from,to) && from > to
			if endTxNum >= 0 && int(ic.files[i].endTxNum) <= endTxNum {
				continue
			}
			if startTxNum >= 0 && ic.files[i].startTxNum > uint64(startTxNum) {
				break
			}

			it.stack = append(it.stack, ic.files[i])
			it.stack[len(it.stack)-1].getter = it.stack[len(it.stack)-1].src.decompressor.MakeGetter()
			it.stack[len(it.stack)-1].reader = recsplit.NewIndexReader(it.stack[len(it.stack)-1].src.index)
			it.hasNextInFiles = true
		}
		it.hasNextInDb = len(it.stack) == 0 || startTxNum < 0 || it.stack[len(it.stack)-1].endTxNum < uint64(startTxNum)
	}
	it.advance()
	return it, nil
}

type InvertedIterator1 struct {
	roTx           kv.Tx
	cursor         kv.CursorDupSort
	indexTable     string
	key            []byte
	h              ReconHeap
	nextKey        []byte
	nextFileKey    []byte
	nextDbKey      []byte
	endTxNum       uint64
	startTxNum     uint64
	startTxKey     [8]byte
	hasNextInDb    bool
	hasNextInFiles bool
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
	for _, item := range ic.files {
		if item.endTxNum <= startTxNum {
			continue
		}
		if item.startTxNum >= endTxNum {
			break
		}
		if item.endTxNum >= endTxNum {
			ii1.hasNextInDb = false
		}
		g := item.src.decompressor.MakeGetter()
		if g.HasNext() {
			key, _ := g.NextUncompressed()
			heap.Push(&ii1.h, &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: g, txNum: ^item.endTxNum, key: key})
			ii1.hasNextInFiles = true
		}
	}
	binary.BigEndian.PutUint64(ii1.startTxKey[:], startTxNum)
	ii1.startTxNum = startTxNum
	ii1.endTxNum = endTxNum
	ii1.advanceInDb()
	ii1.advanceInFiles()
	ii1.advance()
	return ii1
}

func (ii *InvertedIndex) collate(ctx context.Context, txFrom, txTo uint64, roTx kv.Tx, logEvery *time.Ticker) (map[string]*roaring64.Bitmap, error) {
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
			bitmap = bitmapdb.NewBitmap64()
			indexBitmaps[string(v)] = bitmap
		}
		bitmap.Add(txNum)

		select {
		case <-logEvery.C:
			log.Info("[snapshots] collate history", "name", ii.filenameBase, "range", fmt.Sprintf("%.2f-%.2f", float64(txNum)/float64(ii.aggregationStep), float64(txTo)/float64(ii.aggregationStep)))
			bitmap.RunOptimize()
		case <-ctx.Done():
			err := ctx.Err()
			return nil, err
		default:
		}
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

func (ii *InvertedIndex) buildFiles(ctx context.Context, step uint64, bitmaps map[string]*roaring64.Bitmap) (InvertedFiles, error) {
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
	comp, err = compress.NewCompressor(ctx, "ef", datPath, ii.tmpdir, compress.MinPatternScore, ii.compressWorkers, log.LvlTrace)
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
	if index, err = buildIndexThenOpen(ctx, decomp, idxPath, ii.tmpdir, len(keys), false /* values */); err != nil {
		return InvertedFiles{}, fmt.Errorf("build %s efi: %w", ii.filenameBase, err)
	}
	closeComp = false
	return InvertedFiles{decomp: decomp, index: index}, nil
}

func (ii *InvertedIndex) integrateFiles(sf InvertedFiles, txNumFrom, txNumTo uint64) {
	ii.files.Set(&filesItem{
		frozen:       (txNumTo-txNumFrom)/ii.aggregationStep == StepsInBiggestFile,
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.decomp,
		index:        sf.index,
	})
	ii.reCalcRoFiles()
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
func (ii *InvertedIndex) prune(ctx context.Context, txFrom, txTo, limit uint64, logEvery *time.Ticker) error {
	keysCursor, err := ii.tx.RwCursorDupSort(ii.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
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
	if txFrom >= txTo {
		return nil
	}

	idxC, err := ii.tx.RwCursorDupSort(ii.indexTable)
	if err != nil {
		return err
	}
	defer idxC.Close()

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	for ; err == nil && k != nil; k, v, err = keysCursor.NextNoDup() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		for ; err == nil && k != nil; k, v, err = keysCursor.NextDup() {

			if err = idxC.DeleteExact(v, k); err != nil {
				return err
			}
			//for vv, err := idxC.SeekBothRange(v, k); vv != nil; _, vv, err = idxC.NextDup() {
			//	if err != nil {
			//		return err
			//	}
			//	if binary.BigEndian.Uint64(vv) >= txTo {
			//		break
			//	}
			//	if err = idxC.DeleteCurrent(); err != nil {
			//		return err
			//	}
			//}
		}

		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if err = keysCursor.DeleteCurrentDuplicates(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-logEvery.C:
			log.Info("[snapshots] prune history", "name", ii.filenameBase, "range", fmt.Sprintf("%.2f-%.2f", float64(txNum)/float64(ii.aggregationStep), float64(txTo)/float64(ii.aggregationStep)))
		default:
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s keys: %w", ii.filenameBase, err)
	}
	return nil
}

func (ii *InvertedIndex) DisableReadAhead() {
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.DisableReadAhead()
			if item.index != nil {
				item.index.DisableReadAhead()
			}
		}
		return true
	})
}

func (ii *InvertedIndex) EnableReadAhead() *InvertedIndex {
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.EnableReadAhead()
			if item.index != nil {
				item.index.EnableReadAhead()
			}
		}
		return true
	})
	return ii
}
func (ii *InvertedIndex) EnableMadvWillNeed() *InvertedIndex {
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.EnableWillNeed()
			if item.index != nil {
				item.index.EnableWillNeed()
			}
		}
		return true
	})
	return ii
}
func (ii *InvertedIndex) EnableMadvNormalReadAhead() *InvertedIndex {
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.EnableMadvNormal()
			if item.index != nil {
				item.index.EnableMadvNormal()
			}
		}
		return true
	})
	return ii
}

func (ii *InvertedIndex) collectFilesStat() (filesCount, filesSize, idxSize uint64) {
	if ii.files == nil {
		return 0, 0, 0
	}
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil {
				return false
			}
			filesSize += uint64(item.decompressor.Size())
			idxSize += uint64(item.index.Size())
			filesCount += 2
		}
		return true
	})
	return filesCount, filesSize, idxSize
}

func (ii *InvertedIndex) CleanupDir() {
	files, _ := ii.fileNamesOnDisk()
	uselessFiles := ii.scanStateFiles(files)
	for _, f := range uselessFiles {
		fName := fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, f.startTxNum/ii.aggregationStep, f.endTxNum/ii.aggregationStep)
		err := os.Remove(filepath.Join(ii.dir, fName))
		log.Debug("[clean] remove", "file", fName, "err", err)
		fIdxName := fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, f.startTxNum/ii.aggregationStep, f.endTxNum/ii.aggregationStep)
		err = os.Remove(filepath.Join(ii.dir, fIdxName))
		log.Debug("[clean] remove", "file", fName, "err", err)
	}
	ii.localityIndex.CleanupDir()
}
