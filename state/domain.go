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
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

var (
	historyValCountKey = []byte("ValCount")
)

// filesItem corresponding to a pair of files (.dat and .idx)
type filesItem struct {
	startTxNum   uint64
	endTxNum     uint64
	decompressor *compress.Decompressor
	getter       *compress.Getter // reader for the decompressor
	getterMerge  *compress.Getter // reader for the decompressor used in the background merge thread
	index        *recsplit.Index
	indexReader  *recsplit.IndexReader // reader for the index
	readerMerge  *recsplit.IndexReader // index reader for the background merge thread
}

func (i *filesItem) Less(than btree.Item) bool {
	if i.endTxNum == than.(*filesItem).endTxNum {
		return i.startTxNum > than.(*filesItem).startTxNum
	}
	return i.endTxNum < than.(*filesItem).endTxNum
}

type FileType int

const (
	Values FileType = iota
	History
	EfHistory
	NumberOfTypes
)

func (ft FileType) String() string {
	switch ft {
	case Values:
		return "values"
	case History:
		return "history"
	case EfHistory:
		return "efhistory"
	default:
		panic(fmt.Sprintf("unknown file type: %d", ft))
	}
}

func ParseFileType(s string) (FileType, bool) {
	switch s {
	case "values":
		return Values, true
	case "history":
		return History, true
	default:
		return NumberOfTypes, false
	}
}

// Domain is a part of the state (examples are Accounts, Storage, Code)
// Domain should not have any go routines or locks
type Domain struct {
	dir              string // Directory where static files are created
	aggregationStep  uint64
	filenameBase     string
	keysTable        string // Needs to be table with DupSort
	valsTable        string
	historyKeysTable string // Needs to be table with DupSort
	historyValsTable string
	settingsTable    string // Table containing just one record - counter of value number (keys in the historyValsTable)
	indexTable       string // Needs to be table with DupSort
	tx               kv.RwTx
	txNum            uint64
	files            [NumberOfTypes]*btree.BTree // Static files pertaining to this domain, items are of type `filesItem`
	prefixLen        int                         // Number of bytes in the keys that can be used for prefix iteration
}

func NewDomain(
	dir string,
	aggregationStep uint64,
	filenameBase string,
	keysTable string,
	valsTable string,
	historyKeysTable string,
	historyValsTable string,
	settingsTable string,
	indexTable string,
	prefixLen int,
) (*Domain, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	d := &Domain{
		dir:              dir,
		aggregationStep:  aggregationStep,
		filenameBase:     filenameBase,
		keysTable:        keysTable,
		valsTable:        valsTable,
		historyKeysTable: historyKeysTable,
		historyValsTable: historyValsTable,
		settingsTable:    settingsTable,
		indexTable:       indexTable,
		prefixLen:        prefixLen,
	}
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		d.files[fType] = btree.New(32)
	}
	d.scanStateFiles(files)
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		d.openFiles(fType)
	}
	return d, nil
}

func (d *Domain) scanStateFiles(files []fs.DirEntry) {
	typeStrings := make([]string, NumberOfTypes)
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		typeStrings[fType] = fType.String()
	}
	re := regexp.MustCompile(d.filenameBase + "(" + strings.Join(typeStrings, "|") + ").([0-9]+)-([0-9]+).(dat|idx)")
	var err error
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 5 {
			if len(subs) != 0 {
				log.Warn("File ignored by doman scan, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startTxNum, endTxNum uint64
		if startTxNum, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by domain scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endTxNum, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			log.Warn("File ignored by domain scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startTxNum > endTxNum {
			log.Warn("File ignored by domain scan, startTxNum > endTxNum", "name", name)
			continue
		}
		fType, ok := ParseFileType(subs[1])
		if !ok {
			log.Warn("File ignored by domain scan, type unknown", "type", subs[1])
		}
		var item = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		var foundI *filesItem
		d.files[fType].AscendGreaterOrEqual(&filesItem{startTxNum: endTxNum, endTxNum: endTxNum}, func(i btree.Item) bool {
			it := i.(*filesItem)
			if it.endTxNum == endTxNum {
				foundI = it
			}
			return false
		})
		if foundI == nil || foundI.startTxNum > startTxNum {
			log.Info("Load state file", "name", name, "type", fType.String(), "startTxNum", startTxNum, "endTxNum", endTxNum)
			d.files[fType].ReplaceOrInsert(item)
		}
	}
}

func (d *Domain) openFiles(fType FileType) error {
	var err error
	var totalKeys uint64
	d.files[fType].Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		datPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.dat", d.filenameBase, fType.String(), item.startTxNum, item.endTxNum))
		if item.decompressor, err = compress.NewDecompressor(path.Join(d.dir, datPath)); err != nil {
			return false
		}
		idxPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.idx", d.filenameBase, fType.String(), item.startTxNum, item.endTxNum))
		if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
			return false
		}
		totalKeys += item.index.KeyCount()
		item.getter = item.decompressor.MakeGetter()
		item.getterMerge = item.decompressor.MakeGetter()
		item.indexReader = recsplit.NewIndexReader(item.index)
		item.readerMerge = recsplit.NewIndexReader(item.index)
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *Domain) closeFiles(fType FileType) {
	d.files[fType].Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		if item.index != nil {
			item.index.Close()
		}
		return true
	})
}

func (d *Domain) Close() {
	// Closing state files only after background aggregation goroutine is finished
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		d.closeFiles(fType)
	}
}

func (d *Domain) SetTx(tx kv.RwTx) {
	d.tx = tx
}

func (d *Domain) SetTxNum(txNum uint64) {
	d.txNum = txNum
}

func (d *Domain) get(key []byte) ([]byte, bool, error) {
	var invertedStep [8]byte
	binary.BigEndian.PutUint64(invertedStep[:], ^(d.txNum / d.aggregationStep))
	keyCursor, err := d.tx.CursorDupSort(d.keysTable)
	if err != nil {
		return nil, false, err
	}
	defer keyCursor.Close()
	foundInvStep, err := keyCursor.SeekBothRange(key, invertedStep[:])
	if err != nil {
		return nil, false, err
	}
	if foundInvStep == nil {
		v, found := d.readFromFiles(Values, key)
		return v, found, nil
	}
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	copy(keySuffix[len(key):], foundInvStep)
	v, err := d.tx.GetOne(d.valsTable, keySuffix)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (d *Domain) Get(key []byte) ([]byte, error) {
	v, _, err := d.get(key)
	return v, err
}

func (d *Domain) update(key, original []byte) error {
	var invertedStep [8]byte
	binary.BigEndian.PutUint64(invertedStep[:], ^(d.txNum / d.aggregationStep))
	if err := d.tx.Put(d.keysTable, key, invertedStep[:]); err != nil {
		return err
	}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], d.txNum)
	historyKey := make([]byte, len(key)+8)
	copy(historyKey, key)
	if len(original) > 0 {
		val, err := d.tx.GetOne(d.settingsTable, historyValCountKey)
		if err != nil {
			return err
		}
		var valNum uint64
		if len(val) > 0 {
			valNum = binary.BigEndian.Uint64(val)
		}
		valNum++
		binary.BigEndian.PutUint64(historyKey[len(key):], valNum)
		if err = d.tx.Put(d.settingsTable, historyValCountKey, historyKey[len(key):]); err != nil {
			return err
		}
		if err = d.tx.Put(d.historyValsTable, historyKey[len(key):], original); err != nil {
			return err
		}
	}
	if err := d.tx.Put(d.historyKeysTable, txKey[:], historyKey); err != nil {
		return err
	}
	if err := d.tx.Put(d.indexTable, key, txKey[:]); err != nil {
		return err
	}
	return nil
}

func (d *Domain) Put(key, val []byte) error {
	original, _, err := d.get(key)
	if err != nil {
		return err
	}
	// This call to update needs to happen before d.tx.Put() later, because otherwise the content of `original`` slice is invalidated
	if err = d.update(key, original); err != nil {
		return err
	}
	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	if err = d.tx.Put(d.valsTable, keySuffix, val); err != nil {
		return err
	}
	return nil
}

func (d *Domain) Delete(key []byte) error {
	original, _, err := d.get(key)
	if err != nil {
		return err
	}
	// This call to update needs to happen before d.tx.Delete() later, because otherwise the content of `original`` slice is invalidated
	if err = d.update(key, original); err != nil {
		return err
	}
	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	if err = d.tx.Delete(d.valsTable, keySuffix, nil); err != nil {
		return err
	}
	return nil
}

type CursorType uint8

const (
	FILE_CURSOR CursorType = iota
	DB_CURSOR
)

// CursorItem is the item in the priority queue used to do merge interation
// over storage of a given account
type CursorItem struct {
	t        CursorType // Whether this item represents state file or DB record, or tree
	endTxNum uint64
	key, val []byte
	dg       *compress.Getter
	c        kv.CursorDupSort
}

type CursorHeap []*CursorItem

func (ch CursorHeap) Len() int {
	return len(ch)
}

func (ch CursorHeap) Less(i, j int) bool {
	cmp := bytes.Compare(ch[i].key, ch[j].key)
	if cmp == 0 {
		// when keys match, the items with later blocks are preferred
		return ch[i].endTxNum > ch[j].endTxNum
	}
	return cmp < 0
}

func (ch *CursorHeap) Swap(i, j int) {
	(*ch)[i], (*ch)[j] = (*ch)[j], (*ch)[i]
}

func (ch *CursorHeap) Push(x interface{}) {
	*ch = append(*ch, x.(*CursorItem))
}

func (ch *CursorHeap) Pop() interface{} {
	old := *ch
	n := len(old)
	x := old[n-1]
	*ch = old[0 : n-1]
	return x
}

func (d *Domain) IteratePrefix(prefix []byte, it func(k, v []byte)) error {
	if len(prefix) != d.prefixLen {
		return fmt.Errorf("wrong prefix length, this %s domain supports prefixLen %d, given [%x]", d.filenameBase, d.prefixLen, prefix)
	}
	var cp CursorHeap
	heap.Init(&cp)
	keysCursor, err := d.tx.CursorDupSort(d.keysTable)
	if err != nil {
		return err
	}
	defer keysCursor.Close()
	var k, v []byte
	if k, v, err = keysCursor.Seek(prefix); err != nil {
		return err
	}
	if bytes.HasPrefix(k, prefix) {
		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		step := ^binary.BigEndian.Uint64(v)
		txNum := step * d.aggregationStep
		if v, err = d.tx.GetOne(d.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(&cp, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(v), c: keysCursor, endTxNum: txNum})
	}
	d.files[Values].Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		if item.index.Empty() {
			return true
		}
		offset := item.indexReader.Lookup(prefix)
		g := item.getter
		g.Reset(offset)
		if g.HasNext() {
			if keyMatch, _ := g.Match(prefix); !keyMatch {
				return true
			}
			g.Skip()
		}
		if g.HasNext() {
			key, _ := g.Next(nil)
			if bytes.HasPrefix(key, prefix) {
				val, _ := g.Next(nil)
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, key: key, val: val, dg: g, endTxNum: item.endTxNum})
			}
		}
		return true
	})
	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := cp[0]
			switch ci1.t {
			case FILE_CURSOR:
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.Next(ci1.key[:0])
					if bytes.HasPrefix(ci1.key, prefix) {
						ci1.val, _ = ci1.dg.Next(ci1.val[:0])
						heap.Fix(&cp, 0)
					} else {
						heap.Pop(&cp)
					}
				} else {
					heap.Pop(&cp)
				}
			case DB_CURSOR:
				k, v, err = ci1.c.NextNoDup()
				if err != nil {
					return err
				}
				if k != nil && bytes.HasPrefix(k, prefix) {
					ci1.key = common.Copy(k)
					keySuffix := make([]byte, len(k)+8)
					copy(keySuffix, k)
					copy(keySuffix[len(k):], v)
					if v, err = d.tx.GetOne(d.valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = common.Copy(v)
					heap.Fix(&cp, 0)
				} else {
					heap.Pop(&cp)
				}
			}
		}
		if len(lastVal) > 0 {
			it(lastKey, lastVal)
		}
	}
	return nil
}

// Collation is the set of compressors created after aggregation
type Collation struct {
	valuesPath   string
	valuesComp   *compress.Compressor
	valuesCount  int
	historyPath  string
	historyComp  *compress.Compressor
	historyCount int
	indexBitmaps map[string]*roaring64.Bitmap
}

// collate gathers domain changes over the specified step, using read-only transaction,
// and returns compressors, elias fano, and bitmaps
// [txFrom; txTo)
func (d *Domain) collate(step uint64, txFrom, txTo uint64, roTx kv.Tx) (Collation, error) {
	var valuesComp, historyComp *compress.Compressor
	var err error
	closeComp := true
	defer func() {
		if closeComp {
			if valuesComp != nil {
				valuesComp.Close()
			}
			if historyComp != nil {
				historyComp.Close()
			}
		}
	}()
	blockFrom := step * d.aggregationStep
	blockTo := (step + 1) * d.aggregationStep
	valuesPath := filepath.Join(d.dir, fmt.Sprintf("%s-values.%d-%d.dat", d.filenameBase, blockFrom, blockTo))
	if valuesComp, err = compress.NewCompressor(context.Background(), "collate values", valuesPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}
	keysCursor, err := roTx.CursorDupSort(d.keysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()
	var prefix []byte // Track prefix to insert it before entries
	var k, v []byte
	valuesCount := 0
	for k, _, err = keysCursor.First(); err == nil && k != nil; k, _, err = keysCursor.NextNoDup() {
		if v, err = keysCursor.LastDup(); err != nil {
			return Collation{}, fmt.Errorf("find last %s key for aggregation step k=[%x]: %w", d.filenameBase, k, err)
		}
		s := ^binary.BigEndian.Uint64(v)
		if s == step {
			keySuffix := make([]byte, len(k)+8)
			copy(keySuffix, k)
			copy(keySuffix[len(k):], v)
			v, err := roTx.GetOne(d.valsTable, keySuffix)
			if err != nil {
				return Collation{}, fmt.Errorf("find last %s value for aggregation step k=[%x]: %w", d.filenameBase, k, err)
			}
			if d.prefixLen > 0 && (prefix == nil || !bytes.HasPrefix(k, prefix)) {
				prefix = append(prefix[:0], k[:d.prefixLen]...)
				if err = valuesComp.AddUncompressedWord(prefix); err != nil {
					return Collation{}, fmt.Errorf("add %s values prefix [%x]: %w", d.filenameBase, prefix, err)
				}
				if err = valuesComp.AddUncompressedWord(nil); err != nil {
					return Collation{}, fmt.Errorf("add %s values prefix val [%x]: %w", d.filenameBase, prefix, err)
				}
				valuesCount++
			}
			if err = valuesComp.AddUncompressedWord(k); err != nil {
				return Collation{}, fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, k, err)
			}
			valuesCount++ // Only counting keys, not values
			if err = valuesComp.AddUncompressedWord(v); err != nil {
				return Collation{}, fmt.Errorf("add %s values val [%x]=>[%x]: %w", d.filenameBase, k, v, err)
			}
		}
	}
	if err != nil {
		return Collation{}, fmt.Errorf("iterate over %s keys cursor: %w", d.filenameBase, err)
	}
	historyPath := filepath.Join(d.dir, fmt.Sprintf("%s-history.%d-%d.dat", d.filenameBase, blockFrom, blockTo))
	if historyComp, err = compress.NewCompressor(context.Background(), "collate history", historyPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
		return Collation{}, fmt.Errorf("create %s history compressor: %w", d.filenameBase, err)
	}
	historyKeysCursor, err := roTx.CursorDupSort(d.historyKeysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s history cursor: %w", d.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	indexBitmaps := map[string]*roaring64.Bitmap{}
	historyCount := 0
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var val []byte
	var historyKey []byte
	for k, v, err = historyKeysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		historyKey = append(append(historyKey[:0], k...), v[:len(v)-8]...)
		if err = historyComp.AddUncompressedWord(historyKey); err != nil {
			return Collation{}, fmt.Errorf("add %s history key [%x]: %w", d.filenameBase, k, err)
		}
		valNum := binary.BigEndian.Uint64(v[len(v)-8:])
		if valNum == 0 {
			val = nil
		} else {
			if val, err = roTx.GetOne(d.historyValsTable, v[len(v)-8:]); err != nil {
				return Collation{}, fmt.Errorf("get %s history val [%x]=>%d: %w", d.filenameBase, k, valNum, err)
			}
		}
		if err = historyComp.AddUncompressedWord(val); err != nil {
			return Collation{}, fmt.Errorf("add %s history val [%x]=>[%x]: %w", d.filenameBase, k, val, err)
		}
		historyCount++
		var bitmap *roaring64.Bitmap
		var ok bool
		if bitmap, ok = indexBitmaps[string(v[:len(v)-8])]; !ok {
			bitmap = roaring64.New()
			indexBitmaps[string(v[:len(v)-8])] = bitmap
		}
		bitmap.Add(txNum)
	}
	if err != nil {
		return Collation{}, fmt.Errorf("iterate over %s history cursor: %w", d.filenameBase, err)
	}
	closeComp = false
	return Collation{
		valuesPath:   valuesPath,
		valuesComp:   valuesComp,
		valuesCount:  valuesCount,
		historyPath:  historyPath,
		historyComp:  historyComp,
		historyCount: historyCount,
		indexBitmaps: indexBitmaps,
	}, nil
}

type StaticFiles struct {
	valuesDecomp    *compress.Decompressor
	valuesIdx       *recsplit.Index
	historyDecomp   *compress.Decompressor
	historyIdx      *recsplit.Index
	efHistoryDecomp *compress.Decompressor
	efHistoryIdx    *recsplit.Index
}

func (sf StaticFiles) Close() {
	sf.valuesDecomp.Close()
	sf.valuesIdx.Close()
	sf.historyDecomp.Close()
	sf.historyIdx.Close()
	sf.efHistoryDecomp.Close()
	sf.efHistoryIdx.Close()
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (d *Domain) buildFiles(step uint64, collation Collation) (StaticFiles, error) {
	valuesComp := collation.valuesComp
	historyComp := collation.historyComp
	var valuesDecomp, historyDecomp, efHistoryDecomp *compress.Decompressor
	var valuesIdx, historyIdx, efHistoryIdx *recsplit.Index
	var efHistoryComp *compress.Compressor
	closeComp := true
	defer func() {
		if closeComp {
			if valuesComp != nil {
				valuesComp.Close()
			}
			if valuesDecomp != nil {
				valuesDecomp.Close()
			}
			if valuesIdx != nil {
				valuesIdx.Close()
			}
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
		}
	}()
	txNumFrom := step * d.aggregationStep
	txNumTo := (step + 1) * d.aggregationStep
	valuesIdxPath := filepath.Join(d.dir, fmt.Sprintf("%s-values.%d-%d.idx", d.filenameBase, txNumFrom, txNumTo))
	var err error
	if err = valuesComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s values: %w", d.filenameBase, err)
	}
	valuesComp.Close()
	valuesComp = nil
	if valuesDecomp, err = compress.NewDecompressor(collation.valuesPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s values decompressor: %w", d.filenameBase, err)
	}
	if valuesIdx, err = buildIndex(valuesDecomp, valuesIdxPath, d.dir, collation.valuesCount, false /* values */); err != nil {
		return StaticFiles{}, fmt.Errorf("build %s values idx: %w", d.filenameBase, err)
	}
	historyIdxPath := filepath.Join(d.dir, fmt.Sprintf("%s-history.%d-%d.idx", d.filenameBase, txNumFrom, txNumTo))
	if err = historyComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s history: %w", d.filenameBase, err)
	}
	historyComp.Close()
	historyComp = nil
	if historyDecomp, err = compress.NewDecompressor(collation.historyPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s history decompressor: %w", d.filenameBase, err)
	}
	if historyIdx, err = buildIndex(historyDecomp, historyIdxPath, d.dir, collation.historyCount, true /* values */); err != nil {
		return StaticFiles{}, fmt.Errorf("build %s history idx: %w", d.filenameBase, err)
	}
	// Build history ef
	efHistoryPath := filepath.Join(d.dir, fmt.Sprintf("%s-efhistory.%d-%d.dat", d.filenameBase, txNumFrom, txNumTo))
	efHistoryComp, err = compress.NewCompressor(context.Background(), "ef history", efHistoryPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug)
	if err != nil {
		return StaticFiles{}, fmt.Errorf("create %s ef history compressor: %w", d.filenameBase, err)
	}
	var buf []byte
	keys := make([]string, 0, len(collation.indexBitmaps))
	for key := range collation.indexBitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err = efHistoryComp.AddUncompressedWord([]byte(key)); err != nil {
			return StaticFiles{}, fmt.Errorf("add %s ef history key [%x]: %w", d.filenameBase, key, err)
		}
		bitmap := collation.indexBitmaps[key]
		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			ef.AddOffset(it.Next())
		}
		ef.Build()
		buf = ef.AppendBytes(buf[:0])
		if err = efHistoryComp.AddUncompressedWord(buf); err != nil {
			return StaticFiles{}, fmt.Errorf("add %s ef history val: %w", d.filenameBase, err)
		}
	}
	if err = efHistoryComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s ef history: %w", d.filenameBase, err)
	}
	efHistoryComp.Close()
	efHistoryComp = nil
	if efHistoryDecomp, err = compress.NewDecompressor(efHistoryPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s ef history decompressor: %w", d.filenameBase, err)
	}
	efHistoryIdxPath := filepath.Join(d.dir, fmt.Sprintf("%s-efhistory.%d-%d.idx", d.filenameBase, txNumFrom, txNumTo))
	if efHistoryIdx, err = buildIndex(efHistoryDecomp, efHistoryIdxPath, d.dir, len(keys), false /* values */); err != nil {
		return StaticFiles{}, fmt.Errorf("build %s ef history idx: %w", d.filenameBase, err)
	}
	closeComp = false
	return StaticFiles{
		valuesDecomp:    valuesDecomp,
		valuesIdx:       valuesIdx,
		historyDecomp:   historyDecomp,
		historyIdx:      historyIdx,
		efHistoryDecomp: efHistoryDecomp,
		efHistoryIdx:    efHistoryIdx,
	}, nil
}

func buildIndex(d *compress.Decompressor, idxPath, dir string, count int, values bool) (*recsplit.Index, error) {
	var rs *recsplit.RecSplit
	var err error
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     dir,
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		IndexFile: idxPath,
	}); err != nil {
		return nil, fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	word := make([]byte, 0, 256)
	var keyPos, valPos uint64
	g := d.MakeGetter()
	for {
		g.Reset(0)
		for g.HasNext() {
			word, valPos = g.Next(word[:0])
			if values {
				if err = rs.AddKey(word, valPos); err != nil {
					return nil, fmt.Errorf("add idx key [%x]: %w", word, err)
				}
			} else {
				if err = rs.AddKey(word, keyPos); err != nil {
					return nil, fmt.Errorf("add idx key [%x]: %w", word, err)
				}
			}
			// Skip value
			keyPos = g.Skip()
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return nil, fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	var idx *recsplit.Index
	if idx, err = recsplit.OpenIndex(idxPath); err != nil {
		return nil, fmt.Errorf("open idx: %w", err)
	}
	return idx, nil
}

func (d *Domain) integrateFiles(sf StaticFiles, txNumFrom, txNumTo uint64) {
	d.files[Values].ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.valuesDecomp,
		index:        sf.valuesIdx,
		getter:       sf.valuesDecomp.MakeGetter(),
		getterMerge:  sf.valuesDecomp.MakeGetter(),
		indexReader:  recsplit.NewIndexReader(sf.valuesIdx),
		readerMerge:  recsplit.NewIndexReader(sf.valuesIdx),
	})
	d.files[History].ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.historyDecomp,
		index:        sf.historyIdx,
		getter:       sf.historyDecomp.MakeGetter(),
		getterMerge:  sf.historyDecomp.MakeGetter(),
		indexReader:  recsplit.NewIndexReader(sf.historyIdx),
		readerMerge:  recsplit.NewIndexReader(sf.historyIdx),
	})
	d.files[EfHistory].ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.efHistoryDecomp,
		index:        sf.efHistoryIdx,
		getter:       sf.efHistoryDecomp.MakeGetter(),
		getterMerge:  sf.efHistoryDecomp.MakeGetter(),
		indexReader:  recsplit.NewIndexReader(sf.efHistoryIdx),
		readerMerge:  recsplit.NewIndexReader(sf.efHistoryIdx),
	})
}

// [txFrom; txTo)
func (d *Domain) prune(step uint64, txFrom, txTo uint64) error {
	// It is important to clean up tables in a specific order
	// First keysTable, because it is the first one access in the `get` function, i.e. if the record is deleted from there, other tables will not be accessed
	keysCursor, err := d.tx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("%s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()
	var k, v []byte
	for k, v, err = keysCursor.First(); err == nil && k != nil; k, v, err = keysCursor.Next() {
		s := ^binary.BigEndian.Uint64(v)
		if s == step {
			if err = keysCursor.DeleteCurrent(); err != nil {
				return fmt.Errorf("clean up %s for [%x]=>[%x]: %w", d.filenameBase, k, v, err)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("iterate of %s keys: %w", d.filenameBase, err)
	}
	var valsCursor kv.RwCursor
	if valsCursor, err = d.tx.RwCursor(d.valsTable); err != nil {
		return fmt.Errorf("%s vals cursor: %w", d.filenameBase, err)
	}
	defer valsCursor.Close()
	for k, _, err = valsCursor.First(); err == nil && k != nil; k, _, err = valsCursor.Next() {
		s := ^binary.BigEndian.Uint64(k[len(k)-8:])
		if s == step {
			if err = valsCursor.DeleteCurrent(); err != nil {
				return fmt.Errorf("clean up %s for [%x]: %w", d.filenameBase, k, err)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s vals: %w", d.filenameBase, err)
	}
	historyKeysCursor, err := d.tx.RwCursorDupSort(d.historyKeysTable)
	if err != nil {
		return fmt.Errorf("create %s history cursor: %w", d.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	for k, v, err = historyKeysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		if err = d.tx.Delete(d.historyValsTable, v[len(v)-8:], nil); err != nil {
			return err
		}
		if err = d.tx.Delete(d.indexTable, v[:len(v)-8], k); err != nil {
			return err
		}
		// This DeleteCurrent needs to the the last in the loop iteration, because it invalidates k and v
		if err = historyKeysCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s history keys: %w", d.filenameBase, err)
	}
	return nil
}

// staticFilesInRange returns list of static files with txNum in specified range [startTxNum; endTxNum)
// files are in the descending order of endTxNum
func (d *Domain) staticFilesInRange(startTxNum, endTxNum uint64) ([][NumberOfTypes]*filesItem, int) {
	var files [][NumberOfTypes]*filesItem
	var greaterThan, lessOrEqual filesItem
	greaterThan.endTxNum = startTxNum
	lessOrEqual.endTxNum = endTxNum
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		j := 0
		d.files[fType].DescendRange(&lessOrEqual, &greaterThan, func(i btree.Item) bool {
			item := i.(*filesItem)
			for j >= len(files) {
				files = append(files, [NumberOfTypes]*filesItem{})
			}
			files[j][fType] = item
			j++
			return true
		})
	}
	// Reverse the order because it has been produced by Descend
	l := len(files)
	for i := 0; i < l/2; i++ {
		files[i], files[l-i-1] = files[l-i-1], files[i]
	}
	return files
}

// findMergeRange assumes that all fTypes in d.files have items at least as far as maxEndTxNum
// That is why only Values type is inspected
func (d *Domain) findMergeRange(maxEndTxNum uint64, maxSpan uint64) (bool, uint64, uint64) {
	var minFound bool
	var startTxNum, endTxNum uint64
	d.files[Values].Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		endStep := item.endTxNum / d.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * d.aggregationStep
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

func (d *Domain) endTxNumMinimax() uint64 {
	var minimax uint64
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		if d.files[fType].Len() > 0 {
			endTxNum := d.files[fType].Max().(*filesItem).endTxNum
			if minimax == 0 || endTxNum < minimax {
				minimax = endTxNum
			}
		}
	}
	return minimax
}

func (d *Domain) mergeFiles(files [][NumberOfTypes]*filesItem, startTxNum, endTxNum uint64, maxSpan uint64) (*filesItem, error) {
	outItem := &filesItem{}
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
			if outItem.decompressor != nil {
				outItem.decompressor.Close()
			}
			if outItem.index != nil {
				outItem.index.Close()
			}
		}
	}()
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		valCompressed := fType != EfHistory
		removeVals := fType == History && (endTxNum-startTxNum) == maxSpan
		tmpPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.tmp", d.filenameBase, fType.String(), startTxNum, endTxNum))
		datPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.dat", d.filenameBase, fType.String(), startTxNum, endTxNum))
		if removeVals {
			if comp, err = compress.NewCompressor(context.Background(), "merge", tmpPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
				return nil, fmt.Errorf("merge %s history compressor: %w", d.filenameBase, err)
			}
		} else {
			if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
				return nil, fmt.Errorf("merge %s history compressor: %w", d.filenameBase, err)
			}
		}
		var cp CursorHeap
		heap.Init(&cp)
		for _, filesByType := range files {
			item := filesByType[fType]
			g := item.decompressor.MakeGetter()
			g.Reset(0)
			if g.HasNext() {
				key, _ := g.Next(nil)
				val, _ := g.Next(nil)
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, dg: g, key: key, val: val, endTxNum: item.endTxNum})
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
			// Advance all the items that have this key (including the top)
			for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
				ci1 := cp[0]
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.Next(ci1.key[:0])
					if valCompressed {
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
					if err = comp.AddWord(keyBuf); err != nil {
						return nil, err
					}
					count++ // Only counting keys, not values
					if valCompressed {
						if err = comp.AddWord(valBuf); err != nil {
							return nil, err
						}
					} else {
						if err = comp.AddUncompressedWord(valBuf); err != nil {
							return nil, err
						}
					}
				}
				keyBuf = append(keyBuf[:0], lastKey...)
				valBuf = append(valBuf[:0], lastVal...)
			}
		}
		if keyBuf != nil {
			if err = comp.AddWord(keyBuf); err != nil {
				return nil, err
			}
			count++ // Only counting keys, not values
			if valCompressed {
				if err = comp.AddWord(valBuf); err != nil {
					return nil, err
				}
			} else {
				if err = comp.AddUncompressedWord(valBuf); err != nil {
					return nil, err
				}
			}
		}
		if err = comp.Compress(); err != nil {
			return nil, err
		}
		comp.Close()
		comp = nil
		idxPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.idx", d.filenameBase, fType.String(), startTxNum, endTxNum))
		if removeVals {
			if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
				return nil, fmt.Errorf("merge %s remove vals compressor: %w", d.filenameBase, err)
			}
			if decomp, err = compress.NewDecompressor(tmpPath); err != nil {
				return nil, fmt.Errorf("merge %s remove vals decompressor %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			g := decomp.MakeGetter()
			var val []byte
			var count int
			g.Reset(0)
			for g.HasNext() {
				g.Skip() // Skip key on on the first pass
				val, _ = g.Next(val[:0])
				if err = comp.AddWord(val); err != nil {
					return nil, fmt.Errorf("merge %s remove vals add val %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
				}
				count++
			}
			if err = comp.Compress(); err != nil {
				return nil, err
			}
			comp = nil
			if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				return nil, fmt.Errorf("merge %s remove vals decompressor(no val) %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
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
				return nil, fmt.Errorf("merge %s remove vals recsplit %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			g1 := outItem.decompressor.MakeGetter()
			var key []byte
			for {
				g.Reset(0)
				g1.Reset(0)
				var lastOffset uint64
				for g.HasNext() {
					key, _ = g.Next(key[:0])
					g.Skip() // Skip value
					_, pos := g1.Next(nil)
					if err = rs.AddKey(key, lastOffset); err != nil {
						return nil, fmt.Errorf("merge %s remove vals recsplit add key %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
					}
					lastOffset = pos
				}
				if err = rs.Build(); err != nil {
					if rs.Collision() {
						log.Info("Building reduceHistoryFiles. Collision happened. It's ok. Restarting...")
						rs.ResetNextSalt()
					} else {
						return nil, fmt.Errorf("merge %s remove vals recsplit build %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
					}
				} else {
					break
				}
			}
			decomp.Close()
			decomp = nil
		} else {
			if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				return nil, fmt.Errorf("merge %s decompressor %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			if outItem.index, err = buildIndex(outItem.decompressor, idxPath, d.dir, count, fType == History /* values */); err != nil {
				return nil, fmt.Errorf("merge %s buildIndex %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
		}
		outItem.getter = outItem.decompressor.MakeGetter()
		outItem.getterMerge = outItem.decompressor.MakeGetter()
		if outItem.index, err = recsplit.OpenIndex(idxPath); err != nil {
			return nil, fmt.Errorf("merge %s open index %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
		}
		outItem.indexReader = recsplit.NewIndexReader(outItem.index)
		outItem.readerMerge = recsplit.NewIndexReader(outItem.index)
	}
	closeItem = false
	return outItem, nil
}

func (d *Domain) readFromFiles(fType FileType, filekey []byte) ([]byte, bool) {
	var val []byte
	var found bool
	d.files[fType].Descend(func(i btree.Item) bool {
		item := i.(*filesItem)
		if item.index.Empty() {
			return true
		}
		offset := item.indexReader.Lookup(filekey)
		g := item.getter
		g.Reset(offset)
		if g.HasNext() {
			if keyMatch, _ := g.Match(filekey); keyMatch {
				val, _ = g.Next(nil)
				found = true
				return false
			}
		}
		return true
	})
	return val, found
}

// historyAfterTxNum searches history for a value of specified key after txNum
// second return value is true if the value is found in the history (even if it is nil)
func (d *Domain) historyAfterTxNum(key []byte, txNum uint64) ([]byte, bool, error) {
	var search filesItem
	search.endTxNum = txNum + 1
	var foundTxNum uint64
	var foundEndTxNum uint64
	var foundStartTxNum uint64
	var found bool
	d.files[EfHistory].AscendGreaterOrEqual(&search, func(i btree.Item) bool {
		item := i.(*filesItem)
		offset := item.indexReader.Lookup(key)
		g := item.getter
		g.Reset(offset)
		if keyMatch, _ := g.Match(key); keyMatch {
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			if n, ok := ef.Search(txNum + 1); ok {
				foundTxNum = n
				foundEndTxNum = item.endTxNum
				foundStartTxNum = item.startTxNum
				found = true
				return false
			}
		}
		return true
	})
	if !found {
		// Value not found in history files, look in the recent history
		indexCursor, err := d.tx.CursorDupSort(d.indexTable)
		if err != nil {
			return nil, false, err
		}
		defer indexCursor.Close()
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], txNum+1)
		var foundTxNumVal []byte
		if foundTxNumVal, err = indexCursor.SeekBothRange(key, txKey[:]); err != nil {
			return nil, false, err
		}
		if foundTxNumVal != nil {
			var historyKeysCursor kv.CursorDupSort
			if historyKeysCursor, err = d.tx.CursorDupSort(d.historyKeysTable); err != nil {
				return nil, false, err
			}
			defer historyKeysCursor.Close()
			var vn []byte
			if vn, err = historyKeysCursor.SeekBothRange(foundTxNumVal, key); err != nil {
				return nil, false, err
			}
			valNum := binary.BigEndian.Uint64(vn[len(vn)-8:])
			if valNum == 0 {
				// This is special valNum == 0, which is empty value
				return nil, true, nil
			}
			var v []byte
			if v, err = d.tx.GetOne(d.historyValsTable, vn[len(vn)-8:]); err != nil {
				return nil, false, err
			}
			return v, true, nil
		}
		return nil, false, nil
	}
	var lookupKey = make([]byte, len(key)+8)
	binary.BigEndian.PutUint64(lookupKey, foundTxNum)
	copy(lookupKey[8:], key)
	var historyItem *filesItem
	search.startTxNum = foundStartTxNum
	search.endTxNum = foundEndTxNum
	if i := d.files[History].Get(&search); i != nil {
		historyItem = i.(*filesItem)
	} else {
		return nil, false, fmt.Errorf("no %s file found for [%x]", d.filenameBase, key)
	}
	offset := historyItem.indexReader.Lookup(lookupKey)
	g := historyItem.getter
	g.Reset(offset)
	v, _ := g.Next(nil)
	return v, true, nil
}

func (d *Domain) getAfterTxNum(key []byte, txNum uint64) ([]byte, error) {
	v, hOk, err := d.historyAfterTxNum(key, txNum)
	if err != nil {
		return nil, err
	}
	if hOk {
		return v, nil
	}
	if v, _, err = d.get(key); err != nil {
		return nil, err
	}
	return v, nil
}
