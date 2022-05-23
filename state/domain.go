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
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
)

var historyValCountKey = []byte("ValCount")

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
	NumberOfTypes
)

func (ft FileType) String() string {
	switch ft {
	case Values:
		return "values"
	case History:
		return "history"
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
	historyValsCount string // Table containing just one record - counter of value number (keys in the historyValsTable)
	indexTable       string // Needs to be table with DupSort
	tx               kv.RwTx
	txNum            uint64
	files            [NumberOfTypes]*btree.BTree // Static files pertaining to this domain, items are of type `filesItem`
}

func NewDomain(
	dir string,
	aggregationStep uint64,
	filenameBase string,
	keysTable string,
	valsTable string,
	historyKeysTable string,
	historyValsTable string,
	historyValsCount string,
	indexTable string,
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
		historyValsCount: historyValsCount,
		indexTable:       indexTable,
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
	if len(original) > 0 {
		val, err := d.tx.GetOne(d.historyValsCount, historyValCountKey)
		if err != nil {
			return err
		}
		var valNum uint64
		if len(val) > 0 {
			valNum = binary.BigEndian.Uint64(val)
		}
		valNum++
		binary.BigEndian.PutUint64(historyKey, valNum)
		if err = d.tx.Put(d.historyValsCount, historyValCountKey, historyKey[:8]); err != nil {
			return err
		}
		if err = d.tx.Put(d.historyValsTable, historyKey[:8], original); err != nil {
			return err
		}
	}
	copy(historyKey[8:], key)
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
	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	if err = d.tx.Put(d.valsTable, keySuffix, val); err != nil {
		return err
	}
	if err = d.update(key, original); err != nil {
		return err
	}
	return nil
}

func (d *Domain) Delete(key []byte) error {
	original, _, err := d.get(key)
	if err != nil {
		return err
	}
	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	if err = d.tx.Delete(d.valsTable, keySuffix, nil); err != nil {
		return err
	}
	if err = d.update(key, original); err != nil {
		return err
	}
	return nil
}

func (d *Domain) IteratePrefix(prefix []byte, it func(k []byte)) error {
	keyCursor, err := d.tx.CursorDupSort(d.keysTable)
	if err != nil {
		return err
	}
	defer keyCursor.Close()
	var k []byte
	for k, _, err = keyCursor.Seek(prefix); err == nil && bytes.HasPrefix(k, prefix); k, _, err = keyCursor.Next() {
		var invertedStep []byte
		if invertedStep, err = keyCursor.LastDup(); err != nil {
			return err
		}
		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], invertedStep)
		var v []byte
		if v, err = d.tx.GetOne(d.valsTable, keySuffix); err != nil {
			return err
		}
		if len(v) > 0 {
			it(k)
		}
	}
	if err != nil {
		return err
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
	keyCursor, err := roTx.CursorDupSort(d.keysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s keys cursor: %w", d.filenameBase, err)
	}
	defer keyCursor.Close()
	var k []byte
	valuesCount := 0
	for k, _, err = keyCursor.First(); err == nil && k != nil; k, _, err = keyCursor.Next() {
		invertedStep, err := keyCursor.LastDup()
		if err != nil {
			return Collation{}, fmt.Errorf("find last %s key for aggregation step k=[%x]: %w", d.filenameBase, k, err)
		}
		s := ^binary.BigEndian.Uint64(invertedStep)
		if s == step {
			keySuffix := make([]byte, len(k)+8)
			copy(keySuffix, k)
			copy(keySuffix[len(k):], invertedStep)
			v, err := roTx.GetOne(d.valsTable, keySuffix)
			if err != nil {
				return Collation{}, fmt.Errorf("find last %s value for aggregation step k=[%x]: %w", d.filenameBase, k, err)
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
	var v, val []byte
	var historyKey []byte
	for k, v, err = historyKeysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		historyKey = append(append(historyKey[:0], k...), v[8:]...)
		if err = historyComp.AddUncompressedWord(historyKey); err != nil {
			return Collation{}, fmt.Errorf("add %s history key [%x]: %w", d.filenameBase, k, err)
		}
		valNum := binary.BigEndian.Uint64(v)
		if valNum == 0 {
			val = nil
		} else {
			if val, err = roTx.GetOne(d.historyValsTable, v[:8]); err != nil {
				return Collation{}, fmt.Errorf("get %s history val [%x]=>%d: %w", d.filenameBase, k, valNum, err)
			}
		}
		if err = historyComp.AddUncompressedWord(val); err != nil {
			return Collation{}, fmt.Errorf("add %s history val [%x]=>[%x]: %w", d.filenameBase, k, val, err)
		}
		historyCount++
		var bitmap *roaring64.Bitmap
		var ok bool
		if bitmap, ok = indexBitmaps[string(v[8:])]; !ok {
			bitmap = roaring64.New()
			indexBitmaps[string(v[8:])] = bitmap
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
	valuesDecomp  *compress.Decompressor
	valuesIdx     *recsplit.Index
	historyDecomp *compress.Decompressor
	historyIdx    *recsplit.Index
}

func (sf StaticFiles) Close() {
	sf.valuesDecomp.Close()
	sf.valuesIdx.Close()
	sf.historyDecomp.Close()
	sf.historyIdx.Close()
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (d *Domain) buildFiles(step uint64, collation Collation) (StaticFiles, error) {
	valuesComp := collation.valuesComp
	historyComp := collation.historyComp
	var valuesDecomp, historyDecomp *compress.Decompressor
	var valuesIdx, historyIdx *recsplit.Index
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
		}
	}()
	blockFrom := step * d.aggregationStep
	blockTo := (step + 1) * d.aggregationStep
	valuesIdxPath := filepath.Join(d.dir, fmt.Sprintf("%s-values.%d-%d.idx", d.filenameBase, blockFrom, blockTo))
	var err error
	if err = valuesComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s values: %w", d.filenameBase, err)
	}
	valuesComp.Close()
	valuesComp = nil
	if valuesDecomp, err = compress.NewDecompressor(collation.valuesPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s values decompressor: %w", d.filenameBase, err)
	}
	if valuesIdx, err = buildIndex(valuesDecomp, valuesIdxPath, d.dir, collation.valuesCount); err != nil {
		return StaticFiles{}, fmt.Errorf("build %s values idx: %w", d.filenameBase, err)
	}
	historyIdxPath := filepath.Join(d.dir, fmt.Sprintf("%s-history.%d-%d.idx", d.filenameBase, blockFrom, blockTo))
	if err = historyComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s history: %w", d.filenameBase, err)
	}
	historyComp.Close()
	historyComp = nil
	if historyDecomp, err = compress.NewDecompressor(collation.historyPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s history decompressor: %w", d.filenameBase, err)
	}
	if historyIdx, err = buildIndex(historyDecomp, historyIdxPath, d.dir, collation.historyCount); err != nil {
		return StaticFiles{}, fmt.Errorf("build %s history idx: %w", d.filenameBase, err)
	}
	closeComp = false
	return StaticFiles{
		valuesDecomp:  valuesDecomp,
		valuesIdx:     valuesIdx,
		historyDecomp: historyDecomp,
		historyIdx:    historyIdx,
	}, nil
}

func buildIndex(d *compress.Decompressor, idxPath, dir string, count int) (*recsplit.Index, error) {
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
	var pos uint64
	g := d.MakeGetter()
	for {
		g.Reset(0)
		for g.HasNext() {
			word, _ = g.Next(word[:0])
			if err = rs.AddKey(word, pos); err != nil {
				return nil, fmt.Errorf("add idx key [%x]: %w", word, err)
			}
			// Skip value
			pos = g.Skip()
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

func (a *Domain) readFromFiles(fType FileType, filekey []byte) ([]byte, bool) {
	var val []byte
	var found bool
	a.files[fType].Descend(func(i btree.Item) bool {
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
