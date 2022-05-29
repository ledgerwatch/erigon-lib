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
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
)

type InvertedIndex struct {
	dir             string // Directory where static files are created
	aggregationStep uint64
	filenameBase    string
	indexTable      string // Needs to be table with DupSort
	tx              kv.RwTx
	txNum           uint64
	files           *btree.BTree
}

func NewInvertedIndex(
	dir string,
	aggregationStep uint64,
	filenameBase string,
	indexTable string,
) (*InvertedIndex, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	ii := &InvertedIndex{
		dir:             dir,
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
		indexTable:      indexTable,
	}
	ii.files = btree.New(32)
	ii.scanStateFiles(files)
	ii.openFiles()
	return ii, nil
}

func (ii *InvertedIndex) scanStateFiles(files []fs.DirEntry) {
	re := regexp.MustCompile(ii.filenameBase + ".([0-9]+)-([0-9]+).(dat|idx)")
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
		var item = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		var foundI *filesItem
		ii.files.AscendGreaterOrEqual(&filesItem{startTxNum: endTxNum, endTxNum: endTxNum}, func(i btree.Item) bool {
			it := i.(*filesItem)
			if it.endTxNum == endTxNum {
				foundI = it
			}
			return false
		})
		if foundI == nil || foundI.startTxNum > startTxNum {
			log.Info("Load state file", "name", name, "startTxNum", startTxNum, "endTxNum", endTxNum)
			ii.files.ReplaceOrInsert(item)
		}
	}
}

func (ii *InvertedIndex) openFiles() error {
	var err error
	var totalKeys uint64
	ii.files.Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.dat", ii.filenameBase, item.startTxNum, item.endTxNum))
		if item.decompressor, err = compress.NewDecompressor(path.Join(ii.dir, datPath)); err != nil {
			return false
		}
		idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.idx", ii.filenameBase, item.startTxNum, item.endTxNum))
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

func (ii *InvertedIndex) closeFiles() {
	ii.files.Ascend(func(i btree.Item) bool {
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

func (ii *InvertedIndex) Close() {
	ii.closeFiles()
}

func (ii *InvertedIndex) SetTx(tx kv.RwTx) {
	ii.tx = tx
}

func (ii *InvertedIndex) SetTxNum(txNum uint64) {
	ii.txNum = txNum
}
