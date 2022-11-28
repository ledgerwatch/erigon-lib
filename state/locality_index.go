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
	"math/bits"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
)

const LocalityIndexUint64Limit = 64 //bitmap spend 1 bit per file, stored as uint64

// LocalityIndex - has info in which .ef files exists given key
// Format: key -> bitmap(step_number_list)
// step_number_list is list of .ef files where exists given key
type LocalityIndex struct {
	//file         *filesItem
	filenameBase    string
	dir             string // Directory where static files are created
	tmpdir          string // Directory where static files are created
	aggregationStep uint64 // Directory where static files are created

	file *filesItem
}

func NewLocalityIndex(
	dir, tmpdir string,
	aggregationStep uint64,
	filenameBase string,
) (*LocalityIndex, error) {
	li := &LocalityIndex{
		dir:             dir,
		tmpdir:          tmpdir,
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("NewInvertedIndex: %s, %w", filenameBase, err)
	}
	uselessFiles := li.scanStateFiles(files)
	for _, f := range uselessFiles {
		_ = os.Remove(filepath.Join(li.dir, f))
	}
	if err = li.openFiles(); err != nil {
		return nil, fmt.Errorf("NewInvertedIndex: %s, %w", filenameBase, err)
	}
	return li, nil
}

func (li *LocalityIndex) scanStateFiles(files []fs.DirEntry) (uselessFiles []string) {
	re := regexp.MustCompile("^" + li.filenameBase + ".([0-9]+)-([0-9]+).li$")
	var err error
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

		if startStep != 0 {
			log.Warn("LocalityIndex must always starts from step 0")
			continue
		}
		if endStep > StepsInBiggestFile*LocalityIndexUint64Limit {
			log.Warn("LocalityIndex does store bitmaps as uint64, means it can't handle > 2048 steps. But it's possible to implement")
			continue
		}

		startTxNum, endTxNum := startStep*li.aggregationStep, endStep*li.aggregationStep
		if li.file == nil {
			li.file = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		} else if li.file.endTxNum < endTxNum {
			uselessFiles = append(uselessFiles,
				fmt.Sprintf("%s.%d-%d.li", li.filenameBase, li.file.startTxNum/li.aggregationStep, li.file.endTxNum/li.aggregationStep),
			)
			li.file = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		}
	}
	return uselessFiles
}

func (li *LocalityIndex) openFiles() (err error) {
	if li.file == nil {
		return nil
	}
	fromStep, toStep := li.file.startTxNum/li.aggregationStep, li.file.endTxNum/li.aggregationStep
	idxPath := filepath.Join(li.dir, fmt.Sprintf("%s.%d-%d.li", li.filenameBase, fromStep, toStep))
	li.file.index, err = recsplit.OpenIndex(idxPath)
	if err != nil {
		return fmt.Errorf("LocalityIndex.openFiles: %w, %s", err, idxPath)
	}
	return nil
}

func (li *LocalityIndex) closeFiles() {
	if li.file.index != nil {
		li.file.index.Close()
	}
}

func (li *LocalityIndex) Close() {
	li.closeFiles()
}
func (li *LocalityIndex) Files() (res []string) { return res }
func (li *LocalityIndex) NewIdxReader() *recsplit.IndexReader {
	if li != nil && li.file != nil && &li.file.index != nil {
		return recsplit.NewIndexReader(li.file.index)
	}
	return nil
}

func (li *LocalityIndex) lookupIdxFiles(r *recsplit.IndexReader, key []byte, fromTxNum uint64, files *btree.BTreeG[ctxItem]) (exactShard1, exactShard2 ctxItem, orSearchFromTxn uint64, ok1, ok2 bool) {
	if li == nil || li.file == nil || li.file.index == nil {
		return exactShard1, exactShard2, fromTxNum, false, false
	}

	n1, n2, ok1, ok2 := li.lookup(r, key, fromTxNum)

	if ok1 {
		var ok bool
		exactShard1, ok = files.Get(ctxItem{startTxNum: n1 * li.aggregationStep, endTxNum: (n1 + StepsInBiggestFile) * li.aggregationStep})
		if !ok {
			panic(n1)
		}
	}

	if ok2 {
		var ok bool
		exactShard2, ok = files.Get(ctxItem{startTxNum: n2 * li.aggregationStep, endTxNum: (n2 + StepsInBiggestFile) * li.aggregationStep})
		if !ok {
			panic(n2)
		}
	}
	return exactShard1, exactShard2, cmp.Max(li.file.endTxNum+1, fromTxNum), ok1, ok2
}

// prevents searching key in many files
// LocalityIndex return exactly 2 file (step)
func (li *LocalityIndex) lookup(r *recsplit.IndexReader, key []byte, fromTxNum uint64) (exactShardNum1, exactShardNum2 uint64, ok1, ok2 bool) {
	if li == nil || li.file == nil || li.file.index == nil {
		return 0, 0, false, false
	}

	fileNumbers := r.Lookup(key)
	fromFileNum := fromTxNum / li.aggregationStep / StepsInBiggestFile
	if fromFileNum > 0 {
		fileNumbers = (fileNumbers >> fromFileNum) << fromFileNum // clear first N bits
	}
	//if bytes.Equal(key, hex.MustDecodeString("009ba32869045058a3f05d6f3dd2abb967e338f6")) {
	//	fmt.Printf("locIndex2: %x, %b\n", key, fileNumbers)
	//}
	if fileNumbers > 0 {
		ok1 = true
		n := bits.TrailingZeros64(fileNumbers)
		exactShardNum1 = uint64(n * StepsInBiggestFile)
		//if bytes.Equal(key, hex.MustDecodeString("009ba32869045058a3f05d6f3dd2abb967e338f6")) {
		//	fmt.Printf("locIndex3: %x, %b, %d, %d\n", key, fileNumbers, n, exactShardNum)
		//}
		fileNumbers = (fileNumbers >> (n + 1)) << (n + 1) // clear first N bits
		if fileNumbers > 0 {
			ok2 = true
			n = bits.TrailingZeros64(fileNumbers)
			exactShardNum2 = uint64(n * StepsInBiggestFile)
			//if bytes.Equal(key, hex.MustDecodeString("009ba32869045058a3f05d6f3dd2abb967e338f6")) {
			//	fmt.Printf("locIndex4: %x, %b, %d, %d\n", key, fileNumbers, n, exactShardNum)
			//}
		}
	} else {
		//if bytes.Equal(key, hex.MustDecodeString("009ba32869045058a3f05d6f3dd2abb967e338f6")) {
		//fmt.Printf("can early return! %x, %d, txNum=%d\n", key, bm.ToArray(), txNum)
		//}

		//TODO: can't early return, because maybe index returned false-positive...
		//return nil, false, nil
	}

	//if bytes.Equal(key, hex.MustDecodeString("009ba32869045058a3f05d6f3dd2abb967e338f6")) {
	//	fmt.Printf("foundExactShard: %x, %t, %d, %d, stepSize=%d\n", key, foundExactShard1, exactShard1, exactShard2, hc.h.aggregationStep)
	//}
	return exactShardNum1, exactShardNum2, ok1, ok2
}

func (li *LocalityIndex) missedIdxFiles(ii *InvertedIndex) (toStep uint64, idxExists bool) {
	ii.files.Descend(func(item *filesItem) bool {
		if item.endTxNum-item.startTxNum == StepsInBiggestFile*ii.aggregationStep {
			toStep = item.endTxNum / ii.aggregationStep
			return false
		}
		return true
	})
	fName := fmt.Sprintf("%s.%d-%d.li", li.filenameBase, 0, toStep)
	return toStep, dir.FileExist(filepath.Join(li.dir, fName))
}

func (li *LocalityIndex) BuildMissedIndices(ctx context.Context, ii *InvertedIndex) error {
	if li == nil {
		return nil
	}
	toStep, idxExists := li.missedIdxFiles(ii)
	if idxExists {
		return nil
	}
	if toStep == 0 {
		return nil
	}
	defer ii.EnableMadvNormalReadAhead().DisableReadAhead()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	fromStep := uint64(0)

	count := 0
	it := ii.MakeContext().iterateKeysLocality(toStep * ii.aggregationStep)
	total := float64(it.Total())
	for it.HasNext() {
		k, _, progress := it.Next()
		count++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			log.Info("[LocalityIndex] build step1", "name", ii.filenameBase, "k", fmt.Sprintf("%x", k), "progress", fmt.Sprintf("%.2f%%", ((float64(progress)/total)*100)/2))
		default:
		}
	}
	log.Info("[LocalityIndex] keys amount", "total", count)

	fName := fmt.Sprintf("%s.%d-%d.li", ii.filenameBase, fromStep, toStep)
	idxPath := filepath.Join(ii.dir, fName)

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     ii.tmpdir,
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
		it = ii.MakeContext().iterateKeysLocality(toStep * ii.aggregationStep)
		for it.HasNext() {
			k, filesBitmap, progress := it.Next()
			binary.BigEndian.PutUint64(bm, filesBitmap)

			//if bytes.Equal(k, hex.MustDecodeString("e0a2bd4258d2768837baa26a28fe71dc079f84c7")) {
			//fmt.Printf(".l file: %x, %b\n", k, filesBitmap)
			//}
			if err = rs.AddKey(k, filesBitmap); err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info("[LocalityIndex] build step2", "name", ii.filenameBase, "k", fmt.Sprintf("%x", k), "progress", fmt.Sprintf("%.2f%%", 50+((float64(progress)/total)*100)/2))
			default:
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

	var oldFile *filesItem
	if li.file != nil {
		oldFile = li.file
	}
	idx, err := recsplit.OpenIndex(idxPath)
	if err != nil {
		return fmt.Errorf("open idx: %w", err)
	}
	li.file = &filesItem{index: idx, startTxNum: fromStep * ii.aggregationStep, endTxNum: toStep * ii.aggregationStep}
	if oldFile != nil {
		_ = os.Remove(filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.li", ii.filenameBase, oldFile.startTxNum/ii.aggregationStep, oldFile.endTxNum/ii.aggregationStep)))
	}
	return nil
}

type LocalityIterator struct {
	hc         *InvertedIndexContext
	h          ReconHeap
	bitmap     uint64
	nextBitmap uint64
	nextKey    []byte
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
		inStep := uint32(top.startTxNum / si.hc.ii.aggregationStep)
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
	return si.nextKey, si.nextBitmap, si.progress
}

func (hc *InvertedIndexContext) iterateKeysLocality(uptoTxNum uint64) *LocalityIterator {
	si := &LocalityIterator{hc: hc, uptoTxNum: uptoTxNum}
	hc.files.Ascend(func(item ctxItem) bool {
		if (item.endTxNum-item.startTxNum)/hc.ii.aggregationStep != StepsInBiggestFile {
			return false
		}
		if item.startTxNum > uptoTxNum {
			return false
		}
		g := item.getter
		if g.HasNext() {
			key, offset := g.NextUncompressed()
			heap.Push(&si.h, &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: g, txNum: ^item.endTxNum, key: key, startOffset: offset, lastOffset: offset})
		}
		si.total += uint64(item.getter.Size())
		return true
	})
	si.advance()
	return si
}
