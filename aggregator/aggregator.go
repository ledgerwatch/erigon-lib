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

package aggregator

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"github.com/spaolacci/murmur3"
	"golang.org/x/crypto/sha3"
)

// Aggregator of multiple state files to support state reader and state writer
// The convension for the file names are as follows
// State is composed of three types of files:
// 1. Accounts. keys are addresses (20 bytes), values are encoding of accounts
// 2. Contract storage. Keys are concatenation of addresses (20 bytes) and storage locations (32 bytes), values have their leading zeroes removed
// 3. Contract codes. Keys are addresses (20 bytes), values are bycodes
// Within each type, any file can cover an interval of block numbers, for example, `accounts.1-16` represents changes in accounts
// that were effected by the blocks from 1 to 16, inclusively. The second component of the interval will be called "end block" for the file.
// Finally, for each type and interval, there are two files - one with the compressed data (extension `dat`),
// and another with the index (extension `idx`) consisting of the minimal perfect hash table mapping keys to the offsets of corresponding keys
// in the data file
// Aggregator consists (apart from the file it is aggregating) of the 4 parts:
// 1. Persistent table of expiration time for each of the files. Key - name of the file, value - timestamp, at which the file can be removed
// 2. Transient (in-memory) mapping the "end block" of each file to the objects required for accessing the file (compress.Decompressor and resplit.Index)
// 3. Persistent tables (one for accounts, one for contract storage, and one for contract code) summarising all the 1-block state diff files
//    that were not yet merged together to form larger files. In these tables, keys are the same as keys in the state diff files, but values are also
//    augemented by the number of state diff files this key is present. This number gets decremented every time when a 1-block state diff files is removed
//    from the summary table (due to being merged). And when this number gets to 0, the record is deleted from the summary table.
//    This number is encoded into first 4 bytes of the value
// 4. Aggregating persistent hash table. Maps state keys to the block numbers for the use in the part 2 (which is not necessarily the block number where
//    the item last changed, but it is guaranteed to find correct element in the Transient mapping of part 2

type FileType int

const (
	Account FileType = iota
	Storage
	Code
	Commitment
	AccountHistory
	StorageHistory
	CodeHistory
	AccountBitmap
	StorageBitmap
	CodeBitmap
	NumberOfTypes
)

const (
	FirstType                   = Account
	NumberOfAccountStorageTypes = Code
	NumberOfStateTypes          = AccountHistory
)

func (ft FileType) String() string {
	switch ft {
	case Account:
		return "account"
	case Storage:
		return "storage"
	case Code:
		return "code"
	case Commitment:
		return "commitment"
	case AccountHistory:
		return "ahistory"
	case CodeHistory:
		return "chistory"
	case StorageHistory:
		return "shistory"
	case AccountBitmap:
		return "abitmap"
	case CodeBitmap:
		return "cbitmap"
	case StorageBitmap:
		return "sbitmap"
	default:
		panic(fmt.Sprintf("unknown file type: %d", ft))
	}
}

func ParseFileType(s string) (FileType, bool) {
	switch s {
	case "account":
		return Account, true
	case "storage":
		return Storage, true
	case "code":
		return Code, true
	case "commitment":
		return Commitment, true
	case "ahistory":
		return AccountHistory, true
	case "chistory":
		return CodeHistory, true
	case "shistory":
		return StorageHistory, true
	case "abitmap":
		return AccountBitmap, true
	case "cbitmap":
		return CodeBitmap, true
	case "sbitmap":
		return StorageBitmap, true
	default:
		return NumberOfTypes, false
	}
}

type Aggregator struct {
	diffDir              string // Directory where the state diff files are stored
	files                [NumberOfTypes]*btree.BTree
	fileLocks            [NumberOfTypes]sync.RWMutex
	unwindLimit          uint64              // How far the chain may unwind
	aggregationStep      uint64              // How many items (block, but later perhaps txs or changes) are required to form one state diff file
	changesBtree         *btree.BTree        // btree of ChangesItem
	trace                bool                // Turns on tracing for specific accounts and locations
	tracedKeys           map[string]struct{} // Set of keys being traced during aggregations
	hph                  *commitment.HexPatriciaHashed
	keccak               hash.Hash
	changesets           bool // Whether to generate changesets (off by default)
	commitments          bool // Whether to calculate commitments
	aggChannel           chan *AggregationTask
	aggBackCh            chan struct{} // Channel for acknoledgement of AggregationTask
	aggError             chan error
	aggWg                sync.WaitGroup
	mergeChannel         chan struct{}
	mergeError           chan error
	mergeWg              sync.WaitGroup
	historyChannel       chan struct{}
	historyError         chan error
	historyWg            sync.WaitGroup
	trees                [NumberOfStateTypes]*btree.BTree
	fileHits, fileMisses uint64                       // Counters for state file hit ratio
	arches               [NumberOfStateTypes][]uint32 // Over-arching hash tables containing the block number of last aggregation
	archHasher           murmur3.Hash128
}

type ChangeFile struct {
	dir         string
	step        uint64
	namebase    string
	path        string
	file        *os.File
	w           *bufio.Writer
	r           *bufio.Reader
	numBuf      [8]byte
	sizeCounter uint64
	txPos       int64 // Position of the last block iterated upon
	txNum       uint64
	txSize      uint64
	txRemaining uint64 // Remaining number of bytes to read in the current transaction
	words       []byte // Words pending for the next block record, in the same slice
	wordOffsets []int  // Offsets of words in the `words` slice
}

func (cf *ChangeFile) closeFile() error {
	if len(cf.wordOffsets) > 0 {
		return fmt.Errorf("closeFile without finish")
	}
	if cf.w != nil {
		if err := cf.w.Flush(); err != nil {
			return err
		}
		cf.w = nil
	}
	if cf.file != nil {
		if err := cf.file.Close(); err != nil {
			return err
		}
		cf.file = nil
	}
	return nil
}

func (cf *ChangeFile) openFile(blockNum uint64, write bool) error {
	if len(cf.wordOffsets) > 0 {
		return fmt.Errorf("openFile without finish")
	}
	rem := blockNum % cf.step
	startBlock := blockNum - rem
	endBlock := startBlock + cf.step - 1
	if cf.w == nil {
		cf.path = filepath.Join(cf.dir, fmt.Sprintf("%s.%d-%d.chg", cf.namebase, startBlock, endBlock))
		var err error
		if write {
			if cf.file, err = os.OpenFile(cf.path, os.O_RDWR|os.O_CREATE, 0755); err != nil {
				return err
			}
		} else {
			if cf.file, err = os.Open(cf.path); err != nil {
				return err
			}
		}
		if cf.txPos, err = cf.file.Seek(0, 2 /* relative to the end of the file */); err != nil {
			return err
		}
		if write {
			cf.w = bufio.NewWriter(cf.file)
		}
		cf.r = bufio.NewReader(cf.file)
	}
	return nil
}

func (cf *ChangeFile) rewind() error {
	var err error
	if cf.txPos, err = cf.file.Seek(0, 2 /* relative to the end of the file */); err != nil {
		return err
	}
	cf.r = bufio.NewReader(cf.file)
	return nil
}

func (cf *ChangeFile) add(word []byte) {
	cf.words = append(cf.words, word...)
	cf.wordOffsets = append(cf.wordOffsets, len(cf.words))
}

func (cf *ChangeFile) finish(txNum uint64) error {
	// Write out words
	lastOffset := 0
	for _, offset := range cf.wordOffsets {
		word := cf.words[lastOffset:offset]
		n := binary.PutUvarint(cf.numBuf[:], uint64(len(word)))
		if _, err := cf.w.Write(cf.numBuf[:n]); err != nil {
			return err
		}
		if len(word) > 0 {
			if _, err := cf.w.Write(word); err != nil {
				return err
			}
		}
		cf.sizeCounter += uint64(n + len(word))
		lastOffset = offset
	}
	cf.words = cf.words[:0]
	cf.wordOffsets = cf.wordOffsets[:0]
	// Write out tx number and then size of changes in this block
	binary.BigEndian.PutUint64(cf.numBuf[:], txNum)
	if _, err := cf.w.Write(cf.numBuf[:]); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(cf.numBuf[:], cf.sizeCounter)
	if _, err := cf.w.Write(cf.numBuf[:]); err != nil {
		return err
	}
	cf.sizeCounter = 0
	return nil
}

// prevTx positions the reader to the beginning
// of the transaction
func (cf *ChangeFile) prevTx() (bool, error) {
	if cf.txPos == 0 {
		return false, nil
	}
	// Move back 16 bytes to read tx number and tx size
	pos, err := cf.file.Seek(cf.txPos-16, 0 /* relative to the beginning */)
	if err != nil {
		return false, err
	}
	cf.r.Reset(cf.file)
	if _, err = io.ReadFull(cf.r, cf.numBuf[:8]); err != nil {
		return false, err
	}
	cf.txNum = binary.BigEndian.Uint64(cf.numBuf[:])
	if _, err = io.ReadFull(cf.r, cf.numBuf[:8]); err != nil {
		return false, err
	}
	cf.txSize = binary.BigEndian.Uint64(cf.numBuf[:])
	cf.txRemaining = cf.txSize
	cf.txPos, err = cf.file.Seek(pos-int64(cf.txSize), 0)
	if err != nil {
		return false, err
	}
	cf.r.Reset(cf.file)
	return true, nil
}

func (cf *ChangeFile) nextWord(wordBuf []byte) ([]byte, bool, error) {
	if cf.txRemaining == 0 {
		return wordBuf, false, nil
	}
	ws, err := binary.ReadUvarint(cf.r)
	if err != nil {
		return wordBuf, false, fmt.Errorf("word size: %w", err)
	}
	var buf []byte
	if total := len(wordBuf) + int(ws); cap(wordBuf) >= total {
		buf = wordBuf[:total] // Reuse the space in wordBuf, is it has enough capacity
	} else {
		buf = make([]byte, total)
		copy(buf, wordBuf)
	}
	if _, err = io.ReadFull(cf.r, buf[len(wordBuf):]); err != nil {
		return wordBuf, false, fmt.Errorf("read word (%d %d): %w", ws, len(buf[len(wordBuf):]), err)
	}
	n := binary.PutUvarint(cf.numBuf[:], ws)
	cf.txRemaining -= uint64(n) + ws
	return buf, true, nil
}

func (cf *ChangeFile) deleteFile() error {
	return os.Remove(cf.path)
}

type Changes struct {
	namebase string
	keys     ChangeFile
	before   ChangeFile
	after    ChangeFile
	step     uint64
	dir      string
	beforeOn bool
}

func (c *Changes) Init(namebase string, step uint64, dir string, beforeOn bool) {
	c.namebase = namebase
	c.step = step
	c.dir = dir
	c.keys.namebase = namebase + ".keys"
	c.keys.dir = dir
	c.keys.step = step
	c.before.namebase = namebase + ".before"
	c.before.dir = dir
	c.before.step = step
	c.after.namebase = namebase + ".after"
	c.after.dir = dir
	c.after.step = step
	c.beforeOn = beforeOn
}

func (c *Changes) closeFiles() error {
	if err := c.keys.closeFile(); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.closeFile(); err != nil {
			return err
		}
	}
	if err := c.after.closeFile(); err != nil {
		return err
	}
	return nil
}

func (c *Changes) openFiles(blockNum uint64, write bool) error {
	if err := c.keys.openFile(blockNum, write); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.openFile(blockNum, write); err != nil {
			return err
		}
	}
	if err := c.after.openFile(blockNum, write); err != nil {
		return err
	}
	return nil
}

func (c *Changes) insert(key, after []byte) {
	c.keys.add(key)
	if c.beforeOn {
		c.before.add(nil)
	}
	c.after.add(after)
}

func (c *Changes) update(key, before, after []byte) {
	c.keys.add(key)
	if c.beforeOn {
		c.before.add(before)
	}
	c.after.add(after)
}

func (c *Changes) delete(key, before []byte) {
	c.keys.add(key)
	if c.beforeOn {
		c.before.add(before)
	}
	c.after.add(nil)
}

func (c *Changes) finish(txNum uint64) error {
	if err := c.keys.finish(txNum); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.finish(txNum); err != nil {
			return err
		}
	}
	if err := c.after.finish(txNum); err != nil {
		return err
	}
	return nil
}

func (c *Changes) prevTx() (bool, uint64, error) {
	bkeys, err := c.keys.prevTx()
	if err != nil {
		return false, 0, err
	}
	var bbefore, bafter bool
	if c.beforeOn {
		if bbefore, err = c.before.prevTx(); err != nil {
			return false, 0, err
		}
	}
	if bafter, err = c.after.prevTx(); err != nil {
		return false, 0, err
	}
	if c.beforeOn && bkeys != bbefore {
		return false, 0, fmt.Errorf("inconsistent tx iteration")
	}
	if bkeys != bafter {
		return false, 0, fmt.Errorf("inconsistent tx iteration")
	}
	txNum := c.keys.txNum
	if c.beforeOn {
		if txNum != c.before.txNum {
			return false, 0, fmt.Errorf("inconsistent txNum, keys: %d, before: %d", txNum, c.before.txNum)
		}
	}
	if txNum != c.after.txNum {
		return false, 0, fmt.Errorf("inconsistent txNum, keys: %d, after: %d", txNum, c.after.txNum)
	}
	return bkeys, txNum, nil
}

func (c *Changes) rewind() error {
	if err := c.keys.rewind(); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.rewind(); err != nil {
			return err
		}
	}
	if err := c.after.rewind(); err != nil {
		return err
	}
	return nil
}

func (c *Changes) nextTriple(keyBuf, beforeBuf []byte, afterBuf []byte) ([]byte, []byte, []byte, bool, error) {
	key, bkeys, err := c.keys.nextWord(keyBuf)
	if err != nil {
		return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("next key: %w", err)
	}
	var before, after []byte
	var bbefore, bafter bool
	if c.beforeOn {
		if before, bbefore, err = c.before.nextWord(beforeBuf); err != nil {
			return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("next before: %w", err)
		}
	}
	if c.beforeOn && bkeys != bbefore {
		return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("inconsistent word iteration")
	}
	if after, bafter, err = c.after.nextWord(afterBuf); err != nil {
		return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("next after: %w", err)
	}
	if bkeys != bafter {
		return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("inconsistent word iteration")
	}
	return key, before, after, bkeys, nil
}

func (c *Changes) deleteFiles() error {
	if err := c.keys.deleteFile(); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.deleteFile(); err != nil {
			return err
		}
	}
	if err := c.after.deleteFile(); err != nil {
		return err
	}
	return nil
}

func buildIndex(d *compress.Decompressor, idxPath, tmpDir string, count int) (*recsplit.Index, error) {
	var rs *recsplit.RecSplit
	var err error
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		IndexFile: idxPath,
	}); err != nil {
		return nil, err
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
				return nil, err
			}
			// Skip value
			pos = g.Skip()
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return nil, err
			}
		} else {
			break
		}
	}
	var idx *recsplit.Index
	if idx, err = recsplit.OpenIndex(idxPath); err != nil {
		return nil, err
	}
	return idx, nil
}

// aggregate gathers changes from the changefiles into a B-tree, and "removes" them from the database
// This function is time-critical because it needs to be run in the same go-routine (thread) as the general
// execution (due to read-write tx). After that, we can optimistically execute the rest in the background
func (c *Changes) aggregate(blockFrom, blockTo uint64, prefixLen int, dbTree *btree.BTree, commitments bool) (*btree.BTree, error) {
	if err := c.openFiles(blockTo, false /* write */); err != nil {
		return nil, fmt.Errorf("open files: %w", err)
	}
	bt := btree.New(32)
	err := c.aggregateToBtree(bt, prefixLen, commitments)
	if err != nil {
		return nil, fmt.Errorf("aggregateToBtree: %w", err)
	}
	// Clean up the DB table
	var e error
	var search AggregateItem
	bt.Ascend(func(i btree.Item) bool {
		item := i.(*AggregateItem)
		if item.count == 0 {
			return true
		}
		search.k = item.k
		var prevV *AggregateItem
		if prevVI := dbTree.Get(&search); prevVI != nil {
			prevV = prevVI.(*AggregateItem)
		}
		if prevV == nil {
			e = fmt.Errorf("record not found in db tree for key %x", item.k)
			return false
		}
		if prevV.count < item.count {
			e = fmt.Errorf("record count too low for key [%x] count %d, subtracting %d", item.k, prevV.count, item.count)
			return false
		}
		if prevV.count == item.count {
			dbTree.Delete(prevV)
		} else {
			prevV.count -= item.count
		}
		return true
	})
	if e != nil {
		return nil, fmt.Errorf("clean up after aggregation: %w", e)
	}
	return bt, nil
}

func (a *Aggregator) updateArch(bt *btree.BTree, fType FileType, blockNum32 uint32) {
	arch := a.arches[fType]
	h := a.archHasher
	n := uint64(len(arch))
	if n == 0 {
		return
	}
	bt.Ascend(func(i btree.Item) bool {
		item := i.(*AggregateItem)
		if item.count == 0 {
			return true
		}
		h.Reset()
		h.Write(item.k) //nolint:errcheck
		p, _ := h.Sum128()
		p = p % n
		v := atomic.LoadUint32(&arch[p])
		if v < blockNum32 {
			//fmt.Printf("Updated %s arch [%x]=%d %d\n", fType.String(), item.k, p, blockNum32)
			atomic.StoreUint32(&arch[p], blockNum32)
		}
		return true
	})
}

type AggregateItem struct {
	k, v  []byte
	count uint32
}

func (i *AggregateItem) Less(than btree.Item) bool {
	return bytes.Compare(i.k, than.(*AggregateItem).k) < 0
}

func (c *Changes) produceChangeSets(blockFrom, blockTo uint64, historyType, bitmapType FileType) (*compress.Decompressor, *recsplit.Index, *compress.Decompressor, *recsplit.Index, error) {
	chsetDatPath := filepath.Join(c.dir, fmt.Sprintf("%s.%d-%d.dat", historyType.String(), blockFrom, blockTo))
	chsetIdxPath := filepath.Join(c.dir, fmt.Sprintf("%s.%d-%d.idx", historyType.String(), blockFrom, blockTo))
	bitmapDatPath := filepath.Join(c.dir, fmt.Sprintf("%s.%d-%d.dat", bitmapType.String(), blockFrom, blockTo))
	bitmapIdxPath := filepath.Join(c.dir, fmt.Sprintf("%s.%d-%d.idx", bitmapType.String(), blockFrom, blockTo))
	var blockSuffix [8]byte
	binary.BigEndian.PutUint64(blockSuffix[:], blockTo)
	bitmaps := map[string]*roaring64.Bitmap{}
	comp, err := compress.NewCompressor(context.Background(), AggregatorPrefix, chsetDatPath, c.dir, compress.MinPatternScore, 1)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets NewCompressor: %w", err)
	}
	defer func() {
		if comp != nil {
			comp.Close()
		}
	}()
	var totalRecords int
	var b bool
	var e error
	var txNum uint64
	var key, before, after []byte
	if err = c.rewind(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets rewind: %w", err)
	}
	var txKey = make([]byte, 8, 60)
	for b, txNum, e = c.prevTx(); b && e == nil; b, txNum, e = c.prevTx() {
		binary.BigEndian.PutUint64(txKey[:8], txNum)
		for key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]); b && e == nil; key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]) {
			totalRecords++
			txKey = append(txKey[:8], key...)
			// In the inital files and most merged file, the txKey is added to the file, but it gets removed in the final merge
			if err = comp.AddWord(txKey); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("produceChangeSets AddWord key: %w", err)
			}
			if err = comp.AddWord(before); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("produceChangeSets AddWord before: %w", err)
			}
			//if historyType == AccountHistory {
			//	fmt.Printf("produce %s.%d-%d [%x]=>[%x]\n", historyType.String(), blockFrom, blockTo, txKey, before)
			//}
			var bitmap *roaring64.Bitmap
			var ok bool
			if bitmap, ok = bitmaps[string(key)]; !ok {
				bitmap = roaring64.New()
				bitmaps[string(key)] = bitmap
			}
			bitmap.Add(txNum)
		}
		if e != nil {
			return nil, nil, nil, nil, fmt.Errorf("produceChangeSets nextTriple: %w", e)
		}
	}
	if e != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets prevTx: %w", e)
	}
	if err = comp.Compress(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets Compress: %w", err)
	}
	comp.Close()
	comp = nil
	var d *compress.Decompressor
	var index *recsplit.Index
	if d, err = compress.NewDecompressor(chsetDatPath); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets changeset decompressor: %w", err)
	}
	if index, err = buildIndex(d, chsetIdxPath, c.dir, totalRecords); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets changeset buildIndex: %w", err)
	}
	// Create bitmap files
	bitmapC, err := compress.NewCompressor(context.Background(), AggregatorPrefix, bitmapDatPath, c.dir, compress.MinPatternScore, 1)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets bitmap NewCompressor: %w", err)
	}
	defer func() {
		if bitmapC != nil {
			bitmapC.Close()
		}
	}()
	idxKeys := make([]string, len(bitmaps))
	i := 0
	var buf []byte
	for key := range bitmaps {
		idxKeys[i] = key
		i++
	}
	sort.Strings(idxKeys)
	for _, key := range idxKeys {
		if err = bitmapC.AddWord([]byte(key)); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("produceChangeSets bitmap add key: %w", err)
		}
		bitmap := bitmaps[key]
		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			v := it.Next()
			ef.AddOffset(v)
		}
		ef.Build()
		buf = ef.AppendBytes(buf[:0])
		if err = bitmapC.AddUncompressedWord(buf); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("produceChangeSets bitmap add val: %w", err)
		}
	}
	if err = bitmapC.Compress(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets bitmap Compress: %w", err)
	}
	bitmapC.Close()
	bitmapC = nil
	bitmapD, err := compress.NewDecompressor(bitmapDatPath)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets bitmap decompressor: %w", err)
	}

	bitmapI, err := buildIndex(bitmapD, bitmapIdxPath, c.dir, len(idxKeys))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("produceChangeSets bitmap buildIndex: %w", err)
	}
	return d, index, bitmapD, bitmapI, nil
}

// aggregateToBtree iterates over all available changes in the change files covered by this instance `c`
// (there are 3 of them, one for "keys", one for values "before" every change, and one for values "after" every change)
// and create a B-tree where each key is only represented once, with the value corresponding to the "after" value
// of the latest change.
func (c *Changes) aggregateToBtree(bt *btree.BTree, prefixLen int, commitments bool) error {
	var b bool
	var e error
	var key, before, after []byte
	var ai AggregateItem
	var prefix []byte
	// Note that the following loop iterates over transactions backwards, therefore it does not replace entries in the B-tree,
	// but instead just updates their "change count" and the first byte of the value (insertion vs update flag)
	for b, _, e = c.prevTx(); b && e == nil; b, _, e = c.prevTx() {
		// Within each transaction, keys are unique, but they can appear in any order
		for key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]); b && e == nil; key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]) {
			if prefixLen > 0 && !bytes.Equal(prefix, key[:prefixLen]) {
				prefix = common.Copy(key[:prefixLen])
				item := &AggregateItem{k: prefix, count: 0}
				bt.ReplaceOrInsert(item)
			}
			ai.k = key
			i := bt.Get(&ai)
			if i == nil {
				item := &AggregateItem{k: common.Copy(key), v: common.Copy(after), count: 1}
				bt.ReplaceOrInsert(item)
			} else {
				item := i.(*AggregateItem)
				if commitments {
					var err error
					var mergedVal []byte
					if mergedVal, err = commitment.MergeBranches(after, item.v, nil); err != nil {
						return fmt.Errorf("merge branches: %w", err)
					}
					//fmt.Printf("aggregateToBtree prefix [%x], [%x]+[%x]=>[%x]\n", commitment.CompactToHex(key), after, item.v, mergedVal)
					item.v = mergedVal
				}
				item.count++
			}
		}
		if e != nil {
			return fmt.Errorf("aggregateToBtree nextTriple: %w", e)
		}
	}
	if e != nil {
		return fmt.Errorf("aggregateToBtree prevTx: %w", e)
	}
	return nil
}

const AggregatorPrefix = "aggregator"

func btreeToFile(bt *btree.BTree, datPath string, tmpdir string, trace bool, workers int) (int, error) {
	comp, err := compress.NewCompressor(context.Background(), AggregatorPrefix, datPath, tmpdir, compress.MinPatternScore, workers)
	if err != nil {
		return 0, err
	}
	defer comp.Close()
	comp.SetTrace(trace)
	count := 0
	bt.Ascend(func(i btree.Item) bool {
		item := i.(*AggregateItem)
		if err = comp.AddWord(item.k); err != nil {
			return false
		}
		count++ // Only counting keys, not values
		if err = comp.AddWord(item.v); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if err = comp.Compress(); err != nil {
		return 0, err
	}
	return count, nil
}

type ChangesItem struct {
	endBlock   uint64
	startBlock uint64
	fileCount  int
}

func (i *ChangesItem) Less(than btree.Item) bool {
	if i.endBlock == than.(*ChangesItem).endBlock {
		// Larger intevals will come last
		return i.startBlock > than.(*ChangesItem).startBlock
	}
	return i.endBlock < than.(*ChangesItem).endBlock
}

type byEndBlockItem struct {
	startBlock   uint64
	endBlock     uint64
	decompressor *compress.Decompressor
	getter       *compress.Getter // reader for the decompressor
	getterMerge  *compress.Getter // reader for the decomporessor used in the background merge thread
	index        *recsplit.Index
	indexReader  *recsplit.IndexReader // reader for the index
	readerMerge  *recsplit.IndexReader // index reader for the background merge thread
	tree         *btree.BTree          // Substitute for decompressor+index combination
}

func (i *byEndBlockItem) Less(than btree.Item) bool {
	if i.endBlock == than.(*byEndBlockItem).endBlock {
		return i.startBlock > than.(*byEndBlockItem).startBlock
	}
	return i.endBlock < than.(*byEndBlockItem).endBlock
}

func (a *Aggregator) scanStateFiles(files []fs.DirEntry) {
	typeStrings := make([]string, NumberOfTypes)
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		typeStrings[fType] = fType.String()
	}
	re := regexp.MustCompile("(" + strings.Join(typeStrings, "|") + ").([0-9]+)-([0-9]+).(dat|idx)")
	var err error
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 5 {
			if len(subs) != 0 {
				log.Warn("File ignored by aggregator, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startBlock, endBlock uint64
		if startBlock, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by aggregator, parsing startBlock", "error", err, "name", name)
			continue
		}
		if endBlock, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			log.Warn("File ignored by aggregator, parsing endBlock", "error", err, "name", name)
			continue
		}
		if startBlock > endBlock {
			log.Warn("File ignored by aggregator, startBlock > endBlock", "name", name)
			continue
		}
		fType, ok := ParseFileType(subs[1])
		if !ok {
			log.Warn("File ignored by aggregator, type unknown", "type", subs[1])
		}
		var item = &byEndBlockItem{startBlock: startBlock, endBlock: endBlock}
		var foundI *byEndBlockItem
		a.files[fType].AscendGreaterOrEqual(&byEndBlockItem{startBlock: endBlock, endBlock: endBlock}, func(i btree.Item) bool {
			it := i.(*byEndBlockItem)
			if it.endBlock == endBlock {
				foundI = it
			}
			return false
		})
		if foundI == nil || foundI.startBlock > startBlock {
			log.Info("Load state file", "name", name, "type", fType.String(), "startBlock", startBlock, "endBlock", endBlock)
			a.files[fType].ReplaceOrInsert(item)
		}
	}
}

func NewAggregator(diffDir string, unwindLimit uint64, aggregationStep uint64, changesets, commitments bool, minArch uint64) (*Aggregator, error) {
	a := &Aggregator{
		diffDir:         diffDir,
		unwindLimit:     unwindLimit,
		aggregationStep: aggregationStep,
		tracedKeys:      map[string]struct{}{},
		keccak:          sha3.NewLegacyKeccak256(),
		hph:             commitment.NewHexPatriciaHashed(length.Addr, nil, nil, nil, nil, nil),
		aggChannel:      make(chan *AggregationTask),
		aggBackCh:       make(chan struct{}),
		aggError:        make(chan error, 1),
		mergeChannel:    make(chan struct{}, 1),
		mergeError:      make(chan error, 1),
		historyChannel:  make(chan struct{}, 1),
		historyError:    make(chan error, 1),
		changesets:      changesets,
		commitments:     commitments,
		archHasher:      murmur3.New128WithSeed(0), // TODO: Randomise salt
	}
	for fType := FirstType; fType < NumberOfTypes; fType++ {
		a.files[fType] = btree.New(32)
	}
	for fType := FirstType; fType < NumberOfStateTypes; fType++ {
		a.trees[fType] = btree.New(32)
	}
	var closeStateFiles = true // It will be set to false in case of success at the end of the function
	defer func() {
		// Clean up all decompressor and indices upon error
		if closeStateFiles {
			a.Close()
		}
	}()
	// Scan the diff directory and create the mapping of end blocks to files
	files, err := os.ReadDir(diffDir)
	if err != nil {
		return nil, err
	}
	a.scanStateFiles(files)
	// Check for overlaps and holes
	for fType := FirstType; fType < NumberOfTypes; fType++ {
		if err := checkOverlaps(fType.String(), a.files[fType]); err != nil {
			return nil, err
		}
	}
	// Open decompressor and index files for all items in state trees
	for fType := FirstType; fType < NumberOfTypes; fType++ {
		if err := a.openFiles(fType, minArch); err != nil {
			return nil, fmt.Errorf("opening %s state files: %w", fType.String(), err)
		}
	}
	a.changesBtree = btree.New(32)
	re := regexp.MustCompile(`(account|storage|code|commitment).(keys|before|after).([0-9]+)-([0-9]+).chg`)
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 5 {
			if len(subs) != 0 {
				log.Warn("File ignored by changes scan, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startBlock, endBlock uint64
		if startBlock, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			log.Warn("File ignored by changes scan, parsing startBlock", "error", err, "name", name)
			continue
		}
		if endBlock, err = strconv.ParseUint(subs[4], 10, 64); err != nil {
			log.Warn("File ignored by changes scan, parsing endBlock", "error", err, "name", name)
			continue
		}
		if startBlock > endBlock {
			log.Warn("File ignored by changes scan, startBlock > endBlock", "name", name)
			continue
		}
		if endBlock != startBlock+aggregationStep-1 {
			log.Warn("File ignored by changes scan, endBlock != startBlock+aggregationStep-1", "name", name)
			continue
		}
		var item = &ChangesItem{fileCount: 1, startBlock: startBlock, endBlock: endBlock}
		i := a.changesBtree.Get(item)
		if i == nil {
			a.changesBtree.ReplaceOrInsert(item)
		} else {
			item = i.(*ChangesItem)
			if item.startBlock == startBlock {
				item.fileCount++
			} else {
				return nil, fmt.Errorf("change files overlap [%d-%d] with [%d-%d]", item.startBlock, item.endBlock, startBlock, endBlock)
			}
		}
	}
	// Check for holes in change files
	minStart := uint64(math.MaxUint64)
	a.changesBtree.Descend(func(i btree.Item) bool {
		item := i.(*ChangesItem)
		if item.startBlock < minStart {
			if item.endBlock >= minStart {
				err = fmt.Errorf("overlap of change files [%d-%d] with %d", item.startBlock, item.endBlock, minStart)
				return false
			}
			if minStart != math.MaxUint64 && item.endBlock+1 != minStart {
				err = fmt.Errorf("whole in change files [%d-%d]", item.endBlock, minStart)
				return false
			}
			minStart = item.startBlock
		} else {
			err = fmt.Errorf("overlap of change files [%d-%d] with %d", item.startBlock, item.endBlock, minStart)
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	for fType := FirstType; fType < NumberOfStateTypes; fType++ {
		if err = checkOverlapWithMinStart(fType.String(), a.files[fType], minStart); err != nil {
			return nil, err
		}
	}
	if err = a.rebuildRecentState(); err != nil {
		return nil, fmt.Errorf("rebuilding recent state from change files: %w", err)
	}
	closeStateFiles = false
	a.aggWg.Add(1)
	go a.backgroundAggregation()
	a.mergeWg.Add(1)
	go a.backgroundMerge()
	if a.changesets {
		a.historyWg.Add(1)
		go a.backgroundHistoryMerge()
	}
	return a, nil
}

// rebuildRecentState reads change files and reconstructs the recent state
func (a *Aggregator) rebuildRecentState() error {
	t := time.Now()
	var err error
	a.changesBtree.Descend(func(i btree.Item) bool {
		item := i.(*ChangesItem)
		for fType := FirstType; fType < NumberOfStateTypes; fType++ {
			var changes Changes
			changes.Init(fType.String(), a.aggregationStep, a.diffDir, false /* beforeOn */)
			if changes.openFiles(item.startBlock, false /* write */); err != nil {
				return false
			}
			if err = changes.aggregateToBtree(a.trees[fType], 0, fType == Commitment); err != nil {
				return false
			}
			if err = changes.closeFiles(); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	log.Info("reconstructed recent state", "in", time.Since(t))
	return nil
}

type AggregationTask struct {
	changes   [NumberOfStateTypes]Changes
	bt        [NumberOfStateTypes]*btree.BTree
	blockFrom uint64
	blockTo   uint64
}

func (a *Aggregator) removeLocked(fType FileType, toRemove []*byEndBlockItem, item *byEndBlockItem) {
	a.fileLocks[fType].Lock()
	defer a.fileLocks[fType].Unlock()
	if len(toRemove) > 1 {
		for _, ag := range toRemove {
			a.files[fType].Delete(ag)
		}
		a.files[fType].ReplaceOrInsert(item)
	}
}

func (a *Aggregator) removeLockedState(
	accountsToRemove []*byEndBlockItem, accountsItem *byEndBlockItem,
	codeToRemove []*byEndBlockItem, codeItem *byEndBlockItem,
	storageToRemove []*byEndBlockItem, storageItem *byEndBlockItem,
	commitmentToRemove []*byEndBlockItem, commitmentItem *byEndBlockItem,
) {
	for fType := FirstType; fType < NumberOfStateTypes; fType++ {
		a.fileLocks[fType].Lock()
		defer a.fileLocks[fType].Unlock()
	}
	if len(accountsToRemove) > 1 {
		for _, ag := range accountsToRemove {
			a.files[Account].Delete(ag)
		}
		a.files[Account].ReplaceOrInsert(accountsItem)
	}
	if len(codeToRemove) > 1 {
		for _, ag := range codeToRemove {
			a.files[Code].Delete(ag)
		}
		a.files[Code].ReplaceOrInsert(codeItem)
	}
	if len(storageToRemove) > 1 {
		for _, ag := range storageToRemove {
			a.files[Storage].Delete(ag)
		}
		a.files[Storage].ReplaceOrInsert(storageItem)
	}
	if len(commitmentToRemove) > 1 {
		for _, ag := range commitmentToRemove {
			a.files[Commitment].Delete(ag)
		}
		a.files[Commitment].ReplaceOrInsert(commitmentItem)
	}
}

func removeFiles(fType FileType, diffDir string, toRemove []*byEndBlockItem) error {
	// Close all the memory maps etc
	for _, ag := range toRemove {
		if err := ag.index.Close(); err != nil {
			return fmt.Errorf("close index: %w", err)
		}
		if err := ag.decompressor.Close(); err != nil {
			return fmt.Errorf("close decompressor: %w", err)
		}
	}
	// Delete files
	// TODO: in a non-test version, this is delayed to allow other participants to roll over to the next file
	for _, ag := range toRemove {
		if err := os.Remove(path.Join(diffDir, fmt.Sprintf("%s.%d-%d.dat", fType.String(), ag.startBlock, ag.endBlock))); err != nil {
			return fmt.Errorf("remove decompressor file %s.%d-%d.dat: %w", fType.String(), ag.startBlock, ag.endBlock, err)
		}
		if err := os.Remove(path.Join(diffDir, fmt.Sprintf("%s.%d-%d.idx", fType.String(), ag.startBlock, ag.endBlock))); err != nil {
			return fmt.Errorf("remove index file %s.%d-%d.idx: %w", fType.String(), ag.startBlock, ag.endBlock, err)
		}
	}
	return nil
}

// backgroundAggregation is the functin that runs in a background go-routine and performs creation of initial state files
// allowing the main goroutine to proceed
func (a *Aggregator) backgroundAggregation() {
	defer a.aggWg.Done()
	for aggTask := range a.aggChannel {
		typesLimit := Commitment
		if a.commitments {
			typesLimit = AccountHistory
		}
		for fType := FirstType; fType < typesLimit; fType++ {
			if fType < NumberOfStateTypes {
				a.updateArch(aggTask.bt[fType], fType, uint32(aggTask.blockTo))
			}
			a.addLocked(fType, &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo, tree: aggTask.bt[fType]})
		}
		a.aggBackCh <- struct{}{}
		if a.changesets {
			if historyD, historyI, bitmapD, bitmapI, err := aggTask.changes[Account].produceChangeSets(aggTask.blockFrom, aggTask.blockTo, AccountHistory, AccountBitmap); err == nil {
				var historyItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
				historyItem.decompressor = historyD
				historyItem.index = historyI
				historyItem.getter = historyItem.decompressor.MakeGetter()
				historyItem.getterMerge = historyItem.decompressor.MakeGetter()
				historyItem.indexReader = recsplit.NewIndexReader(historyItem.index)
				historyItem.readerMerge = recsplit.NewIndexReader(historyItem.index)
				a.addLocked(AccountHistory, historyItem)
				var bitmapItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
				bitmapItem.decompressor = bitmapD
				bitmapItem.index = bitmapI
				bitmapItem.getter = bitmapItem.decompressor.MakeGetter()
				bitmapItem.getterMerge = bitmapItem.decompressor.MakeGetter()
				bitmapItem.indexReader = recsplit.NewIndexReader(bitmapItem.index)
				bitmapItem.readerMerge = recsplit.NewIndexReader(bitmapItem.index)
				a.addLocked(AccountBitmap, bitmapItem)
			} else {
				a.aggError <- fmt.Errorf("produceChangeSets %s: %w", Account.String(), err)
				return
			}
			if historyD, historyI, bitmapD, bitmapI, err := aggTask.changes[Storage].produceChangeSets(aggTask.blockFrom, aggTask.blockTo, StorageHistory, StorageBitmap); err == nil {
				var historyItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
				historyItem.decompressor = historyD
				historyItem.index = historyI
				historyItem.getter = historyItem.decompressor.MakeGetter()
				historyItem.getterMerge = historyItem.decompressor.MakeGetter()
				historyItem.indexReader = recsplit.NewIndexReader(historyItem.index)
				historyItem.readerMerge = recsplit.NewIndexReader(historyItem.index)
				a.addLocked(StorageHistory, historyItem)
				var bitmapItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
				bitmapItem.decompressor = bitmapD
				bitmapItem.index = bitmapI
				bitmapItem.getter = bitmapItem.decompressor.MakeGetter()
				bitmapItem.getterMerge = bitmapItem.decompressor.MakeGetter()
				bitmapItem.indexReader = recsplit.NewIndexReader(bitmapItem.index)
				bitmapItem.readerMerge = recsplit.NewIndexReader(bitmapItem.index)
				a.addLocked(StorageBitmap, bitmapItem)
			} else {
				a.aggError <- fmt.Errorf("produceChangeSets %s: %w", Storage.String(), err)
				return
			}
			if historyD, historyI, bitmapD, bitmapI, err := aggTask.changes[Code].produceChangeSets(aggTask.blockFrom, aggTask.blockTo, CodeHistory, CodeBitmap); err == nil {
				var historyItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
				historyItem.decompressor = historyD
				historyItem.index = historyI
				historyItem.getter = historyItem.decompressor.MakeGetter()
				historyItem.getterMerge = historyItem.decompressor.MakeGetter()
				historyItem.indexReader = recsplit.NewIndexReader(historyItem.index)
				historyItem.readerMerge = recsplit.NewIndexReader(historyItem.index)
				a.addLocked(CodeHistory, historyItem)
				var bitmapItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
				bitmapItem.decompressor = bitmapD
				bitmapItem.index = bitmapI
				bitmapItem.getter = bitmapItem.decompressor.MakeGetter()
				bitmapItem.getterMerge = bitmapItem.decompressor.MakeGetter()
				bitmapItem.indexReader = recsplit.NewIndexReader(bitmapItem.index)
				bitmapItem.readerMerge = recsplit.NewIndexReader(bitmapItem.index)
				a.addLocked(CodeBitmap, bitmapItem)
			} else {
				a.aggError <- fmt.Errorf("produceChangeSets %s: %w", Code.String(), err)
				return
			}
		}
		for fType := FirstType; fType < typesLimit; fType++ {
			var err error
			if err = aggTask.changes[fType].closeFiles(); err != nil {
				a.aggError <- fmt.Errorf("close %sChanges: %w", fType.String(), err)
				return
			}
			var item = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
			if item.decompressor, item.index, err = createDatAndIndex(fType.String(), a.diffDir, aggTask.bt[fType], aggTask.blockFrom, aggTask.blockTo); err != nil {
				a.aggError <- fmt.Errorf("createDatAndIndex %s: %w", fType.String(), err)
				return
			}
			item.getter = item.decompressor.MakeGetter()
			item.getterMerge = item.decompressor.MakeGetter()
			item.indexReader = recsplit.NewIndexReader(item.index)
			item.readerMerge = recsplit.NewIndexReader(item.index)
			if err = aggTask.changes[fType].deleteFiles(); err != nil {
				a.aggError <- fmt.Errorf("delete %sChanges: %w", fType.String(), err)
				return
			}
			a.addLocked(fType, item)
		}
		// At this point, 3 new state files (containing latest changes) has been created for accounts, code, and storage
		// Corresponding items has been added to the registy of state files, and B-tree are not necessary anymore, change files can be removed
		// What follows can be performed by the 2nd background goroutine
		select {
		case a.mergeChannel <- struct{}{}:
		default:
		}
		select {
		case a.historyChannel <- struct{}{}:
		default:
		}
	}
}

type CommitmentValTransform struct {
	pre  [NumberOfAccountStorageTypes][]*byEndBlockItem // List of state files before the merge
	post [NumberOfAccountStorageTypes][]*byEndBlockItem // List of state files aftee the merge
}

func decodeU64(from []byte) uint64 {
	var i uint64
	for _, b := range from {
		i = (i << 8) | uint64(b)
	}
	return i
}

func encodeU64(i uint64, to []byte) []byte {
	// writes i to b in big endian byte order, using the least number of bytes needed to represent i.
	switch {
	case i < (1 << 8):
		return append(to, byte(i))
	case i < (1 << 16):
		return append(to, byte(i>>8), byte(i))
	case i < (1 << 24):
		return append(to, byte(i>>16), byte(i>>8), byte(i))
	case i < (1 << 32):
		return append(to, byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	case i < (1 << 40):
		return append(to, byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	case i < (1 << 48):
		return append(to, byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	case i < (1 << 56):
		return append(to, byte(i>>48), byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	default:
		return append(to, byte(i>>56), byte(i>>48), byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	}
}

// commitmentValTransform parses the value of of the commitment record to extract references
// to accounts and storage items, then looks them up in the new, merged files, and replaces them with
// the updated references
func (cvt *CommitmentValTransform) commitmentValTransform(val []byte, transValBuf []byte) ([]byte, error) {
	if len(val) == 0 {
		return transValBuf, nil
	}
	accountPlainKeys, storagePlainKeys, err := commitment.ExtractPlainKeys(val)
	if err != nil {
		return nil, err
	}
	var transAccountPks [][]byte
	var transStoragePks [][]byte
	var apkBuf, spkBuf []byte
	for _, accountPlainKey := range accountPlainKeys {
		if len(accountPlainKey) == length.Addr {
			// Non-optimised key originating from a database record
			apkBuf = append(apkBuf[:0], accountPlainKey...)
		} else {
			// Optimised key referencing a state file record (file number and offset within the file)
			fileI := int(accountPlainKey[0])
			offset := decodeU64(accountPlainKey[1:])
			g := cvt.pre[Account][fileI].getterMerge
			g.Reset(offset)
			apkBuf, _ = g.Next(apkBuf[:0])
			//fmt.Printf("replacing account [%x] from [%x]\n", apkBuf, accountPlainKey)
		}
		// Look up apkBuf in the post account files
		for j := len(cvt.post[Account]); j > 0; j-- {
			item := cvt.post[Account][j-1]
			if item.index.Empty() {
				continue
			}
			offset := item.readerMerge.Lookup(apkBuf)
			g := item.getterMerge
			g.Reset(offset)
			if g.HasNext() {
				if keyMatch, _ := g.Match(apkBuf); keyMatch {
					accountPlainKey = encodeU64(offset, []byte{byte(j - 1)})
					//fmt.Printf("replaced account [%x]=>[%x] for file [%d-%d]\n", apkBuf, accountPlainKey, item.startBlock, item.endBlock)
					break
				}
			}
		}
		transAccountPks = append(transAccountPks, accountPlainKey)
	}
	for _, storagePlainKey := range storagePlainKeys {
		if len(storagePlainKey) == length.Addr+length.Hash {
			// Non-optimised key originating from a database record
			spkBuf = append(spkBuf[:0], storagePlainKey...)
		} else {
			// Optimised key referencing a state file record (file number and offset within the file)
			fileI := int(storagePlainKey[0])
			offset := decodeU64(storagePlainKey[1:])
			g := cvt.pre[Storage][fileI].getterMerge
			g.Reset(offset)
			spkBuf, _ = g.Next(spkBuf[:0])
			//fmt.Printf("replacing storage [%x] from [%x]\n", spkBuf, storagePlainKey)
		}
		// Lookup spkBuf in the post storage files
		for j := len(cvt.post[Storage]); j > 0; j-- {
			item := cvt.post[Storage][j-1]
			if item.index.Empty() {
				continue
			}
			offset := item.readerMerge.Lookup(spkBuf)
			g := item.getterMerge
			g.Reset(offset)
			if g.HasNext() {
				if keyMatch, _ := g.Match(spkBuf); keyMatch {
					storagePlainKey = encodeU64(offset, []byte{byte(j - 1)})
					//fmt.Printf("replaced storage [%x]=>[%x]\n", spkBuf, storagePlainKey)
					break
				}
			}
		}
		transStoragePks = append(transStoragePks, storagePlainKey)
	}
	if transValBuf, err = commitment.ReplacePlainKeys(val, transAccountPks, transStoragePks, transValBuf); err != nil {
		return nil, err
	}
	return transValBuf, nil
}

func (a *Aggregator) backgroundMerge() {
	defer a.mergeWg.Done()
	for range a.mergeChannel {
		t := time.Now()
		var err error
		var cvt CommitmentValTransform
		var toRemove [NumberOfStateTypes][]*byEndBlockItem
		var newItems [NumberOfStateTypes]*byEndBlockItem
		var blockFrom, blockTo uint64
		lastType := Code
		typesLimit := Commitment
		if a.commitments {
			lastType = Commitment
			typesLimit = AccountHistory
		}
		// Lock the set of commitment (or code if commitments are off) files - those are the smallest, because account, storage and code files may be added by the aggregation thread first
		toRemove[lastType], _, _, blockFrom, blockTo = a.findLargestMerge(lastType, uint64(math.MaxUint64) /* maxBlockTo */, uint64(math.MaxUint64) /* maxSpan */)

		for fType := FirstType; fType < typesLimit; fType++ {
			var pre, post []*byEndBlockItem
			var from, to uint64
			if fType == lastType {
				from = blockFrom
				to = blockTo
			} else {
				toRemove[fType], pre, post, from, to = a.findLargestMerge(fType, blockTo, uint64(math.MaxUint64) /* maxSpan */)
				if from != blockFrom {
					a.mergeError <- fmt.Errorf("%sFrom %d != blockFrom %d", fType.String(), from, blockFrom)
					return
				}
				if to != blockTo {
					a.mergeError <- fmt.Errorf("%sTo %d != blockTo %d", fType.String(), to, blockTo)
					return
				}
			}
			if len(toRemove[fType]) > 1 {
				var valTransform func([]byte, []byte) ([]byte, error)
				var mergeFunc func([]byte, []byte, []byte) ([]byte, error)
				if fType == Commitment {
					valTransform = cvt.commitmentValTransform
					mergeFunc = mergeCommitments
				} else {
					mergeFunc = mergeReplace
				}
				var prefixLen int
				if fType == Storage {
					prefixLen = length.Addr
				}
				if newItems[fType], err = a.computeAggregation(fType, toRemove[fType], from, to, valTransform, mergeFunc, true /* valCompressed */, true /* withIndex */, prefixLen); err != nil {
					a.mergeError <- fmt.Errorf("computeAggreation %s: %w", fType.String(), err)
					return
				}
				post = append(post, newItems[fType])
			}
			if fType < NumberOfAccountStorageTypes {
				cvt.pre[fType] = pre
				cvt.post[fType] = post
			}
		}
		// Switch aggregator to new state files, close and remove old files
		a.removeLockedState(toRemove[Account], newItems[Account], toRemove[Code], newItems[Code], toRemove[Storage], newItems[Storage], toRemove[Commitment], newItems[Commitment])
		removed := 0
		for fType := FirstType; fType < typesLimit; fType++ {
			if len(toRemove[fType]) > 1 {
				removeFiles(fType, a.diffDir, toRemove[fType])
				removed += len(toRemove[fType]) - 1
			}
		}
		mergeTime := time.Since(t)
		if mergeTime > time.Minute {
			log.Info("Long merge", "from", blockFrom, "to", blockTo, "files", removed, "time", time.Since(t))
		}
	}
}

func (a *Aggregator) reduceHistoryFiles(fType FileType, item *byEndBlockItem) error {
	datTmpPath := filepath.Join(a.diffDir, fmt.Sprintf("%s.%d-%d.dat.tmp", fType.String(), item.startBlock, item.endBlock))
	datPath := filepath.Join(a.diffDir, fmt.Sprintf("%s.%d-%d.dat", fType.String(), item.startBlock, item.endBlock))
	idxPath := filepath.Join(a.diffDir, fmt.Sprintf("%s.%d-%d.idx", fType.String(), item.startBlock, item.endBlock))
	comp, err := compress.NewCompressor(context.Background(), AggregatorPrefix, datTmpPath, a.diffDir, compress.MinPatternScore, 1)
	if err != nil {
		return fmt.Errorf("reduceHistoryFiles create compressor %s: %w", datPath, err)
	}
	defer comp.Close()
	g := item.getter
	var val []byte
	var count int
	g.Reset(0)
	var key []byte
	for g.HasNext() {
		g.Skip() // Skip key on on the first pass
		val, _ = g.Next(val[:0])
		//fmt.Printf("reduce1 [%s.%d-%d] [%x]=>[%x]\n", fType.String(), item.startBlock, item.endBlock, key, val)
		if err = comp.AddWord(val); err != nil {
			return fmt.Errorf("reduceHistoryFiles AddWord: %w", err)
		}
		count++
	}
	if err = comp.Compress(); err != nil {
		return fmt.Errorf("reduceHistoryFiles compress: %w", err)
	}
	var d *compress.Decompressor
	if d, err = compress.NewDecompressor(datTmpPath); err != nil {
		return fmt.Errorf("reduceHistoryFiles create decompressor: %w", err)
	}
	var rs *recsplit.RecSplit
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     a.diffDir,
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		IndexFile: idxPath,
	}); err != nil {
		return fmt.Errorf("reduceHistoryFiles NewRecSplit: %w", err)
	}
	g1 := d.MakeGetter()
	for {
		g.Reset(0)
		g1.Reset(0)
		var lastOffset uint64
		for g.HasNext() {
			key, _ = g.Next(key[:0])
			g.Skip() // Skip value
			_, pos := g1.Next(nil)
			//fmt.Printf("reduce2 [%s.%d-%d] [%x]==>%d\n", fType.String(), item.startBlock, item.endBlock, key, lastOffset)
			if err = rs.AddKey(key, lastOffset); err != nil {
				return fmt.Errorf("reduceHistoryFiles %p AddKey: %w", rs, err)
			}
			lastOffset = pos
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building reduceHistoryFiles. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return fmt.Errorf("reduceHistoryFiles Build: %w", err)
			}
		} else {
			break
		}
	}
	if err = item.decompressor.Close(); err != nil {
		return fmt.Errorf("reduceHistoryFiles close decompressor: %w", err)
	}
	if err = os.Remove(datPath); err != nil {
		return fmt.Errorf("reduceHistoryFiles remove: %w", err)
	}
	if err = os.Rename(datTmpPath, datPath); err != nil {
		return fmt.Errorf("reduceHistoryFiles rename: %w", err)
	}
	if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
		return fmt.Errorf("reduceHistoryFiles create new decompressor: %w", err)
	}
	item.getter = item.decompressor.MakeGetter()
	item.getterMerge = item.decompressor.MakeGetter()
	if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
		return fmt.Errorf("reduceHistoryFiles open index: %w", err)
	}
	item.indexReader = recsplit.NewIndexReader(item.index)
	item.readerMerge = recsplit.NewIndexReader(item.index)
	return nil
}

func mergeReplace(preval, val, buf []byte) ([]byte, error) {
	return append(buf, val...), nil
}

func mergeBitmaps(preval, val, buf []byte) ([]byte, error) {
	preef, _ := eliasfano32.ReadEliasFano(preval)
	ef, _ := eliasfano32.ReadEliasFano(val)
	//fmt.Printf("mergeBitmaps [%x] (count=%d,max=%d) + [%x] (count=%d,max=%d)\n", preval, preef.Count(), preef.Max(), val, ef.Count(), ef.Max())
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

func mergeCommitments(preval, val, buf []byte) ([]byte, error) {
	return commitment.MergeBranches(preval, val, buf)
}

func (a *Aggregator) backgroundHistoryMerge() {
	defer a.historyWg.Done()
	for range a.historyChannel {
		t := time.Now()
		var err error
		var toRemove [NumberOfTypes][]*byEndBlockItem
		var newItems [NumberOfTypes]*byEndBlockItem
		var blockFrom, blockTo uint64
		// Lock the set of commitment files - those are the smallest, because account, storage and code files may be added by the aggregation thread first
		toRemove[CodeBitmap], _, _, blockFrom, blockTo = a.findLargestMerge(CodeBitmap, uint64(math.MaxUint64) /* maxBlockTo */, 500_000 /* maxSpan */)

		finalMerge := blockTo-blockFrom+1 == 500_000
		for fType := AccountHistory; fType < NumberOfTypes; fType++ {
			var from, to uint64
			if fType == CodeBitmap {
				from = blockFrom
				to = blockTo
			} else {
				toRemove[fType], _, _, from, to = a.findLargestMerge(fType, blockTo, 500_000 /* maxSpan */)
				if from != blockFrom {
					a.historyError <- fmt.Errorf("%sFrom %d != blockFrom %d", fType.String(), from, blockFrom)
					return
				}
				if to != blockTo {
					a.historyError <- fmt.Errorf("%sTo %d != blockTo %d", fType.String(), to, blockTo)
					return
				}
			}
			if len(toRemove[fType]) > 1 {
				isBitmap := fType == AccountBitmap || fType == StorageBitmap || fType == CodeBitmap
				var mergeFunc func([]byte, []byte, []byte) ([]byte, error)
				if isBitmap {
					mergeFunc = mergeBitmaps
				} else if fType == Commitment {
					mergeFunc = mergeCommitments
				} else {
					mergeFunc = mergeReplace
				}
				if newItems[fType], err = a.computeAggregation(fType, toRemove[fType], from, to, nil /* valTransform */, mergeFunc,
					!isBitmap /* valCompressed */, !finalMerge || isBitmap /* withIndex */, 0 /* prefixLen */); err != nil {
					a.historyError <- fmt.Errorf("computeAggreation %s: %w", fType.String(), err)
					return
				}
			}
		}
		if finalMerge {
			// Special aggregation for blockTo - blockFrom + 1 == 500_000
			// Remove keys from the .dat files assuming that they will only be used after querying the bitmap index
			// and therefore, there is no situation where non-existent key is queried.
			if err = a.reduceHistoryFiles(AccountHistory, newItems[AccountHistory]); err != nil {
				a.historyError <- fmt.Errorf("reduceHistoryFiles %s: %w", AccountHistory.String(), err)
				return
			}
			if err = a.reduceHistoryFiles(StorageHistory, newItems[StorageHistory]); err != nil {
				a.historyError <- fmt.Errorf("reduceHistoryFiles %s: %w", StorageHistory.String(), err)
				return
			}
			if err = a.reduceHistoryFiles(CodeHistory, newItems[CodeHistory]); err != nil {
				a.historyError <- fmt.Errorf("reduceHistoryFiles %s: %w", CodeHistory.String(), err)
				return
			}
		}
		for fType := AccountHistory; fType < NumberOfTypes; fType++ {
			a.removeLocked(fType, toRemove[fType], newItems[fType])
		}
		removed := 0
		for fType := AccountHistory; fType < NumberOfTypes; fType++ {
			if len(toRemove[fType]) > 1 {
				removeFiles(fType, a.diffDir, toRemove[fType])
				removed += len(toRemove[fType]) - 1
			}
		}
		mergeTime := time.Since(t)
		if mergeTime > time.Minute {
			log.Info("Long history merge", "from", blockFrom, "to", blockTo, "files", removed, "time", time.Since(t))
		}
	}
}

// checkOverlaps does not lock tree, because it is only called from the constructor of aggregator
func checkOverlaps(treeName string, tree *btree.BTree) error {
	var minStart uint64 = math.MaxUint64
	var err error
	tree.Descend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.startBlock < minStart {
			if item.endBlock >= minStart {
				err = fmt.Errorf("overlap of %s state files [%d-%d] with %d", treeName, item.startBlock, item.endBlock, minStart)
				return false
			}
			if minStart != math.MaxUint64 && item.endBlock+1 != minStart {
				err = fmt.Errorf("hole in %s state files [%d-%d]", treeName, item.endBlock, minStart)
				return false
			}
			minStart = item.startBlock
		}
		return true
	})
	return err
}

func (a *Aggregator) openFiles(fType FileType, minArch uint64) error {
	var err error
	var totalKeys uint64
	a.files[fType].Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor, err = compress.NewDecompressor(path.Join(a.diffDir, fmt.Sprintf("%s.%d-%d.dat", fType.String(), item.startBlock, item.endBlock))); err != nil {
			return false
		}
		if item.index, err = recsplit.OpenIndex(path.Join(a.diffDir, fmt.Sprintf("%s.%d-%d.idx", fType.String(), item.startBlock, item.endBlock))); err != nil {
			return false
		}
		totalKeys += item.index.KeyCount()
		item.getter = item.decompressor.MakeGetter()
		item.getterMerge = item.decompressor.MakeGetter()
		item.indexReader = recsplit.NewIndexReader(item.index)
		item.readerMerge = recsplit.NewIndexReader(item.index)
		return true
	})
	if fType >= NumberOfStateTypes {
		return nil
	}
	log.Info("Creating arch...", "type", fType.String(), "total keys in all state files", totalKeys)
	// Allocate arch of double of total keys
	n := totalKeys * 2
	if n < minArch {
		n = minArch
	}
	a.arches[fType] = make([]uint32, n)
	arch := a.arches[fType]
	var key []byte
	h := a.archHasher
	collisions := 0
	a.files[fType].Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		g := item.getter
		g.Reset(0)
		blockNum := uint32(item.endBlock)
		for g.HasNext() {
			key, _ = g.Next(key[:0])
			h.Reset()
			h.Write(key) //nolint:errcheck
			p, _ := h.Sum128()
			p = p % n
			if arch[p] != 0 {
				collisions++
			}
			arch[p] = blockNum
			g.Skip()
		}
		return true
	})
	log.Info("Created arch", "type", fType.String(), "collisions", collisions)
	return err
}

func (a *Aggregator) closeFiles(fType FileType) {
	a.fileLocks[fType].Lock()
	defer a.fileLocks[fType].Unlock()
	a.files[fType].Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		if item.index != nil {
			item.index.Close()
		}
		return true
	})
}

func (a *Aggregator) Close() {
	close(a.aggChannel)
	a.aggWg.Wait() // Need to wait for the background aggregation to finish because itsends to merge channels
	// Drain channel before closing
	select {
	case <-a.mergeChannel:
	default:
	}
	close(a.mergeChannel)
	if a.changesets {
		// Drain channel before closing
		select {
		case <-a.historyChannel:
		default:
		}
		close(a.historyChannel)
		a.historyWg.Wait()
	}
	a.mergeWg.Wait()
	// Closing state files only after background aggregation goroutine is finished
	for fType := FirstType; fType < NumberOfTypes; fType++ {
		a.closeFiles(fType)
	}
}

// checkOverlapWithMinStart does not need to lock tree lock, because it is only used in the constructor of Aggregator
func checkOverlapWithMinStart(treeName string, tree *btree.BTree, minStart uint64) error {
	if lastStateI := tree.Max(); lastStateI != nil {
		item := lastStateI.(*byEndBlockItem)
		if minStart != math.MaxUint64 && item.endBlock+1 != minStart {
			return fmt.Errorf("hole or overlap between %s state files and change files [%d-%d]", treeName, item.endBlock, minStart)
		}
	}
	return nil
}

func (a *Aggregator) readFromFiles(fType FileType, lock bool, blockNum uint64, filekey []byte, trace bool) ([]byte, uint64) {
	if lock {
		if fType == Commitment {
			for lockFType := FirstType; lockFType < NumberOfStateTypes; lockFType++ {
				a.fileLocks[lockFType].RLock()
				defer a.fileLocks[lockFType].RUnlock()
			}
		} else {
			a.fileLocks[fType].RLock()
			defer a.fileLocks[fType].RUnlock()
		}
	}
	h := a.archHasher
	arch := a.arches[fType]
	n := uint64(len(arch))
	if n > 0 {
		h.Reset()
		h.Write(filekey) //nolint:errcheck
		p, _ := h.Sum128()
		p = p % n
		v := uint64(atomic.LoadUint32(&arch[p]))
		//fmt.Printf("Reading from %s arch key [%x]=%d, %d\n", fType.String(), filekey, p, arch[p])
		if v == 0 {
			return nil, 0
		}
		a.files[fType].AscendGreaterOrEqual(&byEndBlockItem{startBlock: v, endBlock: v}, func(i btree.Item) bool {
			item := i.(*byEndBlockItem)
			if item.endBlock < blockNum {
				blockNum = item.endBlock
			}
			return false
		})
	}
	var val []byte
	var startBlock uint64
	a.files[fType].DescendLessOrEqual(&byEndBlockItem{endBlock: blockNum}, func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if trace {
			fmt.Printf("read %s %x: search in file [%d-%d]\n", fType.String(), filekey, item.startBlock, item.endBlock)
		}
		if item.tree != nil {
			ai := item.tree.Get(&AggregateItem{k: filekey})
			if ai == nil {
				return true
			}
			val = ai.(*AggregateItem).v
			startBlock = item.startBlock
			return false
		}
		if item.index.Empty() {
			return true
		}
		offset := item.indexReader.Lookup(filekey)
		g := item.getter
		g.Reset(offset)
		if g.HasNext() {
			if keyMatch, _ := g.Match(filekey); keyMatch {
				val, _ = g.Next(nil)
				if trace {
					fmt.Printf("read %s %x: found [%x] in file [%d-%d]\n", fType.String(), filekey, val, item.startBlock, item.endBlock)
				}
				startBlock = item.startBlock
				atomic.AddUint64(&a.fileHits, 1)
				return false
			}
		}
		atomic.AddUint64(&a.fileMisses, 1)
		return true
	})
	if fType == Commitment {
		// Transform references
		if len(val) > 0 {
			accountPlainKeys, storagePlainKeys, err := commitment.ExtractPlainKeys(val)
			if err != nil {
				panic(err)
			}
			var transAccountPks [][]byte
			var transStoragePks [][]byte
			for _, accountPlainKey := range accountPlainKeys {
				var apkBuf []byte
				if len(accountPlainKey) == length.Addr {
					// Non-optimised key originating from a database record
					apkBuf = accountPlainKey
				} else {
					// Optimised key referencing a state file record (file number and offset within the file)
					fileI := int(accountPlainKey[0])
					offset := decodeU64(accountPlainKey[1:])
					apkBuf, _ = a.readByOffset(Account, fileI, offset)
				}
				transAccountPks = append(transAccountPks, apkBuf)
			}
			for _, storagePlainKey := range storagePlainKeys {
				var spkBuf []byte
				if len(storagePlainKey) == length.Addr+length.Hash {
					// Non-optimised key originating from a database record
					spkBuf = storagePlainKey
				} else {
					// Optimised key referencing a state file record (file number and offset within the file)
					fileI := int(storagePlainKey[0])
					offset := decodeU64(storagePlainKey[1:])
					spkBuf, _ = a.readByOffset(Storage, fileI, offset)
				}
				transStoragePks = append(transStoragePks, spkBuf)
			}
			if val, err = commitment.ReplacePlainKeys(val, transAccountPks, transStoragePks, nil); err != nil {
				panic(err)
			}
		}
	}
	return val, startBlock
}

// readByOffset is assumed to be invoked under a read lock
func (a *Aggregator) readByOffset(fType FileType, fileI int, offset uint64) ([]byte, []byte) {
	var key, val []byte
	fi := 0
	a.files[fType].Ascend(func(i btree.Item) bool {
		if fi < fileI {
			fi++
			return true
		}
		item := i.(*byEndBlockItem)
		g := item.getter
		g.Reset(offset)
		key, _ = g.Next(nil)
		val, _ = g.Next(nil)
		return false
	})
	return key, val
}

func (a *Aggregator) MakeStateReader(blockNum uint64) *Reader {
	r := &Reader{
		a:        a,
		blockNum: blockNum,
	}
	return r
}

type Reader struct {
	a        *Aggregator
	search   AggregateItem
	blockNum uint64
}

func (r *Reader) ReadAccountData(addr []byte, trace bool) []byte {
	// Look in the summary table first
	r.search.k = addr
	if vi := r.a.trees[Account].Get(&r.search); vi != nil {
		return vi.(*AggregateItem).v
	}
	val, _ := r.a.readFromFiles(Account, true /* lock */, r.blockNum, addr, trace)
	return val
}

func (r *Reader) ReadAccountStorage(addr []byte, loc []byte, trace bool) *uint256.Int {
	// Look in the summary table first
	dbkey := make([]byte, len(addr)+len(loc))
	copy(dbkey[0:], addr)
	copy(dbkey[len(addr):], loc)
	r.search.k = dbkey
	var v []byte
	if vi := r.a.trees[Storage].Get(&r.search); vi != nil {
		v = vi.(*AggregateItem).v
	} else {
		v, _ = r.a.readFromFiles(Storage, true /* lock */, r.blockNum, dbkey, trace)
	}
	if v != nil {
		return new(uint256.Int).SetBytes(v)
	}
	return nil
}

func (r *Reader) ReadAccountCode(addr []byte, trace bool) []byte {
	// Look in the summary table first
	r.search.k = addr
	if vi := r.a.trees[Code].Get(&r.search); vi != nil {
		return vi.(*AggregateItem).v
	}
	// Look in the files
	val, _ := r.a.readFromFiles(Code, true /* lock */, r.blockNum, addr, trace)
	return val
}

func (r *Reader) ReadAccountCodeSize(addr []byte, trace bool) int {
	// Look in the summary table first
	r.search.k = addr
	if vi := r.a.trees[Code].Get(&r.search); vi != nil {
		return len(vi.(*AggregateItem).v)
	}
	// Look in the files. TODO - use specialised function to only lookup size
	val, _ := r.a.readFromFiles(Code, true /* lock */, r.blockNum, addr, trace)
	return len(val)
}

type Writer struct {
	a             *Aggregator
	search        AggregateItem // Aggregate item used to search in trees
	blockNum      uint64
	changeFileNum uint64 // Block number associated with the current change files. It is the last block number whose changes will go into that file
	changes       [NumberOfStateTypes]Changes
	commTree      *btree.BTree // BTree used for gathering commitment data
}

func (a *Aggregator) MakeStateWriter(beforeOn bool) *Writer {
	w := &Writer{
		a:        a,
		commTree: btree.New(32),
	}
	for fType := FirstType; fType < NumberOfStateTypes; fType++ {
		w.changes[fType].Init(fType.String(), a.aggregationStep, a.diffDir, w.a.changesets && fType != Commitment /* we do not unwind commitment ? */)
	}
	return w
}

func (w *Writer) Close() {
	typesLimit := Commitment
	if w.a.commitments {
		typesLimit = AccountHistory
	}
	for fType := FirstType; fType < typesLimit; fType++ {
		w.changes[fType].closeFiles()
	}
}

func (w *Writer) Reset(blockNum uint64) error {
	w.blockNum = blockNum
	typesLimit := Commitment
	if w.a.commitments {
		typesLimit = AccountHistory
	}
	if blockNum > w.changeFileNum {
		for fType := FirstType; fType < typesLimit; fType++ {
			if err := w.changes[fType].closeFiles(); err != nil {
				return err
			}
		}
		if w.changeFileNum != 0 {
			w.a.changesBtree.ReplaceOrInsert(&ChangesItem{startBlock: w.changeFileNum + 1 - w.a.aggregationStep, endBlock: w.changeFileNum, fileCount: 12})
		}
	}
	if w.changeFileNum == 0 || blockNum > w.changeFileNum {
		for fType := FirstType; fType < typesLimit; fType++ {
			if err := w.changes[fType].openFiles(blockNum, true /* write */); err != nil {
				return err
			}
		}
		w.changeFileNum = blockNum - (blockNum % w.a.aggregationStep) + w.a.aggregationStep - 1
	}
	return nil
}

type CommitmentItem struct {
	plainKey  []byte
	hashedKey []byte
	u         commitment.Update
}

func (i *CommitmentItem) Less(than btree.Item) bool {
	return bytes.Compare(i.hashedKey, than.(*CommitmentItem).hashedKey) < 0
}

func (w *Writer) lockFn() {
	for fType := FirstType; fType < NumberOfStateTypes; fType++ {
		w.a.fileLocks[fType].RLock()
	}
}

func (w *Writer) unlockFn() {
	for fType := FirstType; fType < NumberOfStateTypes; fType++ {
		w.a.fileLocks[fType].RUnlock()
	}
}

func (w *Writer) branchFn(prefix []byte) []byte {
	for lockFType := FirstType; lockFType < NumberOfStateTypes; lockFType++ {
		w.a.fileLocks[lockFType].RLock()
		defer w.a.fileLocks[lockFType].RUnlock()
	}
	var mergedVal []byte
	// Look in the summary table first
	w.search.k = prefix
	if vi := w.a.trees[Commitment].Get(&w.search); vi != nil {
		mergedVal = vi.(*AggregateItem).v
	}
	// Look in the files and merge, while it becomes complete
	var startBlock uint64 = w.blockNum + 1
	for mergedVal == nil || !commitment.IsComplete(mergedVal) {
		if startBlock == 0 {
			panic(fmt.Sprintf("Incomplete branch data prefix [%x], mergeVal=[%x], startBlock=%d\n", commitment.CompactToHex(prefix), mergedVal, startBlock))
		}
		var val []byte
		val, startBlock = w.a.readFromFiles(Commitment, false /* lock */, startBlock-1, prefix, false /* trace */)
		if val == nil {
			if mergedVal == nil {
				return nil
			}
			panic(fmt.Sprintf("Incomplete branch data prefix [%x], mergeVal=[%x], startBlock=%d\n", commitment.CompactToHex(prefix), mergedVal, startBlock))
		}
		var err error
		//fmt.Printf("Pre-merge prefix [%x] [%x]+[%x], startBlock %d\n", commitment.CompactToHex(prefix), val, mergedVal, startBlock)
		if mergedVal == nil {
			mergedVal = val
		} else if mergedVal, err = commitment.MergeBranches(val, mergedVal, nil); err != nil {
			panic(err)
		}
		//fmt.Printf("Post-merge prefix [%x] [%x], startBlock %d\n", commitment.CompactToHex(prefix), mergedVal, startBlock)
	}
	if mergedVal == nil {
		return nil
	}
	//fmt.Printf("Returning branch data prefix [%x], mergeVal=[%x], startBlock=%d\n", commitment.CompactToHex(prefix), mergedVal, startBlock)
	return mergedVal[2:] // Skip touchMap but keep afterMap
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

func (w *Writer) accountFn(plainKey []byte, cell *commitment.Cell) []byte {
	var enc []byte
	// Look in the summary table first
	w.search.k = plainKey
	if encI := w.a.trees[Account].Get(&w.search); encI != nil {
		enc = encI.(*AggregateItem).v
	} else {
		// Look in the files
		enc, _ = w.a.readFromFiles(Account, true /* lock */, w.blockNum, plainKey, false /* trace */)
	}
	cell.Nonce = 0
	cell.Balance.Clear()
	copy(cell.CodeHash[:], commitment.EmptyCodeHash)

	if len(enc) > 0 {
		pos := 0
		nonceBytes := int(enc[pos])
		pos++
		if nonceBytes > 0 {
			cell.Nonce = bytesToUint64(enc[pos : pos+nonceBytes])
			pos += nonceBytes
		}
		balanceBytes := int(enc[pos])
		pos++
		if balanceBytes > 0 {
			cell.Balance.SetBytes(enc[pos : pos+balanceBytes])
		}
	}
	w.search.k = plainKey
	if encI := w.a.trees[Code].Get(&w.search); encI != nil {
		enc = encI.(*AggregateItem).v
	} else {
		// Look in the files
		enc, _ = w.a.readFromFiles(Code, true /* lock */, w.blockNum, plainKey, false /* trace */)
	}
	if len(enc) > 0 {
		w.a.keccak.Reset()
		w.a.keccak.Write(enc)
		w.a.keccak.(io.Reader).Read(cell.CodeHash[:])
	}
	return plainKey
}

func (w *Writer) storageFn(plainKey []byte, cell *commitment.Cell) []byte {
	var enc []byte
	// Look in the summary table first
	w.search.k = plainKey
	if encI := w.a.trees[Storage].Get(&w.search); encI != nil {
		enc = encI.(*AggregateItem).v
	} else {
		// Look in the files
		enc, _ = w.a.readFromFiles(Storage, true /* lock */, w.blockNum, plainKey, false /* trace */)
	}
	cell.StorageLen = len(enc)
	copy(cell.Storage[:], enc)
	return plainKey
}

func (w *Writer) captureCommitmentType(fType FileType, trace bool, f func(commTree *btree.BTree, h hash.Hash, key, val []byte)) {
	lastOffsetKey := 0
	lastOffsetVal := 0
	for i, offsetKey := range w.changes[fType].keys.wordOffsets {
		offsetVal := w.changes[fType].after.wordOffsets[i]
		key := w.changes[fType].keys.words[lastOffsetKey:offsetKey]
		val := w.changes[fType].after.words[lastOffsetVal:offsetVal]
		if trace {
			fmt.Printf("captureCommitmentData %s [%x]=>[%x]\n", fType.String(), key, val)
		}
		f(w.commTree, w.a.keccak, key, val)
		lastOffsetKey = offsetKey
		lastOffsetVal = offsetVal
	}
}

func (w *Writer) captureCommitmentData(trace bool) {
	if trace {
		fmt.Printf("captureCommitmentData start w.commTree.Len()=%d\n", w.commTree.Len())
	}
	w.captureCommitmentType(Code, trace, func(commTree *btree.BTree, h hash.Hash, key, val []byte) {
		h.Reset()
		h.Write(key)
		hashedKey := h.Sum(nil)
		var c = &CommitmentItem{plainKey: common.Copy(key), hashedKey: make([]byte, len(hashedKey)*2)}
		for i, b := range hashedKey {
			c.hashedKey[i*2] = (b >> 4) & 0xf
			c.hashedKey[i*2+1] = b & 0xf
		}
		c.u.Flags = commitment.CODE_UPDATE
		item := commTree.Get(&CommitmentItem{hashedKey: c.hashedKey})
		if item != nil {
			itemC := item.(*CommitmentItem)
			if itemC.u.Flags&commitment.BALANCE_UPDATE != 0 {
				c.u.Flags |= commitment.BALANCE_UPDATE
				c.u.Balance.Set(&itemC.u.Balance)
			}
			if itemC.u.Flags&commitment.NONCE_UPDATE != 0 {
				c.u.Flags |= commitment.NONCE_UPDATE
				c.u.Nonce = itemC.u.Nonce
			}
			if itemC.u.Flags == commitment.DELETE_UPDATE && len(val) == 0 {
				c.u.Flags = commitment.DELETE_UPDATE
			} else {
				h.Reset()
				h.Write(val)
				h.(io.Reader).Read(c.u.CodeHashOrStorage[:])
			}
		} else {
			h.Reset()
			h.Write(val)
			h.(io.Reader).Read(c.u.CodeHashOrStorage[:])
		}
		commTree.ReplaceOrInsert(c)
	})
	w.captureCommitmentType(Account, trace, func(commTree *btree.BTree, h hash.Hash, key, val []byte) {
		h.Reset()
		h.Write(key)
		hashedKey := h.Sum(nil)
		var c = &CommitmentItem{plainKey: common.Copy(key), hashedKey: make([]byte, len(hashedKey)*2)}
		for i, b := range hashedKey {
			c.hashedKey[i*2] = (b >> 4) & 0xf
			c.hashedKey[i*2+1] = b & 0xf
		}
		if len(val) == 0 {
			c.u.Flags = commitment.DELETE_UPDATE
		} else {
			c.u.DecodeForStorage(val)
			c.u.Flags = commitment.BALANCE_UPDATE | commitment.NONCE_UPDATE
			item := commTree.Get(&CommitmentItem{hashedKey: c.hashedKey})
			if item != nil {
				itemC := item.(*CommitmentItem)
				if itemC.u.Flags&commitment.CODE_UPDATE != 0 {
					c.u.Flags |= commitment.CODE_UPDATE
					copy(c.u.CodeHashOrStorage[:], itemC.u.CodeHashOrStorage[:])
				}
			}
		}
		commTree.ReplaceOrInsert(c)
	})
	w.captureCommitmentType(Storage, trace, func(commTree *btree.BTree, h hash.Hash, key, val []byte) {
		hashedKey := make([]byte, 2*length.Hash)
		h.Reset()
		h.Write(key[:length.Addr])
		h.(io.Reader).Read(hashedKey[:length.Hash])
		h.Reset()
		h.Write(key[length.Addr:])
		h.(io.Reader).Read(hashedKey[length.Hash:])
		var c = &CommitmentItem{plainKey: common.Copy(key), hashedKey: make([]byte, len(hashedKey)*2)}
		for i, b := range hashedKey {
			c.hashedKey[i*2] = (b >> 4) & 0xf
			c.hashedKey[i*2+1] = b & 0xf
		}
		c.u.ValLength = len(val)
		if len(val) > 0 {
			copy(c.u.CodeHashOrStorage[:], val)
		}
		if len(val) == 0 {
			c.u.Flags = commitment.DELETE_UPDATE
		} else {
			c.u.Flags = commitment.STORAGE_UPDATE
		}
		commTree.ReplaceOrInsert(c)
	})
	if trace {
		fmt.Printf("captureCommitmentData end w.commTree.Len()=%d\n", w.commTree.Len())
	}
}

// computeCommitment is computing the commitment to the state after
// the change would have been applied.
// It assumes that the state accessible via the aggregator has already been
// modified with the new values
// At the moment, it is specific version for hex merkle patricia tree commitment
// but it will be extended to support other types of commitments
func (w *Writer) computeCommitment(trace bool) ([]byte, error) {
	if trace {
		fmt.Printf("computeCommitment w.commTree.Len()=%d\n", w.commTree.Len())
	}
	plainKeys := make([][]byte, w.commTree.Len())
	hashedKeys := make([][]byte, w.commTree.Len())
	updates := make([]commitment.Update, w.commTree.Len())
	j := 0
	w.commTree.Ascend(func(i btree.Item) bool {
		item := i.(*CommitmentItem)
		plainKeys[j] = item.plainKey
		hashedKeys[j] = item.hashedKey
		updates[j] = item.u
		j++
		return true
	})
	w.a.hph.Reset()
	w.a.hph.ResetFns(w.branchFn, w.accountFn, w.storageFn, w.lockFn, w.unlockFn)
	w.a.hph.SetTrace(trace)
	branchNodeUpdates, err := w.a.hph.ProcessUpdates(plainKeys, hashedKeys, updates)
	if err != nil {
		return nil, err
	}
	for prefixStr, branchNodeUpdate := range branchNodeUpdates {
		if branchNodeUpdate == nil {
			continue
		}
		prefix := []byte(prefixStr)
		w.search.k = prefix
		var prevV *AggregateItem
		if prevVI := w.a.trees[Commitment].Get(&w.search); prevVI != nil {
			prevV = prevVI.(*AggregateItem)
		}

		var original []byte
		if prevV == nil {
			original, _ = w.a.readFromFiles(Commitment, true /* lock */, w.blockNum, prefix, false)
		} else {
			original = prevV.v
		}
		if original != nil {
			var mergedVal []byte
			if mergedVal, err = commitment.MergeBranches(original, branchNodeUpdate, nil); err == nil {
				//fmt.Printf("computeCommitment merge [%x] [%x]+[%x]=>[%x]\n", commitment.CompactToHex(prefix), original, branchNodeUpdate, mergedVal)
				branchNodeUpdate = mergedVal
			} else {
				return nil, err
			}
		}
		//fmt.Printf("computeCommitment set [%x] [%x]\n", commitment.CompactToHex(prefix), branchNodeUpdate)
		if prevV == nil {
			w.a.trees[Commitment].ReplaceOrInsert(&AggregateItem{k: prefix, v: branchNodeUpdate, count: 1})
		} else {
			prevV.v = branchNodeUpdate
			prevV.count++
		}
		if len(branchNodeUpdate) == 0 {
			w.changes[Commitment].delete(prefix, original)
		} else {
			if prevV == nil && len(original) == 0 {
				w.changes[Commitment].insert(prefix, branchNodeUpdate)
			} else {
				w.changes[Commitment].update(prefix, original, branchNodeUpdate)
			}
		}
	}
	var rootHash []byte
	if rootHash, err = w.a.hph.RootHash(); err != nil {
		return nil, err
	}
	return rootHash, nil
}

func (w *Writer) FinishTx(txNum uint64, trace bool) error {
	if w.a.commitments {
		w.captureCommitmentData(trace)
	}
	var err error
	for fType := FirstType; fType < Commitment; fType++ {
		if err = w.changes[fType].finish(txNum); err != nil {
			return fmt.Errorf("finish %sChanges: %w", fType.String(), err)
		}
	}
	return nil
}

func (w *Writer) ComputeCommitment(trace bool) ([]byte, error) {
	if !w.a.commitments {
		return nil, fmt.Errorf("commitments turned off")
	}
	comm, err := w.computeCommitment(trace)
	if err != nil {
		return nil, fmt.Errorf("compute commitment: %w", err)
	}
	w.commTree.Clear(true)
	if err = w.changes[Commitment].finish(w.blockNum); err != nil {
		return nil, fmt.Errorf("finish commChanges: %w", err)
	}
	return comm, nil
}

// Aggegate should be called to check if the aggregation is required, and
// if it is required, perform it
func (w *Writer) Aggregate(trace bool) error {
	if w.blockNum < w.a.unwindLimit+w.a.aggregationStep-1 {
		return nil
	}
	diff := w.blockNum - w.a.unwindLimit
	if (diff+1)%w.a.aggregationStep != 0 {
		return nil
	}
	if err := w.aggregateUpto(diff+1-w.a.aggregationStep, diff); err != nil {
		return fmt.Errorf("aggregateUpto(%d, %d): %w", diff+1-w.a.aggregationStep, diff, err)
	}
	return nil
}

func (w *Writer) UpdateAccountData(addr []byte, account []byte, trace bool) {
	var prevV *AggregateItem
	w.search.k = addr
	if prevVI := w.a.trees[Account].Get(&w.search); prevVI != nil {
		prevV = prevVI.(*AggregateItem)
	}
	var original []byte
	if prevV == nil {
		original, _ = w.a.readFromFiles(Account, true /* lock */, w.blockNum, addr, trace)
	} else {
		original = prevV.v
	}
	if bytes.Equal(account, original) {
		// No change
		return
	}
	if prevV == nil {
		w.a.trees[Account].ReplaceOrInsert(&AggregateItem{k: addr, v: account, count: 1})
	} else {
		prevV.v = account
		prevV.count++
	}
	if prevV == nil && len(original) == 0 {
		w.changes[Account].insert(addr, account)
	} else {
		w.changes[Account].update(addr, original, account)
	}
	if trace {
		w.a.trace = true
		w.a.tracedKeys[string(addr)] = struct{}{}
	}
}

func (w *Writer) UpdateAccountCode(addr []byte, code []byte, trace bool) {
	var prevV *AggregateItem
	w.search.k = addr
	if prevVI := w.a.trees[Code].Get(&w.search); prevVI != nil {
		prevV = prevVI.(*AggregateItem)
	}
	var original []byte
	if prevV == nil {
		original, _ = w.a.readFromFiles(Code, true /* lock */, w.blockNum, addr, trace)
	} else {
		original = prevV.v
	}
	if prevV == nil {
		w.a.trees[Code].ReplaceOrInsert(&AggregateItem{k: addr, v: code, count: 1})
	} else {
		prevV.v = code
		prevV.count++
	}
	if prevV == nil && len(original) == 0 {
		w.changes[Code].insert(addr, code)
	} else {
		w.changes[Code].update(addr, original, code)
	}
	if trace {
		w.a.trace = true
		w.a.tracedKeys[string(addr)] = struct{}{}
	}
}

type CursorType uint8

const (
	FILE_CURSOR CursorType = iota
	TREE_CURSOR
)

// CursorItem is the item in the priority queue used to do merge interation
// over storage of a given account
type CursorItem struct {
	t        CursorType // Whether this item represents state file or DB record, or tree
	endBlock uint64
	key, val []byte
	dg       *compress.Getter
	tree     *btree.BTree
}

type CursorHeap []*CursorItem

func (ch CursorHeap) Len() int {
	return len(ch)
}

func (ch CursorHeap) Less(i, j int) bool {
	cmp := bytes.Compare(ch[i].key, ch[j].key)
	if cmp == 0 {
		// when keys match, the items with later blocks are preferred
		return ch[i].endBlock > ch[j].endBlock
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

func (w *Writer) deleteAccount(addr []byte, trace bool) bool {
	var prevV *AggregateItem
	w.search.k = addr
	if prevVI := w.a.trees[Account].Get(&w.search); prevVI != nil {
		prevV = prevVI.(*AggregateItem)
	}
	var original []byte
	if prevV == nil {
		original, _ = w.a.readFromFiles(Account, true /* lock */, w.blockNum, addr, trace)
		if original == nil {
			return false
		}
	} else {
		original = prevV.v
	}
	if prevV == nil {
		w.a.trees[Account].ReplaceOrInsert(&AggregateItem{k: addr, v: nil, count: 1})
	} else {
		prevV.v = nil
		prevV.count++
	}
	w.changes[Account].delete(addr, original)
	return true
}

func (w *Writer) deleteCode(addr []byte, trace bool) {
	var prevV *AggregateItem
	w.search.k = addr
	if prevVI := w.a.trees[Code].Get(&w.search); prevVI != nil {
		prevV = prevVI.(*AggregateItem)
	}
	var original []byte
	if prevV == nil {
		original, _ = w.a.readFromFiles(Code, true /* lock */, w.blockNum, addr, trace)
		if original == nil {
			// Nothing to do
			return
		}
	} else {
		original = prevV.v
	}
	if prevV == nil {
		w.a.trees[Code].ReplaceOrInsert(&AggregateItem{k: addr, v: nil, count: 1})
	} else {
		prevV.v = nil
		prevV.count++
	}
	w.changes[Code].delete(addr, original)
}

func (w *Writer) DeleteAccount(addr []byte, trace bool) {
	if deleted := w.deleteAccount(addr, trace); !deleted {
		return
	}
	w.a.fileLocks[Storage].RLock()
	defer w.a.fileLocks[Storage].RUnlock()
	w.deleteCode(addr, trace)
	// Find all storage items for this address
	var cp CursorHeap
	heap.Init(&cp)
	w.search.k = addr
	foundInTree := false
	var k, v []byte
	w.a.trees[Storage].AscendGreaterOrEqual(&w.search, func(i btree.Item) bool {
		item := i.(*AggregateItem)
		if bytes.HasPrefix(item.k, addr) {
			foundInTree = true
			k = item.k
			v = item.v
		}
		return false
	})
	if foundInTree {
		heap.Push(&cp, &CursorItem{t: TREE_CURSOR, key: common.Copy(k), val: common.Copy(v), tree: w.a.trees[Storage], endBlock: w.blockNum})
	}
	w.a.files[Storage].Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.tree != nil {
			item.tree.AscendGreaterOrEqual(&AggregateItem{k: addr}, func(ai btree.Item) bool {
				aitem := ai.(*AggregateItem)
				if !bytes.HasPrefix(aitem.k, addr) {
					return false
				}
				if len(aitem.k) == len(addr) {
					return true
				}
				heap.Push(&cp, &CursorItem{t: TREE_CURSOR, key: aitem.k, val: aitem.v, tree: item.tree, endBlock: item.endBlock})
				return false
			})
			return true
		}
		if item.index.Empty() {
			return true
		}
		offset := item.indexReader.Lookup(addr)
		g := item.getter
		g.Reset(offset)
		if g.HasNext() {
			if keyMatch, _ := g.Match(addr); !keyMatch {
				//fmt.Printf("DeleteAccount %x - not found anchor in file [%d-%d]\n", addr, item.startBlock, item.endBlock)
				return true
			}
			g.Skip()
		}
		if g.HasNext() {
			key, _ := g.Next(nil)
			if bytes.HasPrefix(key, addr) {
				val, _ := g.Next(nil)
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, key: key, val: val, dg: g, endBlock: item.endBlock})
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
					if bytes.HasPrefix(ci1.key, addr) {
						ci1.val, _ = ci1.dg.Next(ci1.val[:0])
						heap.Fix(&cp, 0)
					} else {
						heap.Pop(&cp)
					}
				} else {
					heap.Pop(&cp)
				}
			case TREE_CURSOR:
				skip := true
				var aitem *AggregateItem
				ci1.tree.AscendGreaterOrEqual(&AggregateItem{k: ci1.key}, func(ai btree.Item) bool {
					if skip {
						skip = false
						return true
					}
					aitem = ai.(*AggregateItem)
					return false
				})
				if aitem != nil && bytes.HasPrefix(aitem.k, addr) {
					ci1.key = aitem.k
					ci1.val = aitem.v
					heap.Fix(&cp, 0)
				} else {
					heap.Pop(&cp)
				}
			}
		}
		var prevV *AggregateItem
		w.search.k = lastKey
		if prevVI := w.a.trees[Storage].Get(&w.search); prevVI != nil {
			prevV = prevVI.(*AggregateItem)
		}
		if prevV == nil {
			w.a.trees[Storage].ReplaceOrInsert(&AggregateItem{k: lastKey, v: nil, count: 1})
		} else {
			prevV.v = nil
			prevV.count++
		}
		w.changes[Storage].delete(lastKey, lastVal)
	}
	if trace {
		w.a.trace = true
		w.a.tracedKeys[string(addr)] = struct{}{}
	}
}

func (w *Writer) WriteAccountStorage(addr []byte, loc []byte, value *uint256.Int, trace bool) {
	dbkey := make([]byte, len(addr)+len(loc))
	copy(dbkey[0:], addr)
	copy(dbkey[len(addr):], loc)
	w.search.k = dbkey
	var prevV *AggregateItem
	if prevVI := w.a.trees[Storage].Get(&w.search); prevVI != nil {
		prevV = prevVI.(*AggregateItem)
	}
	var original []byte
	if prevV == nil {
		original, _ = w.a.readFromFiles(Storage, true /* lock */, w.blockNum, dbkey, trace)
	} else {
		original = prevV.v
	}
	vLen := value.ByteLen()
	v := make([]byte, vLen)
	value.WriteToSlice(v)
	if bytes.Equal(v, original) {
		// No change
		return
	}
	if prevV == nil {
		w.a.trees[Storage].ReplaceOrInsert(&AggregateItem{k: dbkey, v: v, count: 1})
	} else {
		prevV.v = v
		prevV.count++
	}
	if prevV == nil && len(original) == 0 {
		w.changes[Storage].insert(dbkey, v)
	} else {
		w.changes[Storage].update(dbkey, original, v)
	}
	if trace {
		w.a.trace = true
		w.a.tracedKeys[string(dbkey)] = struct{}{}
	}
}

// findLargestMerge looks through the state files of the speficied type and determines the largest merge that can be undertaken
// a state file block [a; b] is valid if its length is a divisor of its starting block, or `(b-a+1) = 0 mod a`
func (a *Aggregator) findLargestMerge(fType FileType, maxTo uint64, maxSpan uint64) (toAggregate []*byEndBlockItem, pre []*byEndBlockItem, post []*byEndBlockItem, aggFrom uint64, aggTo uint64) {
	a.fileLocks[fType].RLock()
	defer a.fileLocks[fType].RUnlock()
	var maxEndBlock uint64
	a.files[fType].DescendLessOrEqual(&byEndBlockItem{endBlock: maxTo}, func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor == nil {
			return true
		}
		maxEndBlock = item.endBlock
		return false
	})
	if maxEndBlock == 0 {
		return
	}
	a.files[fType].Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor == nil {
			return true // Skip B-tree based items
		}
		pre = append(pre, item)
		if aggTo == 0 {
			var doubleEnd uint64
			nextDouble := item.endBlock
			for nextDouble <= maxEndBlock && nextDouble-item.startBlock < maxSpan {
				doubleEnd = nextDouble
				nextDouble = doubleEnd + (doubleEnd - item.startBlock) + 1
			}
			if doubleEnd != item.endBlock {
				aggFrom = item.startBlock
				aggTo = doubleEnd
			} else {
				post = append(post, item)
				return true
			}
		}
		toAggregate = append(toAggregate, item)
		return item.endBlock < aggTo
	})
	return
}

func (a *Aggregator) computeAggregation(fType FileType,
	toAggregate []*byEndBlockItem, aggFrom uint64, aggTo uint64,
	valTransform func(val []byte, transValBuf []byte) ([]byte, error),
	mergeFunc func(preval, val, buf []byte) ([]byte, error),
	valCompressed bool,
	withIndex bool, prefixLen int) (*byEndBlockItem, error) {
	var item2 = &byEndBlockItem{startBlock: aggFrom, endBlock: aggTo}
	var cp CursorHeap
	heap.Init(&cp)
	for _, ag := range toAggregate {
		g := ag.decompressor.MakeGetter()
		g.Reset(0)
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
			heap.Push(&cp, &CursorItem{t: FILE_CURSOR, dg: g, key: key, val: val, endBlock: ag.endBlock})
		}
	}
	var err error
	var count int
	if item2.decompressor, count, err = a.mergeIntoStateFile(&cp, prefixLen, fType, aggFrom, aggTo, a.diffDir, valTransform, mergeFunc, valCompressed); err != nil {
		return nil, fmt.Errorf("mergeIntoStateFile %s [%d-%d]: %w", fType.String(), aggFrom, aggTo, err)
	}
	item2.getter = item2.decompressor.MakeGetter()
	item2.getterMerge = item2.decompressor.MakeGetter()
	if withIndex {
		idxPath := filepath.Join(a.diffDir, fmt.Sprintf("%s.%d-%d.idx", fType.String(), aggFrom, aggTo))
		if item2.index, err = buildIndex(item2.decompressor, idxPath, a.diffDir, count); err != nil {
			return nil, fmt.Errorf("mergeIntoStateFile buildIndex %s [%d-%d]: %w", fType.String(), aggFrom, aggTo, err)
		}
		item2.indexReader = recsplit.NewIndexReader(item2.index)
		item2.readerMerge = recsplit.NewIndexReader(item2.index)
	}
	return item2, nil
}

func createDatAndIndex(treeName string, diffDir string, bt *btree.BTree, blockFrom uint64, blockTo uint64) (*compress.Decompressor, *recsplit.Index, error) {
	datPath := filepath.Join(diffDir, fmt.Sprintf("%s.%d-%d.dat", treeName, blockFrom, blockTo))
	idxPath := filepath.Join(diffDir, fmt.Sprintf("%s.%d-%d.idx", treeName, blockFrom, blockTo))
	count, err := btreeToFile(bt, datPath, diffDir, false /* trace */, 1 /* workers */)
	if err != nil {
		return nil, nil, fmt.Errorf("createDatAndIndex %s build btree: %w", treeName, err)
	}
	var d *compress.Decompressor
	if d, err = compress.NewDecompressor(datPath); err != nil {
		return nil, nil, fmt.Errorf("createDatAndIndex %s decompressor: %w", treeName, err)
	}
	var index *recsplit.Index
	if index, err = buildIndex(d, idxPath, diffDir, count); err != nil {
		return nil, nil, fmt.Errorf("createDatAndIndex %s buildIndex: %w", treeName, err)
	}
	return d, index, nil
}

func (a *Aggregator) addLocked(fType FileType, item *byEndBlockItem) {
	a.fileLocks[fType].Lock()
	defer a.fileLocks[fType].Unlock()
	a.files[fType].ReplaceOrInsert(item)
}

func (w *Writer) aggregateUpto(blockFrom, blockTo uint64) error {
	// React on any previous error of aggregation or merge
	select {
	case err := <-w.a.aggError:
		return err
	case err := <-w.a.mergeError:
		return err
	case err := <-w.a.historyError:
		return err
	default:
	}
	typesLimit := Commitment
	if w.a.commitments {
		typesLimit = AccountHistory
	}
	t := time.Now()
	i := w.a.changesBtree.Get(&ChangesItem{startBlock: blockFrom, endBlock: blockTo})
	if i == nil {
		return fmt.Errorf("did not find change files for [%d-%d], w.a.changesBtree.Len() = %d", blockFrom, blockTo, w.a.changesBtree.Len())
	}
	item := i.(*ChangesItem)
	if item.startBlock != blockFrom {
		return fmt.Errorf("expected change files[%d-%d], got [%d-%d]", blockFrom, blockTo, item.startBlock, item.endBlock)
	}
	w.a.changesBtree.Delete(i)
	var aggTask AggregationTask
	for fType := FirstType; fType < typesLimit; fType++ {
		aggTask.changes[fType].Init(fType.String(), w.a.aggregationStep, w.a.diffDir, w.a.changesets && fType != Commitment)
	}
	var err error
	for fType := FirstType; fType < typesLimit; fType++ {
		var prefixLen int
		if fType == Storage {
			prefixLen = length.Addr
		}
		if aggTask.bt[fType], err = aggTask.changes[fType].aggregate(blockFrom, blockTo, prefixLen, w.a.trees[fType], fType == Commitment); err != nil {
			return fmt.Errorf("aggregate %sChanges: %w", fType.String(), err)
		}
	}
	aggTask.blockFrom = blockFrom
	aggTask.blockTo = blockTo
	aggTime := time.Since(t)
	t = time.Now()
	// At this point, all the changes are gathered in 4 B-trees (accounts, code, storage and commitment) and removed from the database
	// What follows can be done in the 1st background goroutine
	w.a.aggChannel <- &aggTask
	<-w.a.aggBackCh // Waiting for the B-tree based items have been added
	handoverTime := time.Since(t)
	if handoverTime > time.Second {
		log.Info("Long handover to background aggregation", "from", blockFrom, "to", blockTo, "composition", aggTime, "handover", time.Since(t))
	}
	return nil
}

// mergeIntoStateFile assumes that all entries in the cp heap have type FILE_CURSOR
func (a *Aggregator) mergeIntoStateFile(cp *CursorHeap, prefixLen int,
	fType FileType, startBlock, endBlock uint64, dir string,
	valTransform func(val []byte, transValBuf []byte) ([]byte, error),
	mergeFunc func(preval, val, buf []byte) ([]byte, error),
	valCompressed bool,
) (*compress.Decompressor, int, error) {
	datPath := filepath.Join(dir, fmt.Sprintf("%s.%d-%d.dat", fType.String(), startBlock, endBlock))
	comp, err := compress.NewCompressor(context.Background(), AggregatorPrefix, datPath, dir, compress.MinPatternScore, 1)
	if err != nil {
		return nil, 0, fmt.Errorf("compressor %s: %w", datPath, err)
	}
	defer comp.Close()
	count := 0
	// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
	// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
	// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
	// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
	// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
	var keyBuf, valBuf, transValBuf []byte
	for cp.Len() > 0 {
		lastKey := common.Copy((*cp)[0].key)
		lastVal := common.Copy((*cp)[0].val)
		var mergedOnce bool
		if a.trace {
			if _, ok := a.tracedKeys[string(lastKey)]; ok {
				fmt.Printf("looking at key %x val [%x] endBlock %d to merge into [%d-%d]\n", lastKey, lastVal, (*cp)[0].endBlock, startBlock, endBlock)
			}
		}
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal((*cp)[0].key, lastKey) {
			ci1 := (*cp)[0]
			if a.trace {
				if _, ok := a.tracedKeys[string(ci1.key)]; ok {
					fmt.Printf("skipping same key %x val [%x] endBlock %d to merge into [%d-%d]\n", ci1.key, ci1.val, ci1.endBlock, startBlock, endBlock)
				}
			}
			if ci1.t != FILE_CURSOR {
				return nil, 0, fmt.Errorf("mergeIntoStateFile: cursor of unexpected type: %d", ci1.t)
			}
			if mergedOnce {
				//fmt.Printf("mergeIntoStateFile pre-merge prefix [%x], [%x]+[%x]\n", commitment.CompactToHex(lastKey), ci1.val, lastVal)
				if lastVal, err = mergeFunc(ci1.val, lastVal, nil); err != nil {
					return nil, 0, fmt.Errorf("mergeIntoStateFile: merge values: %w", err)
				}
				//fmt.Printf("mergeIntoStateFile post-merge  prefix [%x], [%x]\n", commitment.CompactToHex(lastKey), lastVal)
			} else {
				mergedOnce = true
			}
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.Next(ci1.key[:0])
				if valCompressed {
					ci1.val, _ = ci1.dg.Next(ci1.val[:0])
				} else {
					ci1.val, _ = ci1.dg.NextUncompressed()
				}
				heap.Fix(cp, 0)
			} else {
				heap.Pop(cp)
			}
		}
		var skip bool
		switch fType {
		case Storage:
			// Inside storage files, there is a special item with empty value, and the key equal to the contract's address
			// This special item is inserted before the contract storage items, in order to find them using un-ordered index
			// (for the purposes of SELF-DESTRUCT and some RPC methods that require enumeration of contract storage)
			// We will only skip this special item if there are no more corresponding storage items left
			// (this is checked further down with `bytes.HasPrefix(lastKey, keyBuf)`)
			skip = startBlock == 0 && len(lastVal) == 0 && len(lastKey) != prefixLen
		case Commitment:
			// For commitments, the 3rd and 4th bytes of the value (zero-based 2 and 3) contain so-called `afterMap`
			// Its bit are set for children that are present in the tree, and unset for those that are not (deleted, for example)
			// If all bits are zero (check below), this branch can be skipped, since it is empty
			skip = startBlock == 0 && len(lastVal) >= 4 && lastVal[2] == 0 && lastVal[3] == 0
		case AccountHistory, StorageHistory, CodeHistory:
			skip = false
		default:
			// For the rest of types, empty value means deletion
			skip = startBlock == 0 && len(lastVal) == 0
		}
		if skip { // Deleted marker can be skipped if we merge into the first file, except for the storage addr marker
			if _, ok := a.tracedKeys[string(keyBuf)]; ok {
				fmt.Printf("skipped key %x for [%d-%d]\n", keyBuf, startBlock, endBlock)
			}
		} else {
			// The check `bytes.HasPrefix(lastKey, keyBuf)` is checking whether the `lastKey` is the first item
			// of some contract's storage, and `keyBuf` (the item just before that) is the special item with the
			// key being contract's address. If so, the special item (keyBuf => []) needs to be preserved
			if keyBuf != nil && (prefixLen == 0 || len(keyBuf) != prefixLen || bytes.HasPrefix(lastKey, keyBuf)) {
				if err = comp.AddWord(keyBuf); err != nil {
					return nil, 0, err
				}
				if a.trace {
					if _, ok := a.tracedKeys[string(keyBuf)]; ok {
						fmt.Printf("merge key %x val [%x] into [%d-%d]\n", keyBuf, valBuf, startBlock, endBlock)
					}
				}
				count++ // Only counting keys, not values
				if valTransform != nil {
					if transValBuf, err = valTransform(valBuf, transValBuf[:0]); err != nil {
						return nil, 0, fmt.Errorf("mergeIntoStateFile valTransform [%x]: %w", valBuf, err)
					}
					if err = comp.AddWord(transValBuf); err != nil {
						return nil, 0, err
					}
				} else if valCompressed {
					if err = comp.AddWord(valBuf); err != nil {
						return nil, 0, err
					}
				} else {
					if err = comp.AddUncompressedWord(valBuf); err != nil {
						return nil, 0, err
					}
				}
				//if fType == AccountHistory {
				//	fmt.Printf("merge %s.%d-%d [%x]=>[%x]\n", fType.String(), startBlock, endBlock, keyBuf, valBuf)
				//}
			}
			keyBuf = append(keyBuf[:0], lastKey...)
			valBuf = append(valBuf[:0], lastVal...)
		}
	}
	if keyBuf != nil {
		if err = comp.AddWord(keyBuf); err != nil {
			return nil, 0, err
		}
		if a.trace {
			if _, ok := a.tracedKeys[string(keyBuf)]; ok {
				fmt.Printf("merge key %x val [%x] into [%d-%d]\n", keyBuf, valBuf, startBlock, endBlock)
			}
		}
		count++ // Only counting keys, not values
		if valTransform != nil {
			if transValBuf, err = valTransform(valBuf, transValBuf[:0]); err != nil {
				return nil, 0, fmt.Errorf("mergeIntoStateFile valTransform [%x]: %w", valBuf, err)
			}
			if err = comp.AddWord(transValBuf); err != nil {
				return nil, 0, err
			}
		} else if valCompressed {
			if err = comp.AddWord(valBuf); err != nil {
				return nil, 0, err
			}
		} else {
			if err = comp.AddUncompressedWord(valBuf); err != nil {
				return nil, 0, err
			}
		}
		//if fType == AccountHistory {
		//	fmt.Printf("merge %s.%d-%d [%x]=>[%x]\n", fType.String(), startBlock, endBlock, keyBuf, valBuf)
		//}
	}
	if err = comp.Compress(); err != nil {
		return nil, 0, err
	}
	var d *compress.Decompressor
	if d, err = compress.NewDecompressor(datPath); err != nil {
		return nil, 0, fmt.Errorf("decompressor: %w", err)
	}
	return d, count, nil
}

func (a *Aggregator) stats(fType FileType) (count int, datSize, idxSize int64) {
	a.fileLocks[fType].RLock()
	defer a.fileLocks[fType].RUnlock()
	count = 0
	datSize = 0
	idxSize = 0
	a.files[fType].Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor != nil {
			count++
			datSize += item.decompressor.Size()
			count++
			idxSize += item.index.Size()
		}
		return true
	})
	return
}

type FilesStats struct {
	AccountsCount     int
	AccountsDatSize   int64
	AccountsIdxSize   int64
	CodeCount         int
	CodeDatSize       int64
	CodeIdxSize       int64
	StorageCount      int
	StorageDatSize    int64
	StorageIdxSize    int64
	CommitmentCount   int
	CommitmentDatSize int64
	CommitmentIdxSize int64
	Hits              uint64
	Misses            uint64
}

func (a *Aggregator) Stats() FilesStats {
	var fs FilesStats
	fs.AccountsCount, fs.AccountsDatSize, fs.AccountsIdxSize = a.stats(Account)
	fs.CodeCount, fs.CodeDatSize, fs.CodeIdxSize = a.stats(Code)
	fs.StorageCount, fs.StorageDatSize, fs.StorageIdxSize = a.stats(Storage)
	fs.CommitmentCount, fs.CommitmentDatSize, fs.CommitmentIdxSize = a.stats(Commitment)
	fs.Hits = atomic.LoadUint64(&a.fileHits)
	fs.Misses = atomic.LoadUint64(&a.fileMisses)
	return fs
}
