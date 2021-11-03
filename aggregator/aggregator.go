/*
   Copyright 2021 Erigon contributors

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
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path"
	"regexp"
	"strconv"

	"github.com/google/btree"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
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

type Aggregator struct {
	diffDir         string // Directory where the state diff files are stored
	byEndBlock      *btree.BTree
	unwindLimit     uint64 // How far the chain may unwind
	aggregationStep uint64 // How many items (block, but later perhaps txs or changes) are required to form one state diff file
	changeFileNum   uint64 // Block number associated with the current change files. It is the last block number whose changes will go into that file
	accountChanges  Changes
	codeChanges     Changes
	storageChanges  Changes
	changesBtree    *btree.BTree // btree of ChangesItem
}

type ChangeFile struct {
	dir         string
	step        uint64
	namebase    string
	file        *os.File
	w           *bufio.Writer
	numBuf      [8]byte
	sizeCounter uint64
}

func (cf *ChangeFile) closeFile() error {
	if cf.w != nil {
		if err := cf.w.Flush(); err != nil {
			return err
		}
		if err := cf.file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (cf *ChangeFile) openFile(blockNum uint64) error {
	rem := (blockNum - 1) % cf.step
	startBlock := blockNum - rem
	endBlock := startBlock + cf.step - 1
	if cf.w == nil {
		path := path.Join(cf.dir, fmt.Sprintf("%s.%d-%d.chg", cf.namebase, startBlock, endBlock))
		var err error
		if cf.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755); err != nil {
			return err
		}
		cf.w = bufio.NewWriter(cf.file)
	}
	return nil
}

func (cf *ChangeFile) add(word []byte) error {
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
	return nil
}

func (cf *ChangeFile) finish(blockNum uint64) error {
	// Write out block number and then size of changes in this block
	binary.BigEndian.PutUint64(cf.numBuf[:], blockNum)
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

type Changes struct {
	namebase string
	keys     ChangeFile
	before   ChangeFile
	after    ChangeFile
	step     uint64
	dir      string
}

func (c *Changes) Init(namebase string, step uint64, dir string) {
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
}

func (c *Changes) closeFiles() error {
	if err := c.keys.closeFile(); err != nil {
		return err
	}
	if err := c.before.closeFile(); err != nil {
		return err
	}
	if err := c.after.closeFile(); err != nil {
		return err
	}
	return nil
}

func (c *Changes) openFiles(blockNum uint64) error {
	if err := c.keys.openFile(blockNum); err != nil {
		return err
	}
	if err := c.before.openFile(blockNum); err != nil {
		return err
	}
	if err := c.after.openFile(blockNum); err != nil {
		return err
	}
	return nil
}

func (c *Changes) addChange(key, before, after []byte) error {
	if err := c.keys.add(key); err != nil {
		return err
	}
	if err := c.before.add(before); err != nil {
		return err
	}
	if err := c.after.add(after); err != nil {
		return err
	}
	return nil
}

func (c *Changes) finish(blockNum uint64) error {
	if err := c.keys.finish(blockNum); err != nil {
		return err
	}
	if err := c.before.finish(blockNum); err != nil {
		return err
	}
	if err := c.after.finish(blockNum); err != nil {
		return err
	}
	return nil
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
	startBlock  uint64
	endBlock    uint64
	fileCount   int
	accountsD   *compress.Decompressor
	accountsIdx *recsplit.Index
	storageD    *compress.Decompressor
	storageIdx  *recsplit.Index
	codeD       *compress.Decompressor
	codeIdx     *recsplit.Index
}

func (i *byEndBlockItem) Less(than btree.Item) bool {
	return i.endBlock < than.(*byEndBlockItem).endBlock
}

func NewAggregator(diffDir string, unwindLimit uint64, aggregationStep uint64) (*Aggregator, error) {
	a := &Aggregator{
		diffDir:         diffDir,
		unwindLimit:     unwindLimit,
		aggregationStep: aggregationStep,
	}
	byEndBlock := btree.New(32)
	var closeBtree bool = true // It will be set to false in case of success at the end of the function
	defer func() {
		// Clean up all decompressor and indices upon error
		if closeBtree {
			closeFiles(byEndBlock)
		}
	}()
	// Scan the diff directory and create the mapping of end blocks to files
	// TODO: Larger files preferred over the small ones that overlap with them
	files, err := os.ReadDir(diffDir)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`(accounts|storage|code).([0-9]+)-([0-9]+).(dat|idx)`)
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 5 {
			log.Warn("File ignored by aggregator, more than 4 submatches", "name", name)
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
		var item *byEndBlockItem = &byEndBlockItem{fileCount: 1, startBlock: startBlock, endBlock: endBlock}
		i := byEndBlock.Get(item)
		if i == nil {
			byEndBlock.ReplaceOrInsert(item)
		} else if i.(*byEndBlockItem).startBlock > startBlock {
			byEndBlock.ReplaceOrInsert(item)
		} else if i.(*byEndBlockItem).startBlock == startBlock {
			item = i.(*byEndBlockItem)
			item.fileCount++
		}
	}
	// Check for overlaps and holes while moving items out of temporary btree
	a.byEndBlock = btree.New(32)
	var minStart uint64 = math.MaxUint64
	byEndBlock.Descend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.startBlock < minStart {
			if item.endBlock >= minStart {
				err = fmt.Errorf("overlap of state files [%d-%d] with %d", item.startBlock, item.endBlock, minStart)
				return false
			}
			if minStart != math.MaxUint64 && item.endBlock+1 != minStart {
				err = fmt.Errorf("hole in state files [%d-%d]", item.endBlock, minStart)
				return false
			}
			if item.fileCount != 6 {
				err = fmt.Errorf("missing state files for interval [%d-%d]", item.startBlock, item.endBlock)
				return false
			}
			minStart = item.startBlock
			a.byEndBlock.ReplaceOrInsert(i)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	a.byEndBlock.Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.accountsD, err = compress.NewDecompressor(path.Join(diffDir, fmt.Sprintf("accounts.%d-%d.dat", item.startBlock, item.endBlock))); err != nil {
			return false
		}
		if item.accountsIdx, err = recsplit.NewIndex(path.Join(diffDir, fmt.Sprintf("accounts.%d-%d.idx", item.startBlock, item.endBlock))); err != nil {
			return false
		}
		if item.codeD, err = compress.NewDecompressor(path.Join(diffDir, fmt.Sprintf("code.%d-%d.dat", item.startBlock, item.endBlock))); err != nil {
			return false
		}
		if item.codeIdx, err = recsplit.NewIndex(path.Join(diffDir, fmt.Sprintf("code.%d-%d.idx", item.startBlock, item.endBlock))); err != nil {
			return false
		}
		if item.storageD, err = compress.NewDecompressor(path.Join(diffDir, fmt.Sprintf("storage.%d-%d.dat", item.startBlock, item.endBlock))); err != nil {
			return false
		}
		if item.storageIdx, err = recsplit.NewIndex(path.Join(diffDir, fmt.Sprintf("storage.%d-%d.idx", item.startBlock, item.endBlock))); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	a.accountChanges.Init(diffDir, aggregationStep, "accounts")
	a.codeChanges.Init(diffDir, aggregationStep, "code")
	a.storageChanges.Init(diffDir, aggregationStep, "storage")
	a.changesBtree = btree.New(32)
	re = regexp.MustCompile(`(accounts|storage|code).(keys|before|after).([0-9]+)-([0-9]+).chg`)
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 5 {
			log.Warn("File ignored by changes scan, more than 4 submatches", "name", name)
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
		var item *ChangesItem = &ChangesItem{fileCount: 1, startBlock: startBlock, endBlock: endBlock}
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
	minStart = math.MaxUint64
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
			if item.fileCount != 9 {
				err = fmt.Errorf("missing change files for interval [%d-%d]", item.startBlock, item.endBlock)
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
	if lastStateI := a.byEndBlock.Max(); lastStateI != nil {
		item := lastStateI.(*byEndBlockItem)
		if minStart != math.MaxUint64 && item.endBlock+1 != minStart {
			return nil, fmt.Errorf("hole or overlap between state files and change files [%d-%d]", item.endBlock, minStart)
		}
	}
	return a, nil
}

func closeFiles(byEndBlock *btree.BTree) {
	byEndBlock.Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.accountsD != nil {
			item.accountsD.Close()
		}
		if item.accountsIdx != nil {
			item.accountsIdx.Close()
		}
		if item.storageD != nil {
			item.storageD.Close()
		}
		if item.storageIdx != nil {
			item.storageIdx.Close()
		}
		if item.codeD != nil {
			item.codeD.Close()
		}
		if item.codeIdx != nil {
			item.codeIdx.Close()
		}
		return true
	})
}

func (a *Aggregator) Close() {
	closeFiles(a.byEndBlock)
}

func (a *Aggregator) MakeStateReader(tx kv.Getter, blockNum uint64) *Reader {
	r := &Reader{
		a:        a,
		tx:       tx,
		blockNum: blockNum,
	}
	return r
}

type Reader struct {
	a        *Aggregator
	tx       kv.Getter
	blockNum uint64
}

func (r *Reader) ReadAccountData(addr []byte) ([]byte, error) {
	// Look in the summary table first
	v, err := r.tx.GetOne(kv.StateAccounts, addr)
	if err != nil {
		return nil, err
	}
	if v != nil {
		// First 4 bytes is the number of 1-block state diffs containing the key
		return v[4:], nil
	}
	// Look in the files
	var val []byte
	r.a.byEndBlock.DescendLessOrEqual(&byEndBlockItem{endBlock: r.blockNum}, func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		offset := item.accountsIdx.Lookup(addr)
		g := item.accountsD.MakeGetter() // TODO Cache in the reader
		g.Reset(offset)
		if g.HasNext() {
			key, _ := g.Next(nil) // Add special function that just checks the key
			if bytes.Equal(key, addr) {
				val, _ = g.Next(nil)
				return false
			}
		}
		return true
	})
	return val, nil
}

func (r *Reader) ReadAccountStorage(addr []byte, incarnation uint64, loc []byte) ([]byte, error) {
	// Look in the summary table first
	dbkey := make([]byte, len(addr)+8+len(loc))
	copy(dbkey[0:], addr)
	binary.BigEndian.PutUint64(dbkey[len(addr):], incarnation)
	copy(dbkey[len(addr)+8:], loc)
	v, err := r.tx.GetOne(kv.StateStorage, dbkey)
	if err != nil {
		return nil, err
	}
	if v != nil {
		// First 4 bytes is the number of 1-block state diffs containing the key
		return v[4:], nil
	}
	// Look in the files
	filekey := make([]byte, len(addr)+len(loc))
	copy(filekey[0:], addr)
	copy(filekey[len(addr):], loc)
	var val []byte
	r.a.byEndBlock.DescendLessOrEqual(&byEndBlockItem{endBlock: r.blockNum}, func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		offset := item.storageIdx.Lookup(filekey)
		g := item.storageD.MakeGetter() // TODO Cache in the reader
		g.Reset(offset)
		if g.HasNext() {
			key, _ := g.Next(nil) // Add special function that just checks the key
			if bytes.Equal(key, filekey) {
				val, _ = g.Next(nil)
				return false
			}
		}
		return true
	})
	return val, nil
}

func (r *Reader) ReadAccountCode(addr []byte, incarnation uint64) ([]byte, error) {
	// Look in the summary table first
	v, err := r.tx.GetOne(kv.StateCode, addr)
	if err != nil {
		return nil, err
	}
	if v != nil {
		// First 4 bytes is the number of 1-block state diffs containing the key
		return v[4:], nil
	}
	// Look in the files
	var val []byte
	r.a.byEndBlock.DescendLessOrEqual(&byEndBlockItem{endBlock: r.blockNum}, func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		offset := item.codeIdx.Lookup(addr)
		g := item.codeD.MakeGetter() // TODO Cache in the reader
		g.Reset(offset)
		if g.HasNext() {
			key, _ := g.Next(nil) // Add special function that just checks the key
			if bytes.Equal(key, addr) {
				val, _ = g.Next(nil)
				return false
			}
		}
		return true
	})
	return val, nil
}

func (r *Reader) ReadAccountIncarnation(addr []byte) uint64 {
	return r.blockNum - 1
}

type Writer struct {
	a        *Aggregator
	tx       kv.RwTx
	blockNum uint64
}

func (a *Aggregator) MakeStateWriter(tx kv.RwTx, blockNum uint64) (*Writer, error) {
	w := &Writer{
		a:        a,
		tx:       tx,
		blockNum: blockNum,
	}
	if blockNum > a.changeFileNum {
		if err := a.accountChanges.closeFiles(); err != nil {
			return nil, err
		}
		if err := a.codeChanges.closeFiles(); err != nil {
			return nil, err
		}
		if err := a.storageChanges.closeFiles(); err != nil {
			return nil, err
		}
	}
	if err := a.accountChanges.openFiles(blockNum); err != nil {
		return nil, err
	}
	if err := a.codeChanges.openFiles(blockNum); err != nil {
		return nil, err
	}
	if err := a.codeChanges.openFiles(blockNum); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) Finish() error {
	if err := w.a.accountChanges.finish(w.blockNum); err != nil {
		return err
	}
	if err := w.a.codeChanges.finish(w.blockNum); err != nil {
		return err
	}
	if err := w.a.storageChanges.finish(w.blockNum); err != nil {
		return err
	}
	if w.blockNum <= w.a.unwindLimit+w.a.aggregationStep {
		return nil
	}
	diff := w.blockNum - w.a.unwindLimit
	if diff%w.a.aggregationStep != 0 {
		return nil
	}
	return w.aggregateUpto(diff-w.a.aggregationStep, diff)
}

func (w *Writer) UpdateAccountData(addr []byte, account []byte) error {
	prevV, err := w.tx.GetOne(kv.StateAccounts, addr)
	if err != nil {
		return err
	}
	var prevNum uint32
	if prevV != nil {
		prevNum = binary.BigEndian.Uint32(prevV[:4])
	}
	v := make([]byte, 4+len(account))
	binary.BigEndian.PutUint32(v[:4], prevNum+1)
	copy(v[4:], account)
	if err = w.tx.Put(kv.StateAccounts, addr, v); err != nil {
		return err
	}
	if err = w.a.accountChanges.addChange(addr, prevV[4:], account); err != nil {
		return err
	}
	return nil
}

func (w *Writer) UpdateAccountCode(addr []byte, code []byte) error {
	prevV, err := w.tx.GetOne(kv.StateCode, addr)
	if err != nil {
		return err
	}
	var prevNum uint32
	if prevV != nil {
		prevNum = binary.BigEndian.Uint32(prevV[:4])
	}
	v := make([]byte, 4+len(code))
	binary.BigEndian.PutUint32(v[:4], prevNum+1)
	copy(v[4:], code)
	if err = w.tx.Put(kv.StateCode, addr, v); err != nil {
		return err
	}
	if err = w.a.codeChanges.addChange(addr, prevV[4:], code); err != nil {
		return err
	}
	return nil
}

func (w *Writer) DeleteAccount(addr []byte) error {
	prevV, err := w.tx.GetOne(kv.StateAccounts, addr)
	if err != nil {
		return err
	}
	var prevNum uint32
	if prevV != nil {
		prevNum = binary.BigEndian.Uint32(prevV[:4])
	}
	v := make([]byte, 4)
	binary.BigEndian.PutUint32(v[:4], prevNum+1)
	if err = w.tx.Put(kv.StateAccounts, addr, v); err != nil {
		return err
	}
	if err = w.a.accountChanges.addChange(addr, prevV[4:], nil); err != nil {
		return err
	}
	return nil
}

func (w *Writer) WriteAccountStorage(addr []byte, incarnation uint64, loc []byte, original, value *uint256.Int) error {
	dbkey := make([]byte, len(addr)+8+len(loc))
	copy(dbkey[0:], addr)
	binary.BigEndian.PutUint64(dbkey[len(addr):], incarnation)
	copy(dbkey[len(addr)+8:], loc)
	prevV, err := w.tx.GetOne(kv.StateStorage, dbkey)
	if err != nil {
		return err
	}
	var prevNum uint32
	if prevV != nil {
		prevNum = binary.BigEndian.Uint32(prevV[:4])
	}
	vLen := value.ByteLen()
	v := make([]byte, 4+vLen)
	binary.BigEndian.PutUint32(v[:4], prevNum+1)
	value.WriteToSlice(v[4:])
	if err = w.tx.Put(kv.StateStorage, addr, v); err != nil {
		return err
	}
	if err = w.a.storageChanges.addChange(dbkey, prevV[4:], v[4:]); err != nil {
		return err
	}
	return nil
}

type AggregateItem struct {
	k, v []byte
}

func (i *AggregateItem) Less(than btree.Item) bool {
	return bytes.Compare(i.k, than.(*AggregateItem).k) < 0
}

func (w *Writer) aggregateUpto(blockFrom, blockTo uint64) error {
	return nil
}
