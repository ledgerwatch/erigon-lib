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
	diffDir           string // Directory where the state diff files are stored
	byEndBlock        *btree.BTree
	unwindLimit       uint64        // How far the chain may unwind
	aggregationStep   uint64        // How many items (block, but later perhaps txs or changes) are required to form one state diff file
	changeFileNum     uint64        // Block number associated with the current change files. It is the last block number whose changes will go into that file
	accountChangeFile *os.File      // Currently open change file to append account changes to (or truncate when unwinding)
	accountChangeW    *bufio.Writer // Writer associated with the currently open accounts change file
	codeChangeFile    *os.File
	codeChangeW       *bufio.Writer
	storageChangeFile *os.File
	storageChangeW    *bufio.Writer
}

type byEndBlockItem struct {
	endBlock    uint64
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
		var item *byEndBlockItem = &byEndBlockItem{endBlock: endBlock}
		i := byEndBlock.Get(item)
		if i != nil {
			byEndBlock.ReplaceOrInsert(item)
		} else {
			item = i.(*byEndBlockItem)
		}
		var d *compress.Decompressor
		var idx *recsplit.Index
		switch subs[4] {
		case "dat":
			if d, err = compress.NewDecompressor(path.Join(diffDir, name)); err != nil {
				return nil, err
			}
		case "idx":
			if idx, err = recsplit.NewIndex(path.Join(diffDir, name)); err != nil {
				return nil, err
			}
		}
		switch subs[1] {
		case "accounts":
			if d != nil {
				item.accountsD = d
			} else {
				item.accountsIdx = idx
			}
		case "storage":
			if d != nil {
				item.storageD = d
			} else {
				item.storageIdx = idx
			}
		case "code":
			if d != nil {
				item.codeD = d
			} else {
				item.codeIdx = idx
			}
		}
	}
	a.byEndBlock = byEndBlock
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

func (a *Aggregator) closeChangeFile() error {
	if a.accountChangeW != nil {
		if err := a.accountChangeW.Flush(); err != nil {
			return err
		}
		if err := a.accountChangeFile.Close(); err != nil {
			return err
		}
	}
	if a.codeChangeW != nil {
		if err := a.codeChangeW.Flush(); err != nil {
			return err
		}
		if err := a.codeChangeFile.Close(); err != nil {
			return err
		}
	}
	if a.storageChangeW != nil {
		if err := a.storageChangeW.Flush(); err != nil {
			return err
		}
		if err := a.storageChangeFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (a *Aggregator) openChangeFiles(blockNum uint64) error {
	rem := (blockNum - 1) % a.aggregationStep
	startBlock := blockNum - rem
	endBlock := startBlock + a.aggregationStep - 1
	if a.accountChangeW == nil {
		path := path.Join(a.diffDir, fmt.Sprintf("accounts.%d-%d.chg", startBlock, endBlock))
		var err error
		if a.accountChangeFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755); err != nil {
			return err
		}
		a.accountChangeW = bufio.NewWriter(a.accountChangeFile)
	}
	if a.storageChangeW == nil {
		path := path.Join(a.diffDir, fmt.Sprintf("storage.%d-%d.chg", startBlock, endBlock))
		var err error
		if a.storageChangeFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755); err != nil {
			return err
		}
		a.storageChangeW = bufio.NewWriter(a.storageChangeFile)
	}
	if a.codeChangeW == nil {
		path := path.Join(a.diffDir, fmt.Sprintf("code.%d-%d.chg", startBlock, endBlock))
		var err error
		if a.codeChangeFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755); err != nil {
			return err
		}
		a.codeChangeW = bufio.NewWriter(a.codeChangeFile)
	}
	return nil
}

func (a *Aggregator) MakeStateWriter(tx kv.RwTx, blockNum uint64) (*Writer, error) {
	w := &Writer{
		a:        a,
		tx:       tx,
		blockNum: blockNum,
	}
	if blockNum > a.changeFileNum {
		if err := a.closeChangeFile(); err != nil {
			return nil, err
		}
	}
	if err := a.openChangeFiles(blockNum); err != nil {
		return nil, err
	}
	return w, nil
}

type Writer struct {
	a                 *Aggregator
	tx                kv.RwTx
	blockNum          uint64
	numBuf            [8]byte
	accountChangeSize uint64
	codeChangeSize    uint64
	storageChangeSize uint64
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
	// Update changes
	changeK := make([]byte, 8+len(addr))
	binary.BigEndian.PutUint64(changeK[:], w.blockNum)
	copy(changeK[8:], addr)
	n := binary.PutUvarint(w.numBuf[:], uint64(len(account)))
	changeV := make([]byte, len(account)+n+len(prevV)-4)
	copy(changeV, w.numBuf[:n])
	copy(changeV[n:], account)
	copy(changeV[n+len(account):], prevV[4:])
	if err = w.tx.Put(kv.ChangeAccounts, changeK, changeV); err != nil {
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
	// Update changes
	changeK := make([]byte, 8+len(addr))
	binary.BigEndian.PutUint64(changeK[:], w.blockNum)
	copy(changeK[8:], addr)
	n := binary.PutUvarint(w.numBuf[:], uint64(len(code)))
	changeV := make([]byte, len(code)+n+len(prevV)-4)
	copy(changeV, w.numBuf[:n])
	copy(changeV[n:], code)
	copy(changeV[n+len(code):], prevV[4:])
	if err = w.tx.Put(kv.ChangeCode, changeK, changeV); err != nil {
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
	// Update changes
	changeK := make([]byte, 8+len(addr))
	binary.BigEndian.PutUint64(changeK[:], w.blockNum)
	copy(changeK[8:], addr)
	n := binary.PutUvarint(w.numBuf[:], 0)
	changeV := make([]byte, n+len(prevV)-4)
	copy(changeV, w.numBuf[:n])
	copy(changeV[n:], prevV[4:])
	if err = w.tx.Put(kv.ChangeAccounts, changeK, changeV); err != nil {
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
	// Update changes
	changeK := make([]byte, 8+len(addr)+8+len(loc))
	binary.BigEndian.PutUint64(changeK[:], w.blockNum)
	copy(changeK[8:], dbkey)
	n := binary.PutUvarint(w.numBuf[:], uint64(vLen))
	changeV := make([]byte, vLen+n+len(prevV)-4)
	copy(changeV, w.numBuf[:n])
	copy(changeV[n:], v[4:])
	copy(changeV[n+vLen:], prevV[4:])
	if err = w.tx.Put(kv.ChangeAccounts, changeK, changeV); err != nil {
		return err
	}
	return nil
}

// Finish checks whether it is necessary to aggregate
// some of the changes into a file, and perform it if necessary
func (w *Writer) Finish() error {
	if w.blockNum <= w.a.unwindLimit+w.a.aggregationStep {
		return nil
	}
	diff := w.blockNum - w.a.unwindLimit
	if diff%w.a.aggregationStep != 0 {
		return nil
	}
	// Aggregate into a file
	return w.aggregateUpto(diff-w.a.aggregationStep, diff)
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
