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

package compress

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

const ASSERT = false

// Compressor is the main operating type for performing per-word compression
// After creating a compression, one needs to add superstrings to it, using `AddWord` function
// In order to add word without compression, function `AddUncompressedWord` needs to be used
// Compressor only tracks which words are compressed and which are not until the compressed
// file is created. After that, the user of the file needs to know when to call
// `Next` or `NextUncompressed` function on the decompressor.
// After that, `Compress` function needs to be called to perform the compression
// and eventually create output file
type Compressor struct {
	uncompressedFile           *DecompressedFile
	outputFile, tmpOutFilePath string // File where to output the dictionary and compressed data
	tmpDir                     string // temporary directory to use for ETL when building dictionary
	workers                    int

	// Buffer for "superstring" - transformation of superstrings where each byte of a word, say b,
	// is turned into 2 bytes, 0x01 and b, and two zero bytes 0x00 0x00 are inserted after each word
	// this is needed for using ordinary (one string) suffix sorting algorithm instead of a generalised (many superstrings) suffix
	// sorting algorithm
	superstring      []byte
	superstrings     chan []byte
	wg               *sync.WaitGroup
	suffixCollectors []*etl.Collector
	wordsCount       uint64
	superstringCount uint64

	ctx       context.Context
	logPrefix string
	Ratio     CompressionRatio
	trace     bool
	lvl       log.Lvl
}

func NewCompressor(ctx context.Context, logPrefix, outputFile, tmpDir string, minPatternScore uint64, workers int, lvl log.Lvl) (*Compressor, error) {
	dir2.MustExist(tmpDir)
	dir, fileName := filepath.Split(outputFile)
	tmpOutFilePath := filepath.Join(dir, fileName) + ".tmp"
	// UncompressedFile - it's intermediate .idt file, outputFile it's final .seg (or .dat) file.
	// tmpOutFilePath - it's ".seg.tmp" (".idt.tmp") file which will be renamed to .seg file if everything succeed.
	// It allow atomically create .seg file (downloader will not see partially ready/ non-ready .seg files).
	// I didn't create ".seg.tmp" file in tmpDir, because I think tmpDir and snapsthoDir may be mounted to different drives
	uncompressedPath := filepath.Join(tmpDir, fileName) + ".idt"

	uncompressedFile, err := NewUncompressedFile(uncompressedPath)
	if err != nil {
		return nil, err
	}

	// Collector for dictionary superstrings (sorted by their score)
	superstrings := make(chan []byte, workers*2)
	wg := &sync.WaitGroup{}
	wg.Add(workers)
	suffixCollectors := make([]*etl.Collector, workers)
	for i := 0; i < workers; i++ {
		collector := etl.NewCollector(compressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		suffixCollectors[i] = collector
		go processSuperstring(superstrings, collector, minPatternScore, wg)
	}

	return &Compressor{
		uncompressedFile: uncompressedFile,
		tmpOutFilePath:   tmpOutFilePath,
		outputFile:       outputFile,
		tmpDir:           tmpDir,
		logPrefix:        logPrefix,
		workers:          workers,
		ctx:              ctx,
		superstrings:     superstrings,
		suffixCollectors: suffixCollectors,
		lvl:              lvl,
		wg:               wg,
	}, nil
}

func (c *Compressor) Close() {
	c.uncompressedFile.Close()
	for _, collector := range c.suffixCollectors {
		collector.Close()
	}
	c.suffixCollectors = nil
}

func (c *Compressor) SetTrace(trace bool) {
	c.trace = trace
}

func (c *Compressor) Count() int { return int(c.wordsCount) }

func (c *Compressor) AddWord(word []byte) error {
	c.wordsCount++

	if len(c.superstring)+2*len(word)+2 > superstringLimit {
		if c.superstringCount%4 == 0 {
			c.superstrings <- c.superstring
		}
		c.superstringCount++
		c.superstring = nil
	}
	for _, a := range word {
		c.superstring = append(c.superstring, 1, a)
	}
	c.superstring = append(c.superstring, 0, 0)

	return c.uncompressedFile.Append(word)
}

func (c *Compressor) AddUncompressedWord(word []byte) error {
	c.wordsCount++
	return c.uncompressedFile.AppendUncompressed(word)
}

func (c *Compressor) Compress() error {
	fmt.Printf("superstringCount: %d\n", c.superstringCount)
	c.uncompressedFile.w.Flush()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	if len(c.superstring) > 0 {
		c.superstrings <- c.superstring
	}
	close(c.superstrings)
	c.wg.Wait()

	db, err := DictionaryBuilderFromCollectors(c.ctx, compressLogPrefix, c.tmpDir, c.suffixCollectors)
	if err != nil {

		return err
	}
	if c.trace {
		_, fileName := filepath.Split(c.outputFile)
		if err := PersistDictrionary(filepath.Join(c.tmpDir, fileName)+".dictionary.txt", db); err != nil {
			return err
		}
	}

	defer os.Remove(c.tmpOutFilePath)
	if err := reducedict(c.ctx, c.trace, c.logPrefix, c.tmpOutFilePath, c.uncompressedFile, c.workers, db, c.lvl); err != nil {
		return err
	}

	if err := os.Rename(c.tmpOutFilePath, c.outputFile); err != nil {
		return fmt.Errorf("renaming: %w", err)
	}
	c.Ratio, err = Ratio(c.uncompressedFile.filePath, c.outputFile)
	if err != nil {
		return fmt.Errorf("ratio: %w", err)
	}

	return nil
}

// superstringLimit limits how large can one "superstring" get before it is processed
// CompressorSequential allocates 7 bytes for each uint of superstringLimit. For example,
// superstingLimit 16m will result in 112Mb being allocated for various arrays
const superstringLimit = 16 * 1024 * 1024

// minPatternLen is minimum length of pattern we consider to be included into the dictionary
const minPatternLen = 5
const maxPatternLen = 128

// maxDictPatterns is the maximum number of patterns allowed in the initial (not reduced dictionary)
// Large values increase memory consumption of dictionary reduction phase
const maxDictPatterns = 32 * 1024

// nolint
const compressLogPrefix = "compress"

type DictionaryBuilder struct {
	limit         int
	lastWord      []byte
	lastWordScore uint64
	items         []*Pattern
}

func (db *DictionaryBuilder) Reset(limit int) {
	db.limit = limit
	db.items = db.items[:0]
}

func (db *DictionaryBuilder) Len() int { return len(db.items) }
func (db *DictionaryBuilder) Less(i, j int) bool {
	if db.items[i].score == db.items[j].score {
		return bytes.Compare(db.items[i].word, db.items[j].word) < 0
	}
	return db.items[i].score < db.items[j].score
}

func dictionaryBuilderLess(i, j *Pattern) bool {
	if i.score == j.score {
		return bytes.Compare(i.word, j.word) < 0
	}
	return i.score < j.score
}

func (db *DictionaryBuilder) Swap(i, j int) {
	db.items[i], db.items[j] = db.items[j], db.items[i]
}
func (db *DictionaryBuilder) Sort() {
	slices.SortFunc(db.items, dictionaryBuilderLess)
	fmt.Printf("dict size: %d\n", len(db.items))
}

func (db *DictionaryBuilder) Push(x interface{}) {
	db.items = append(db.items, x.(*Pattern))
}

func (db *DictionaryBuilder) Pop() interface{} {
	old := db.items
	n := len(old)
	x := old[n-1]
	db.items = old[0 : n-1]
	return x
}

func (db *DictionaryBuilder) processWord(chars []byte, score uint64) {
	heap.Push(db, &Pattern{word: common.Copy(chars), score: score})
	if db.Len() > db.limit {
		// Remove the element with smallest score
		heap.Pop(db)
	}
}

func (db *DictionaryBuilder) loadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	score := binary.BigEndian.Uint64(v)
	if bytes.Equal(k, db.lastWord) {
		db.lastWordScore += score
	} else {
		if db.lastWord != nil {
			db.processWord(db.lastWord, db.lastWordScore)
		}
		db.lastWord = append(db.lastWord[:0], k...)
		db.lastWordScore = score
	}
	return nil
}

func (db *DictionaryBuilder) finish() {
	if db.lastWord != nil {
		db.processWord(db.lastWord, db.lastWordScore)
	}
}

func (db *DictionaryBuilder) ForEach(f func(score uint64, word []byte)) {
	for i := db.Len(); i > 0; i-- {
		f(db.items[i-1].score, db.items[i-1].word)
	}
}

func (db *DictionaryBuilder) Close() {
	db.items = nil
	db.lastWord = nil
}

// Pattern is representation of a pattern that is searched in the superstrings to compress them
// patterns are stored in a patricia tree and contain pattern score (calculated during
// the initial dictionary building), frequency of usage, and code
type Pattern struct {
	score    uint64 // Score assigned to the pattern during dictionary building
	uses     uint64 // How many times this pattern has been used during search and optimisation
	code     uint64 // Allocated numerical code
	codeBits int    // Number of bits in the code
	word     []byte // Pattern characters
	depth    int    // Depth of the pattern in the huffman tree (for encoding in the file)
}

// PatternList is a sorted list of pattern for the purpose of
// building Huffman tree to determine efficient coding.
// Patterns with least usage come first, we use numerical code
// as a tie breaker to make sure the resulting Huffman code is canonical
type PatternList []*Pattern

func (pl PatternList) Len() int { return len(pl) }
func patternListLess(i, j *Pattern) bool {
	if i.uses == j.uses {
		return bits.Reverse64(i.code) < bits.Reverse64(j.code)
	}
	return i.uses < j.uses
}

// PatternHuff is an intermediate node in a huffman tree of patterns
// It has two children, each of which may either be another intermediate node (h0 or h1)
// or leaf node, which is Pattern (p0 or p1).
type PatternHuff struct {
	uses       uint64
	tieBreaker uint64
	p0, p1     *Pattern
	h0, h1     *PatternHuff
}

func (h *PatternHuff) AddZero() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.codeBits++
	} else {
		h.h0.AddZero()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.codeBits++
	} else {
		h.h1.AddZero()
	}
}

func (h *PatternHuff) AddOne() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.code++
		h.p0.codeBits++
	} else {
		h.h0.AddOne()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.code++
		h.p1.codeBits++
	} else {
		h.h1.AddOne()
	}
}

func (h *PatternHuff) SetDepth(depth int) {
	if h.p0 != nil {
		h.p0.depth = depth + 1
		h.p0.uses = 0
	}
	if h.p1 != nil {
		h.p1.depth = depth + 1
		h.p1.uses = 0
	}
	if h.h0 != nil {
		h.h0.SetDepth(depth + 1)
	}
	if h.h1 != nil {
		h.h1.SetDepth(depth + 1)
	}
}

// PatternHeap is priority queue of pattern for the purpose of building
// Huffman tree to determine efficient coding. Patterns with least usage
// have highest priority. We use a tie-breaker to make sure
// the resulting Huffman code is canonical
type PatternHeap []*PatternHuff

func (ph PatternHeap) Len() int {
	return len(ph)
}

func (ph PatternHeap) Less(i, j int) bool {
	if ph[i].uses == ph[j].uses {
		return ph[i].tieBreaker < ph[j].tieBreaker
	}
	return ph[i].uses < ph[j].uses
}

func (ph *PatternHeap) Swap(i, j int) {
	(*ph)[i], (*ph)[j] = (*ph)[j], (*ph)[i]
}

func (ph *PatternHeap) Push(x interface{}) {
	*ph = append(*ph, x.(*PatternHuff))
}

func (ph *PatternHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	x := old[n-1]
	*ph = old[0 : n-1]
	return x
}

type Position struct {
	uses     uint64
	pos      uint64
	code     uint64
	codeBits int
	depth    int // Depth of the position in the huffman tree (for encoding in the file)
}

type PositionHuff struct {
	uses       uint64
	tieBreaker uint64
	p0, p1     *Position
	h0, h1     *PositionHuff
}

func (h *PositionHuff) AddZero() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.codeBits++
	} else {
		h.h0.AddZero()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.codeBits++
	} else {
		h.h1.AddZero()
	}
}

func (h *PositionHuff) AddOne() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.code++
		h.p0.codeBits++
	} else {
		h.h0.AddOne()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.code++
		h.p1.codeBits++
	} else {
		h.h1.AddOne()
	}
}

func (h *PositionHuff) SetDepth(depth int) {
	if h.p0 != nil {
		h.p0.depth = depth + 1
		h.p0.uses = 0
	}
	if h.p1 != nil {
		h.p1.depth = depth + 1
		h.p1.uses = 0
	}
	if h.h0 != nil {
		h.h0.SetDepth(depth + 1)
	}
	if h.h1 != nil {
		h.h1.SetDepth(depth + 1)
	}
}

type PositionList []*Position

func (pl PositionList) Len() int { return len(pl) }

func positionListLess(i, j *Position) bool {
	if i.uses == j.uses {
		return bits.Reverse64(i.code) < bits.Reverse64(j.code)
	}
	return i.uses < j.uses
}

type PositionHeap []*PositionHuff

func (ph PositionHeap) Len() int {
	return len(ph)
}

func (ph PositionHeap) Less(i, j int) bool {
	if ph[i].uses == ph[j].uses {
		return ph[i].tieBreaker < ph[j].tieBreaker
	}
	return ph[i].uses < ph[j].uses
}

func (ph *PositionHeap) Swap(i, j int) {
	(*ph)[i], (*ph)[j] = (*ph)[j], (*ph)[i]
}

func (ph *PositionHeap) Push(x interface{}) {
	*ph = append(*ph, x.(*PositionHuff))
}

func (ph *PositionHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	x := old[n-1]
	*ph = old[0 : n-1]
	return x
}

type HuffmanCoder struct {
	w          *bufio.Writer
	outputBits int
	outputByte byte
}

func (hf *HuffmanCoder) encode(code uint64, codeBits int) error {
	for codeBits > 0 {
		var bitsUsed int
		if hf.outputBits+codeBits > 8 {
			bitsUsed = 8 - hf.outputBits
		} else {
			bitsUsed = codeBits
		}
		mask := (uint64(1) << bitsUsed) - 1
		hf.outputByte |= byte((code & mask) << hf.outputBits)
		code >>= bitsUsed
		codeBits -= bitsUsed
		hf.outputBits += bitsUsed
		if hf.outputBits == 8 {
			if e := hf.w.WriteByte(hf.outputByte); e != nil {
				return e
			}
			hf.outputBits = 0
			hf.outputByte = 0
		}
	}
	return nil
}

func (hf *HuffmanCoder) flush() error {
	if hf.outputBits > 0 {
		if e := hf.w.WriteByte(hf.outputByte); e != nil {
			return e
		}
		hf.outputBits = 0
		hf.outputByte = 0
	}
	return nil
}

// DynamicCell represents result of dynamic programming for certain starting position
type DynamicCell struct {
	optimStart  int
	coverStart  int
	compression int
	score       uint64
	patternIdx  int // offset of the last element in the pattern slice
}

type Ring struct {
	cells             []DynamicCell
	head, tail, count int
}

func NewRing() *Ring {
	return &Ring{
		cells: make([]DynamicCell, 16),
		head:  0,
		tail:  0,
		count: 0,
	}
}

func (r *Ring) Reset() {
	r.count = 0
	r.head = 0
	r.tail = 0
}

func (r *Ring) ensureSize() {
	if r.count < len(r.cells) {
		return
	}
	newcells := make([]DynamicCell, r.count*2)
	if r.tail > r.head {
		copy(newcells, r.cells[r.head:r.tail])
	} else {
		n := copy(newcells, r.cells[r.head:])
		copy(newcells[n:], r.cells[:r.tail])
	}
	r.head = 0
	r.tail = r.count
	r.cells = newcells
}

func (r *Ring) PushFront() *DynamicCell {
	r.ensureSize()
	if r.head == 0 {
		r.head = len(r.cells)
	}
	r.head--
	r.count++
	return &r.cells[r.head]
}

func (r *Ring) PushBack() *DynamicCell {
	r.ensureSize()
	if r.tail == len(r.cells) {
		r.tail = 0
	}
	result := &r.cells[r.tail]
	r.tail++
	r.count++
	return result
}

func (r Ring) Len() int {
	return r.count
}

func (r *Ring) Get(i int) *DynamicCell {
	if i < 0 || i >= r.count {
		return nil
	}
	return &r.cells[(r.head+i)&(len(r.cells)-1)]
}

// Truncate removes all items starting from i
func (r *Ring) Truncate(i int) {
	r.count = i
	r.tail = (r.head + i) & (len(r.cells) - 1)
}

type DictAggregator struct {
	lastWord      []byte
	lastWordScore uint64
	collector     *etl.Collector

	dist map[int]int
}

func (da *DictAggregator) processWord(word []byte, score uint64) error {
	var scoreBuf [8]byte
	binary.BigEndian.PutUint64(scoreBuf[:], score)
	return da.collector.Collect(word, scoreBuf[:])
}

func (da *DictAggregator) Load(loadFunc etl.LoadFunc, args etl.TransformArgs) error {
	defer da.collector.Close()
	return da.collector.Load(nil, "", loadFunc, args)
}

func (da *DictAggregator) aggLoadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	if _, ok := da.dist[len(k)]; !ok {
		da.dist[len(k)] = 0
	}
	da.dist[len(k)]++

	score := binary.BigEndian.Uint64(v)
	if bytes.Equal(k, da.lastWord) {
		da.lastWordScore += score
	} else {
		if da.lastWord != nil {
			if err := da.processWord(da.lastWord, da.lastWordScore); err != nil {
				return err
			}
		}
		da.lastWord = append(da.lastWord[:0], k...)
		da.lastWordScore = score
	}
	return nil
}

func (da *DictAggregator) finish() error {
	if da.lastWord != nil {
		return da.processWord(da.lastWord, da.lastWordScore)
	}
	return nil
}

type CompressionRatio float64

func (r CompressionRatio) String() string { return fmt.Sprintf("%.2f", r) }

func Ratio(f1, f2 string) (CompressionRatio, error) {
	s1, err := os.Stat(f1)
	if err != nil {
		return 0, err
	}
	s2, err := os.Stat(f2)
	if err != nil {
		return 0, err
	}
	return CompressionRatio(float64(s1.Size()) / float64(s2.Size())), nil
}

// DecompressedFile - .dat file format - simple format for temporary data store
type DecompressedFile struct {
	filePath string
	f        *os.File
	w        *bufio.Writer
	count    uint64
	buf      []byte
}

func NewUncompressedFile(filePath string) (*DecompressedFile, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, 16*etl.BufIOSize)
	return &DecompressedFile{filePath: filePath, f: f, w: w, buf: make([]byte, 128)}, nil
}
func (f *DecompressedFile) Close() {
	f.w.Flush()
	//f.f.Sync()
	f.f.Close()
	os.Remove(f.filePath)
}
func (f *DecompressedFile) Append(v []byte) error {
	f.count++
	// For compressed words, the length prefix is shifted to make lowest bit zero
	n := binary.PutUvarint(f.buf, 2*uint64(len(v)))
	if _, e := f.w.Write(f.buf[:n]); e != nil {
		return e
	}
	if len(v) > 0 {
		if _, e := f.w.Write(v); e != nil {
			return e
		}
	}
	return nil
}
func (f *DecompressedFile) AppendUncompressed(v []byte) error {
	f.count++
	// For uncompressed words, the length prefix is shifted to make lowest bit one
	n := binary.PutUvarint(f.buf, 2*uint64(len(v))+1)
	if _, e := f.w.Write(f.buf[:n]); e != nil {
		return e
	}
	if len(v) > 0 {
		if _, e := f.w.Write(v); e != nil {
			return e
		}
	}
	return nil
}

// ForEach - Read keys from the file and generate superstring (with extra byte 0x1 prepended to each character, and with 0x0 0x0 pair inserted between keys and values)
// We only consider values with length > 2, because smaller values are not compressible without going into bits
func (f *DecompressedFile) ForEach(walker func(v []byte, compressed bool) error) error {
	_, err := f.f.Seek(0, 0)
	if err != nil {
		return err
	}
	r := bufio.NewReaderSize(f.f, 4*etl.BufIOSize)
	buf := make([]byte, 4096)
	l, e := binary.ReadUvarint(r)
	for ; e == nil; l, e = binary.ReadUvarint(r) {
		// extract lowest bit of length prefix as "uncompressed" flag and shift to obtain correct length
		compressed := (l & 1) == 0
		l >>= 1
		if len(buf) < int(l) {
			buf = make([]byte, l)
		}
		if _, e = io.ReadFull(r, buf[:l]); e != nil {
			return e
		}
		if err := walker(buf[:l], compressed); err != nil {
			return err
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	return nil
}
