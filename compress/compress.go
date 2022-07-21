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
	"io/ioutil"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/flanglet/kanzi-go/transform"
	"github.com/ledgerwatch/erigon-lib/common"
	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/patricia"
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
	ext := filepath.Ext(fileName)
	// UncompressedFile - it's intermediate .idt file, outputFile it's final .seg (or .dat) file.
	// tmpOutFilePath - it's ".seg.tmp" (".idt.tmp") file which will be renamed to .seg file if everything succeed.
	// It allow atomically create .seg file (downloader will not see partially ready/ non-ready .seg files).
	// I didn't create ".seg.tmp" file in tmpDir, because I think tmpDir and snapsthoDir may be mounted to different drives
	uncompressedPath := filepath.Join(tmpDir, fileName[:len(fileName)-len(ext)]) + ".idt"

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
		collector := etl.NewCollector(compressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize/2))
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
		c.superstrings <- c.superstring
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

type CompressorSequential struct {
	outputFile      string // File where to output the dictionary and compressed data
	tmpDir          string // temporary directory to use for ETL when building dictionary
	minPatternScore uint64 //minimum score (per superstring) required to consider including pattern into the dictionary
	// Buffer for "superstring" - transformation of superstrings where each byte of a word, say b,
	// is turned into 2 bytes, 0x01 and b, and two zero bytes 0x00 0x00 are inserted after each word
	// this is needed for using ordinary (one string) suffix sorting algorithm instead of a generalised (many superstrings) suffix
	// sorting algorithm
	superstring []byte
	divsufsort  *transform.DivSufSort       // Instance of DivSufSort - algorithm for building suffix array for the superstring
	suffixarray []int32                     // Suffix array - output for divsufsort algorithm
	lcp         []int32                     // LCP array (Longest Common Prefix)
	collector   *etl.Collector              // Collector used to handle very large sets of superstrings
	numBuf      [binary.MaxVarintLen64]byte // Buffer for producing var int serialisation
	collectBuf  []byte                      // Buffer for forming key to call collector
	dictBuilder DictionaryBuilder           // Priority queue that selects dictionary patterns with highest scores, and then sorts them by scores
	pt          patricia.PatriciaTree       // Patricia tree of dictionary patterns
	mf          patricia.MatchFinder        // Match finder to use together with patricia tree (it stores search context and buffers matches)
	ring        *Ring                       // Cycling ring for dynamic programming algorithm determining optimal coverage of word by dictionary patterns
	wordFile    *os.File                    // Temporary file to keep superstrings in for the second pass
	wordW       *bufio.Writer               // Bufferred writer for temporary file
	interFile   *os.File                    // File to write intermediate compression to
	interW      *bufio.Writer               // Buffered writer associate to interFile
	patterns    []int                       // Buffer of pattern ids (used in the dynamic programming algorithm to remember patterns corresponding to dynamic cells)
	uncovered   []int                       // Buffer of intervals that are not covered by patterns
	posMap      map[uint64]uint64           // Counter of use for each position within compressed word (for building huffman code for positions)

	wordsCount, emptyWordsCount uint64
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
const maxDictPatterns = 1 * 1024 * 1024

//nolint
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
func (db *DictionaryBuilder) Sort() { slices.SortFunc(db.items, dictionaryBuilderLess) }

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

func NewCompressorSequential(logPrefix, outputFile string, tmpDir string, minPatternScore uint64) (*CompressorSequential, error) {
	c := &CompressorSequential{
		minPatternScore: minPatternScore,
		outputFile:      outputFile,
		tmpDir:          tmpDir,
		superstring:     make([]byte, 0, superstringLimit), // Allocate enough, so we never need to resize
		suffixarray:     make([]int32, superstringLimit),
		lcp:             make([]int32, superstringLimit/2),
		collectBuf:      make([]byte, 8, 256),
		ring:            NewRing(),
		patterns:        make([]int, 0, 32),
		uncovered:       make([]int, 0, 32),
		posMap:          make(map[uint64]uint64),
	}
	var err error
	if c.divsufsort, err = transform.NewDivSufSort(); err != nil {
		return nil, err
	}
	if c.wordFile, err = ioutil.TempFile(c.tmpDir, "superstrings-"); err != nil {
		return nil, err
	}
	c.wordW = bufio.NewWriterSize(c.wordFile, etl.BufIOSize)
	c.collector = etl.NewCollector(logPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize/2))
	return c, nil
}

// AddWord needs to be called repeatedly to provide all the superstrings to compress
func (c *CompressorSequential) AddWord(word []byte) error {
	c.wordsCount++
	if len(word) == 0 {
		c.emptyWordsCount++
	}
	if len(c.superstring)+2*len(word)+2 > superstringLimit {
		// Adding this word would make superstring go over the limit
		if err := c.processSuperstring(); err != nil {
			return fmt.Errorf("buildDictNextWord: error processing superstring: %w", err)
		}
	}
	for _, b := range word {
		c.superstring = append(c.superstring, 1, b)
	}
	c.superstring = append(c.superstring, 0, 0)
	n := binary.PutUvarint(c.numBuf[:], uint64(len(word)))
	if _, err := c.wordW.Write(c.numBuf[:n]); err != nil {
		return err
	}
	if len(word) > 0 {
		if _, err := c.wordW.Write(word); err != nil {
			return err
		}
	}
	return nil
}

func (c *CompressorSequential) Compress() error {
	if c.wordW != nil {
		if err := c.wordW.Flush(); err != nil {
			return err
		}
	}
	if err := c.buildDictionary(); err != nil {
		return err
	}
	if err := c.findMatches(); err != nil {
		return err
	}
	if err := c.optimiseCodes(); err != nil {
		return err
	}
	return nil
}

func (c *CompressorSequential) Close() {
	c.collector.Close()
	c.wordFile.Close()
	c.interFile.Close()
}

func (c *CompressorSequential) findMatches() error {
	// Build patricia tree out of the patterns in the dictionary, for further matching in individual superstrings
	// Allocate temporary initial codes to the patterns so that patterns with higher scores get smaller code
	// This helps reduce the size of intermediate compression
	for i, p := range c.dictBuilder.items {
		p.code = uint64(len(c.dictBuilder.items) - i - 1)
		c.pt.Insert(p.word, p)
	}
	var err error
	if c.interFile, err = ioutil.TempFile(c.tmpDir, "inter-compress-"); err != nil {
		return err
	}
	c.interW = bufio.NewWriterSize(c.interFile, etl.BufIOSize)
	if _, err := c.wordFile.Seek(0, 0); err != nil {
		return err
	}
	defer os.Remove(c.wordFile.Name())
	defer c.wordFile.Close()
	r := bufio.NewReaderSize(c.wordFile, etl.BufIOSize)
	var readBuf []byte
	l, e := binary.ReadUvarint(r)
	for ; e == nil; l, e = binary.ReadUvarint(r) {
		c.posMap[l+1]++
		c.posMap[0]++
		if int(l) > len(readBuf) {
			readBuf = make([]byte, l)
		}
		if _, e := io.ReadFull(r, readBuf[:l]); e != nil {
			return e
		}
		word := readBuf[:l]
		// Encode length of the word as var int for the intermediate compression
		n := binary.PutUvarint(c.numBuf[:], uint64(len(word)))
		if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
			return err
		}
		if len(word) > 0 {
			matches := c.mf.FindLongestMatches(word)
			if len(matches) == 0 {
				n = binary.PutUvarint(c.numBuf[:], 0)
				if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
					return err
				}
				if _, err := c.interW.Write(word); err != nil {
					return err
				}
				continue
			}
			c.ring.Reset()
			c.patterns = append(c.patterns[:0], 0, 0) // Sentinel entry - no meaning
			lastF := matches[len(matches)-1]
			for j := lastF.Start; j < lastF.End; j++ {
				d := c.ring.PushBack()
				d.optimStart = j + 1
				d.coverStart = len(word)
				d.compression = 0
				d.patternIdx = 0
				d.score = 0
			}
			// Starting from the last match
			for i := len(matches); i > 0; i-- {
				f := matches[i-1]
				p := f.Val.(*Pattern)
				firstCell := c.ring.Get(0)
				maxCompression := firstCell.compression
				maxScore := firstCell.score
				maxCell := firstCell
				var maxInclude bool
				for e := 0; e < c.ring.Len(); e++ {
					cell := c.ring.Get(e)
					comp := cell.compression - 4
					if cell.coverStart >= f.End {
						comp += f.End - f.Start
					} else {
						comp += cell.coverStart - f.Start
					}
					score := cell.score + p.score
					if comp > maxCompression || (comp == maxCompression && score > maxScore) {
						maxCompression = comp
						maxScore = score
						maxInclude = true
						maxCell = cell
					} else if cell.optimStart > f.End {
						c.ring.Truncate(e)
						break
					}
				}
				d := c.ring.PushFront()
				d.optimStart = f.Start
				d.score = maxScore
				d.compression = maxCompression
				if maxInclude {
					d.coverStart = f.Start
					d.patternIdx = len(c.patterns)
					c.patterns = append(c.patterns, i-1, maxCell.patternIdx)
				} else {
					d.coverStart = maxCell.coverStart
					d.patternIdx = maxCell.patternIdx
				}
			}
			optimCell := c.ring.Get(0)
			// Count number of patterns
			var patternCount uint64
			patternIdx := optimCell.patternIdx
			for patternIdx != 0 {
				patternCount++
				patternIdx = c.patterns[patternIdx+1]
			}
			n = binary.PutUvarint(c.numBuf[:], patternCount)
			if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
				return err
			}
			patternIdx = optimCell.patternIdx
			lastStart := 0
			var lastUncovered int
			c.uncovered = c.uncovered[:0]
			for patternIdx != 0 {
				pattern := c.patterns[patternIdx]
				p := matches[pattern].Val.(*Pattern)
				if matches[pattern].Start > lastUncovered {
					c.uncovered = append(c.uncovered, lastUncovered, matches[pattern].Start)
				}
				lastUncovered = matches[pattern].End
				// Starting position
				c.posMap[uint64(matches[pattern].Start-lastStart+1)]++
				lastStart = matches[pattern].Start
				n = binary.PutUvarint(c.numBuf[:], uint64(matches[pattern].Start))
				if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
					return err
				}
				// Code
				n = binary.PutUvarint(c.numBuf[:], p.code)
				if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
					return err
				}
				p.uses++
				patternIdx = c.patterns[patternIdx+1]
			}
			if len(word) > lastUncovered {
				c.uncovered = append(c.uncovered, lastUncovered, len(word))
			}
			// Add uncoded input
			for i := 0; i < len(c.uncovered); i += 2 {
				if _, err := c.interW.Write(word[c.uncovered[i]:c.uncovered[i+1]]); err != nil {
					return err
				}
			}
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	if err = c.interW.Flush(); err != nil {
		return err
	}
	return nil
}

// optimises coding for patterns and positions
func (c *CompressorSequential) optimiseCodes() error {
	if _, err := c.interFile.Seek(0, 0); err != nil {
		return err
	}
	defer os.Remove(c.interFile.Name())
	defer c.interFile.Close()
	// Select patterns with non-zero use and sort them by increasing frequency of use (in preparation for building Huffman tree)
	var patternList PatternList
	for _, p := range c.dictBuilder.items {
		if p.uses > 0 {
			patternList = append(patternList, p)
		}
	}
	slices.SortFunc[*Pattern](patternList, patternListLess)

	i := 0 // Will be going over the patternList
	// Build Huffman tree for codes
	var codeHeap PatternHeap
	heap.Init(&codeHeap)
	tieBreaker := uint64(0)
	for codeHeap.Len()+(patternList.Len()-i) > 1 {
		// New node
		h := &PatternHuff{
			tieBreaker: tieBreaker,
		}
		if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
			// Take h0 from the heap
			h.h0 = heap.Pop(&codeHeap).(*PatternHuff)
			h.h0.AddZero()
			h.uses += h.h0.uses
		} else {
			// Take p0 from the list
			h.p0 = patternList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			i++
		}
		if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&codeHeap).(*PatternHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
		} else {
			// Take p1 from the list
			h.p1 = patternList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			i++
		}
		tieBreaker++
		heap.Push(&codeHeap, h)
	}
	if codeHeap.Len() > 0 {
		root := heap.Pop(&codeHeap).(*PatternHuff) // Root node of huffman tree
		root.SetDepth(0)
	}
	// Calculate total size of the dictionary
	var patternsSize uint64
	for _, p := range patternList {
		ns := binary.PutUvarint(c.numBuf[:], uint64(p.depth))    // Length of the word's depth
		n := binary.PutUvarint(c.numBuf[:], uint64(len(p.word))) // Length of the word's length
		patternsSize += uint64(ns + n + len(p.word))
	}
	// Start writing to result file
	cf, err := os.Create(c.outputFile)
	if err != nil {
		return err
	}
	defer cf.Close()
	defer cf.Sync()
	cw := bufio.NewWriterSize(cf, etl.BufIOSize)
	defer cw.Flush()
	// 1-st, output amount of words and emptyWords in file
	binary.BigEndian.PutUint64(c.numBuf[:], c.wordsCount)
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(c.numBuf[:], c.emptyWordsCount)
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	// 2-nd, output dictionary size
	binary.BigEndian.PutUint64(c.numBuf[:], patternsSize) // Dictionary size
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	// 3-rd, write all the pattens, with their depths
	slices.SortFunc[*Pattern](patternList, patternListLess)
	for _, p := range patternList {
		ns := binary.PutUvarint(c.numBuf[:], uint64(p.depth))
		if _, err = cw.Write(c.numBuf[:ns]); err != nil {
			return err
		}
		n := binary.PutUvarint(c.numBuf[:], uint64(len(p.word)))
		if _, err = cw.Write(c.numBuf[:n]); err != nil {
			return err
		}
		if _, err = cw.Write(p.word); err != nil {
			return err
		}
		//fmt.Printf("[comp] depth=%d, code=[%b], pattern=[%x]\n", p.depth, p.code, p.word)
	}
	var positionList PositionList
	pos2code := make(map[uint64]*Position)
	for pos, uses := range c.posMap {
		p := &Position{pos: pos, uses: uses, code: pos, codeBits: 0}
		positionList = append(positionList, p)
		pos2code[pos] = p
	}
	slices.SortFunc(positionList, positionListLess)
	i = 0 // Will be going over the positionList
	// Build Huffman tree for codes
	var posHeap PositionHeap
	heap.Init(&posHeap)
	tieBreaker = uint64(0)
	for posHeap.Len()+(positionList.Len()-i) > 1 {
		// New node
		h := &PositionHuff{
			tieBreaker: tieBreaker,
		}
		if posHeap.Len() > 0 && (i >= positionList.Len() || posHeap[0].uses < positionList[i].uses) {
			// Take h0 from the heap
			h.h0 = heap.Pop(&posHeap).(*PositionHuff)
			h.h0.AddZero()
			h.uses += h.h0.uses
		} else {
			// Take p0 from the list
			h.p0 = positionList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			i++
		}
		if posHeap.Len() > 0 && (i >= positionList.Len() || posHeap[0].uses < positionList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&posHeap).(*PositionHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
		} else {
			// Take p1 from the list
			h.p1 = positionList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			i++
		}
		tieBreaker++
		heap.Push(&posHeap, h)
	}
	if posHeap.Len() > 0 {
		posRoot := heap.Pop(&posHeap).(*PositionHuff)
		posRoot.SetDepth(0)
	}
	// Calculate the size of pos dictionary
	var posSize uint64
	for _, p := range positionList {
		ns := binary.PutUvarint(c.numBuf[:], uint64(p.depth)) // Length of the position's depth
		n := binary.PutUvarint(c.numBuf[:], p.pos)
		posSize += uint64(ns + n)
	}
	// First, output dictionary size
	binary.BigEndian.PutUint64(c.numBuf[:], posSize) // Dictionary size
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	slices.SortFunc(positionList, positionListLess)
	// Write all the positions and their depths
	for _, p := range positionList {
		ns := binary.PutUvarint(c.numBuf[:], uint64(p.depth))
		if _, err = cw.Write(c.numBuf[:ns]); err != nil {
			return err
		}
		n := binary.PutUvarint(c.numBuf[:], p.pos)
		if _, err = cw.Write(c.numBuf[:n]); err != nil {
			return err
		}
	}
	r := bufio.NewReaderSize(c.interFile, etl.BufIOSize)
	var hc HuffmanCoder
	hc.w = cw
	l, e := binary.ReadUvarint(r)
	for ; e == nil; l, e = binary.ReadUvarint(r) {
		posCode := pos2code[l+1]
		if posCode != nil {
			if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
				return e
			}
		}
		if l == 0 {
			if e = hc.flush(); e != nil {
				return e
			}
		} else {
			var pNum uint64 // Number of patterns
			if pNum, e = binary.ReadUvarint(r); e != nil {
				return e
			}
			// Now reading patterns one by one
			var lastPos uint64
			var lastUncovered int
			var uncoveredCount int
			for i := 0; i < int(pNum); i++ {
				var pos uint64 // Starting position for pattern
				if pos, e = binary.ReadUvarint(r); e != nil {
					return e
				}
				posCode = pos2code[pos-lastPos+1]
				lastPos = pos
				if posCode != nil {
					if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
						return e
					}
				}
				var code uint64 // Code of the pattern
				if code, e = binary.ReadUvarint(r); e != nil {
					return e
				}
				patternCode := c.dictBuilder.items[len(c.dictBuilder.items)-1-int(code)]
				if int(pos) > lastUncovered {
					uncoveredCount += int(pos) - lastUncovered
				}
				lastUncovered = int(pos) + len(patternCode.word)
				if e = hc.encode(patternCode.code, patternCode.codeBits); e != nil {
					return e
				}
			}
			if int(l) > lastUncovered {
				uncoveredCount += int(l) - lastUncovered
			}
			// Terminating position and flush
			posCode = pos2code[0]
			if posCode != nil {
				if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
					return e
				}
			}
			if e = hc.flush(); e != nil {
				return e
			}
			// Copy uncovered characters
			if uncoveredCount > 0 {
				if _, e = io.CopyN(cw, r, int64(uncoveredCount)); e != nil {
					return e
				}
			}
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	return nil
}

func (c *CompressorSequential) buildDictionary() error {
	if len(c.superstring) > 0 {
		// Process any residual superstrings
		if err := c.processSuperstring(); err != nil {
			return fmt.Errorf("buildDictionary: error processing superstring: %w", err)
		}
	}
	c.dictBuilder.Reset(maxDictPatterns)
	if err := c.collector.Load(nil, "", c.dictBuilder.loadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	c.dictBuilder.finish()
	c.collector.Close()
	// Sort dictionary inside the dictionary bilder in the order of increasing scores
	(&c.dictBuilder).Sort()
	return nil
}

func (c *CompressorSequential) processSuperstring() error {
	c.divsufsort.ComputeSuffixArray(c.superstring, c.suffixarray[:len(c.superstring)])
	// filter out suffixes that start with odd positions - we reuse the first half of sa.suffixarray for that
	// because it won't be used after filtration
	n := len(c.superstring) / 2
	saFiltered := c.suffixarray[:n]
	j := 0
	for _, s := range c.suffixarray[:len(c.superstring)] {
		if (s & 1) == 0 {
			saFiltered[j] = s >> 1
			j++
		}
	}
	// Now create an inverted array - we reuse the second half of sa.suffixarray for that
	saInverted := c.suffixarray[:n]
	for i := 0; i < n; i++ {
		saInverted[saFiltered[i]] = int32(i)
	}
	// Create LCP array (Kasai's algorithm)
	var k int
	// Process all suffixes one by one starting from
	// first suffix in superstring
	for i := 0; i < n; i++ {
		/* If the current suffix is at n-1, then we don’t
		   have next substring to consider. So lcp is not
		   defined for this substring, we put zero. */
		if saInverted[i] == int32(n-1) {
			k = 0
			continue
		}

		/* j contains index of the next substring to
		   be considered  to compare with the present
		   substring, i.e., next string in suffix array */
		j := int(saFiltered[saInverted[i]+1])

		// Directly start matching from k'th index as
		// at-least k-1 characters will match
		for i+k < n && j+k < n && c.superstring[(i+k)*2] != 0 && c.superstring[(j+k)*2] != 0 && c.superstring[(i+k)*2+1] == c.superstring[(j+k)*2+1] {
			k++
		}

		c.lcp[saInverted[i]] = int32(k) // lcp for the present suffix.

		// Deleting the starting character from the string.
		if k > 0 {
			k--
		}
	}
	// Walk over LCP array and compute the scores of the strings
	b := saInverted
	j = 0
	for i := 0; i < n-1; i++ {
		// Only when there is a drop in LCP value
		if c.lcp[i+1] >= c.lcp[i] {
			j = i
			continue
		}
		for l := c.lcp[i]; l > c.lcp[i+1]; l-- {
			if l < minPatternLen || l > maxPatternLen {
				continue
			}
			// Go back
			var isNew bool
			for j > 0 && c.lcp[j-1] >= l {
				j--
				isNew = true
			}
			if !isNew {
				break
			}
			window := i - j + 2
			copy(b, saFiltered[j:i+2])
			sort.Slice(b[:window], func(i1, i2 int) bool { return b[i1] < b[i2] })
			repeats := 1
			lastK := 0
			for k := 1; k < window; k++ {
				if b[k] >= b[lastK]+l {
					repeats++
					lastK = k
				}
			}
			score := uint64(repeats * int(l-4))
			if score >= c.minPatternScore {
				// Dictionary key is the concatenation of the score and the dictionary word (to later aggregate the scores from multiple chunks)
				c.collectBuf = c.collectBuf[:8]
				for s := int32(0); s < l; s++ {
					c.collectBuf = append(c.collectBuf, c.superstring[(saFiltered[i]+s)*2+1])
				}
				binary.BigEndian.PutUint64(c.collectBuf[:8], score)
				if err := c.collector.Collect(c.collectBuf[8:], c.collectBuf[:8]); err != nil { // key will be copied by Collect function
					return fmt.Errorf("collecting %x with score %d: %w", c.collectBuf[8:], score, err)
				}
			}
		}
	}
	c.superstring = c.superstring[:0]
	return nil
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
	w := bufio.NewWriterSize(f, etl.BufIOSize)
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
	r := bufio.NewReaderSize(f.f, etl.BufIOSize)
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
