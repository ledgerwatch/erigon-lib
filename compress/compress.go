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
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/flanglet/kanzi-go/transform"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
)

// Compressor is the main operating type for performing per-word compression
// This include several phases:
// 1. Building initial dictionary
// 2. Reduction of dictionary
// 3. Compression
// For each ot these phases, the set of words needs to be provided, using corresponding functions
type Compressor struct {
	outputFile string // File where to output the dictionary and compressed data
	tmpDir     string // temporary directory to use for ETL when building dictionary
	phase      Phase  // Current phase
	// Buffer for "superstring" - transformation of words where each byte of a word, say b,
	// is turned into 2 bytes, 0x01 and b, and two zero bytes 0x00 0x00 are inserted after each word
	// this is needed for using ordinary (one string) suffix sorting algorithm instead of a generalised (many words) suffix
	// sorting algorithm
	superstring []byte
	divsufsort  *transform.DivSufSort
	suffixarray []int32        // Suffix array - output for divsufsort algorithm
	lcp         []int32        // LCP array (Longest Common Prefix)
	collector   *etl.Collector // Collector used to handle very large sets of words
	collectBuf  []byte         // Buffer for forming key to call collector
	dictBuilder DictionaryBuilder
}

type Phase int

const (
	BuildDict Phase = iota
	ReduceDict
	Compress
)

// superstringLimit limits how large can one "superstring" get before it is processed
// Compressor allocates 7 bytes for each uint of superstringLimit. For example,
// superstingLimit 16m will result in 112Mb being allocated for various arrays
const superstringLimit = 16 * 1024 * 1024

// minPatternLen is minimum length of pattern we consider to be included into the dictionary
const minPatternLen = 5

// minPatternRepeats is minimum repeats (per superstring) required to consider including pattern into the dictionary
const minPatternRepeats = 128

// maxDictPatterns is the maximum number of patterns allowed in the initial (not reduced dictionary)
// Large values increase memory consumption of dictionary reduction phase
const maxDictPatterns = 1024 * 1024

const compressLogPrefix = "compress"

type DictionaryItem struct {
	word  []byte
	score uint64
}

type DictionaryBuilder struct {
	limit         int
	lastWord      []byte
	lastWordScore uint64
	items         []DictionaryItem
}

func (db *DictionaryBuilder) Reset(limit int) {
	db.limit = limit
	db.items = db.items[:0]
}

func (db DictionaryBuilder) Len() int {
	return len(db.items)
}

func (db DictionaryBuilder) Less(i, j int) bool {
	if db.items[i].score == db.items[j].score {
		return bytes.Compare(db.items[i].word, db.items[j].word) < 0
	}
	return db.items[i].score < db.items[j].score
}

func (db *DictionaryBuilder) Swap(i, j int) {
	db.items[i], db.items[j] = db.items[j], db.items[i]
}

func (db *DictionaryBuilder) Push(x interface{}) {
	db.items = append(db.items, x.(DictionaryItem))
}

func (db *DictionaryBuilder) Pop() interface{} {
	old := db.items
	n := len(old)
	x := old[n-1]
	db.items = old[0 : n-1]
	return x
}

func (db *DictionaryBuilder) processWord(word []byte, score uint64) {
	heap.Push(db, DictionaryItem{word: word, score: score})
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
		db.lastWord = common.Copy(k)
		db.lastWordScore = score
	}
	return nil
}

func (db *DictionaryBuilder) finish() {
	if db.lastWord != nil {
		db.processWord(db.lastWord, db.lastWordScore)
	}
}

// Pattern is representation of a pattern that is searched in the words to compress them
// patterns are stored in a patricia tree and contain pattern score (calculated during
// the initial dictionary building), frequency of usage, and code
type Pattern struct {
	score    uint64
	uses     uint64
	code     uint64 // Allocated numerical code or huffman code
	codeBits int    // Number of bits in the code
}

func NewCompressor(outputFile string, tmpDir string) (*Compressor, error) {
	c := &Compressor{
		outputFile:  outputFile,
		tmpDir:      tmpDir,
		superstring: make([]byte, 0, superstringLimit), // Allocate enough, so we never need to resize
		suffixarray: make([]int32, superstringLimit),
		lcp:         make([]int32, superstringLimit/2),
		collectBuf:  make([]byte, 8, 256),
	}
	var err error
	if c.divsufsort, err = transform.NewDivSufSort(); err != nil {
		return nil, err
	}
	c.collector = etl.NewCollector(tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	return c, nil
}

// BuildDictNextWord needs to be called repeatedly to provide all the words for building the dictonary phase
func (c *Compressor) BuildDictNextWord(word []byte) error {
	if c.phase != BuildDict {
		return fmt.Errorf("buidling dictionary already finished, phase %v", c.phase)
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
	return nil
}

// ReduceDictNextWord needs to be called repeatedly to provide all the words for reducing the dictionary
func (c *Compressor) ReduceDictNextWord(word []byte) error {
	switch c.phase {
	case BuildDict:
		// Perform dictionary building and switch to the next phase
		if err := c.buildDictionary(); err != nil {
			return err
		}
		c.phase = ReduceDict
	case ReduceDict:
	default:
		return fmt.Errorf("reducing dictonary already finished, phase %v", c.phase)
	}
	return nil
}

// CompressNextWord needs to be called repeatedly to provide all the words for compressing and generating output file
func (c *Compressor) CompressNextWord(word []byte) error {
	switch c.phase {
	case BuildDict:
		return fmt.Errorf("need to reduce dictionary before compression")
	case ReduceDict:
		// Perform dictionary reduction and switch to the next phase
		if err := c.reduceDictionary(); err != nil {
			return err
		}
		c.phase = Compress
	}
	return nil
}

// Close finishes compression and closes output file, and cleans up temporary files
func (c *Compressor) Close() error {
	return nil
}

func (c *Compressor) buildDictionary() error {
	if len(c.superstring) > 0 {
		// Process any residual words
		if err := c.processSuperstring(); err != nil {
			return fmt.Errorf("buildDictionary: error processing superstring: %w", err)
		}
	}
	c.dictBuilder.Reset(maxDictPatterns)
	if err := c.collector.Load(compressLogPrefix, nil /* db */, "" /* toBucket */, c.dictBuilder.loadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	c.dictBuilder.finish()
	// Sort dictionary inside the dictionary bilder in the order of increasing scores
	sort.Sort(&c.dictBuilder)
	return nil
}

func (c *Compressor) reduceDictionary() error {
	return nil
}

func (c *Compressor) processSuperstring() error {
	c.divsufsort.ComputeSuffixArray(c.superstring, c.suffixarray[:len(c.superstring)])
	// filter out suffixes that start with odd positions - we reuse the first half of sa.suffixarray for that
	// because it won't be used after filtration
	n := len(c.superstring) / 2
	saFiltered := c.suffixarray[:n]
	for i := 0; i < n; i++ {
		saFiltered[i] = c.suffixarray[i*2] / 2
	}
	// Now create an inverted array - we reuse the second half of sa.suffixarray for that
	saInverted := c.suffixarray[n:]
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
	j := 0
	for i := 0; i < n-1; i++ {
		// Only when there is a drop in LCP value
		if c.lcp[i+1] >= c.lcp[i] {
			j = i
			continue
		}
		for l := c.lcp[i]; l > c.lcp[i+1]; l-- {
			if l < minPatternLen {
				continue
			}
			// Go back
			var new bool
			for j > 0 && c.lcp[j-1] >= l {
				j--
				new = true
			}
			if !new {
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
			if repeats > minPatternRepeats {
				score := uint64(repeats * int(l-4))
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
