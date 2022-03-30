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
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/sais/gsa"
	"github.com/ledgerwatch/log/v3"
)

// CompressorGSA is the main operating type for performing per-word compression
// After creating a compression, one needs to add superstrings to it, using `AddWord` function
// In order to add word without compression, function `AddUncompressedWord` needs to be used
// Compressor only tracks which words are compressed and which are not until the compressed
// file is created. After that, the user of the file needs to know when to call
// `Next` or `NextUncompressed` function on the decompressor.
// After that, `Compress` function needs to be called to perform the compression
// and eventually create output file
type CompressorGSA struct {
	uncompressedFile           *DecompressedFile
	outputFile, tmpOutFilePath string // File where to output the dictionary and compressed data
	tmpDir                     string // temporary directory to use for ETL when building dictionary
	workers                    int

	// Buffer for "superstring" - transformation of superstrings where each byte of a word, say b,
	// is turned into 2 bytes, 0x01 and b, and two zero bytes 0x00 0x00 are inserted after each word
	// this is needed for using ordinary (one string) suffix sorting algorithm instead of a generalised (many superstrings) suffix
	// sorting algorithm
	superstring      superstring
	superstrings     chan superstring
	wg               *sync.WaitGroup
	suffixCollectors []*etl.Collector
	wordsCount       uint64

	ctx       context.Context
	logPrefix string
	Ratio     CompressionRatio
	trace     bool
}

func NewCompressorGSA(ctx context.Context, logPrefix, outputFile, tmpDir string, minPatternScore uint64, workers int) (*CompressorGSA, error) {
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
	superstrings := make(chan superstring, workers*2)
	wg := &sync.WaitGroup{}
	wg.Add(workers)
	suffixCollectors := make([]*etl.Collector, workers)
	for i := 0; i < workers; i++ {
		collector := etl.NewCollector(compressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		suffixCollectors[i] = collector
		go processSuperstring2(superstrings, collector, minPatternScore, wg)
	}

	return &CompressorGSA{
		uncompressedFile: uncompressedFile,
		tmpOutFilePath:   tmpOutFilePath,
		outputFile:       outputFile,
		tmpDir:           tmpDir,
		logPrefix:        logPrefix,
		workers:          workers,
		ctx:              ctx,
		superstrings:     superstrings,
		suffixCollectors: suffixCollectors,
		wg:               wg,
	}, nil
}

func (c *CompressorGSA) Close() {
	c.uncompressedFile.Close()
	for _, collector := range c.suffixCollectors {
		collector.Close()
	}
	c.suffixCollectors = nil
}

func (c *CompressorGSA) SetTrace(trace bool) {
	c.trace = trace
}

func (c *CompressorGSA) AddWord(word []byte) error {
	c.wordsCount++

	if len(c.superstring.str)+2*len(word)+2 > superstringLimit {
		c.superstring.str = append(c.superstring.str, 0)
		c.superstrings <- c.superstring
		c.superstring.str = nil
		c.superstring.wordsCount = 0
	}

	c.superstring.wordsCount++
	for _, a := range word {
		if a < 255 && a > 1 {
			c.superstring.str = append(c.superstring.str, a+1)
		}
	}
	if len(word) > 0 {
		if c.superstring.str[len(c.superstring.str)-1] > 1 {
			c.superstring.str = append(c.superstring.str, 1)
		}
	}

	return c.uncompressedFile.Append(word)
}

func (c *CompressorGSA) AddUncompressedWord(word []byte) error {
	c.wordsCount++
	return c.uncompressedFile.AppendUncompressed(word)
}

func (c *CompressorGSA) Compress() error {
	c.uncompressedFile.w.Flush()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	if len(c.superstring.str) > 0 {
		c.superstring.str = append(c.superstring.str, 0)
		c.superstrings <- c.superstring
		c.superstring.str = nil
		c.superstring.wordsCount = 0
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
	if err := reducedict(c.ctx, c.trace, c.logPrefix, c.tmpOutFilePath, c.uncompressedFile, c.workers, db); err != nil {
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

type superstring struct {
	str        []byte
	wordsCount uint64
}

// processSuperstring is the worker that processes one superstring and puts results
// into the collector, using lock to mutual exclusion. At the end (when the input channel is closed),
// it notifies the waitgroup before exiting, so that the caller known when all work is done
// No error channels for now
func processSuperstring2(superstringCh chan superstring, dictCollector *etl.Collector, minPatternScore uint64, completion *sync.WaitGroup) {
	defer completion.Done()
	dictVal := make([]byte, 8)
	dictKey := make([]byte, maxPatternLen)
	var sa []uint
	var lcp []int
	var da []int32
	for superstring := range superstringCh {
		str := superstring.str
		n := len(str)
		sa = make([]uint, n)
		//if cap(sa) < n {
		//	sa = make([]uint, n)
		//} else {
		//	sa = sa[:n]
		//}
		lcp = make([]int, n)
		//if cap(lcp) < n {
		//	lcp = make([]int, n)
		//} else {
		//	lcp = lcp[:n]
		//}
		da = make([]int32, n)
		//if cap(da) < n {
		//	da = make([]int32, n)
		//} else {
		//	da = da[:n]
		//}
		_ = gsa.GSA(str, sa, lcp, da)

		gsa.PrintArrays(str, sa, lcp, da)
		gsa.PrintRepeats(str, sa, lcp, da)

		//remove terminator
		sa = sa[1:]
		lcp = lcp[1:]
		da = da[1:]
		n = len(sa) - 1
		//fmt.Printf(" a: %d, %d, %d\n", sa, lcp, da)
		//[4 17 22 13 18 5 15 20 7 2 11 0 9 3 12 1 10 14 19 6 16 21 8],
		//[0 0 0 0 4 4 0 2 2 0 2 2 4 0 1 1 3 0 3 3 0 1 1],
		//[0 1 2 1 2 1 1 2 1 0 1 0 1 0 1 0 1 1 2 1 1 2 1]

		var repeats int
		for i := 0; i < len(da)-1; i++ {
			posAfter := sa[da[i]]
			l := posAfter - sa[i]

			if l < minPatternLen || l > maxPatternLen || l > 20 && (l&(l-1)) != 0 { // is power of 2
				repeats = 0
				continue
			}

			repeats++
			if da[i] < da[i+1] { // same suffix
				continue
			}
			if (l < 8 && repeats < int(minPatternScore)) || (l > 64 && repeats < 200) {
				repeats = 0
				continue
			}

			// Only when there is a drop in LCP value
			//if lcp[i+1] >= lcp[i] {
			//	j = i
			//	//continue
			//}
			//var isNew bool
			//for j > 0 && lcp[j-1] >= lcp[i] {
			//	j--
			//	isNew = true
			//	_ = isNew
			//}

			score := uint64(l) * uint64(repeats)
			if score < minPatternScore {
				repeats = 0
				continue
			}
			dictKey = dictKey[:0]
			for s := int(sa[i]); s < int(sa[i]+l); s++ {
				dictKey = append(dictKey, str[s]-1)
			}

			binary.BigEndian.PutUint64(dictVal, score)
			if err := dictCollector.Collect(dictKey, dictVal); err != nil {
				log.Error("processSuperstring", "collect", err)
			}
			repeats = 0
		}
		_, _ = dictVal, dictKey
		/*
			var b Int32Sort = inv
			j = 0
			for i := 0; i < n-1; i++ {
				// Only when there is a drop in LCP value
				if lcp[i+1] >= lcp[i] {
					j = i
					continue
				}
				prevSkipped := false
				for l := int(lcp[i]); l > int(lcp[i+1]) && l >= minPatternLen; l-- {
					if l > maxPatternLen ||
						l > 20 && (l&(l-1)) != 0 { // is power of 2
						prevSkipped = true
						continue
					}

					// Go back
					var isNew bool
					for j > 0 && int(lcp[j-1]) >= l {
						j--
						isNew = true
					}

					if !isNew && !prevSkipped {
						break
					}

					window := i - j + 2
					copy(b, filtered[j:i+2])
					sort.Sort(b[:window])
					repeats := 1
					lastK := 0
					for k := 1; k < window; k++ {
						if b[k] >= b[lastK]+int32(l) {
							repeats++
							lastK = k
						}
					}

					if (l < 8 && repeats < int(minPatternScore)) ||
						(l > 64 && repeats < 200) {
						prevSkipped = true
						continue
					}

					score := uint64(repeats * (l))
					if score < minPatternScore {
						prevSkipped = true
						continue
					}

					dictKey = dictKey[:l]
					for s := 0; s < l; s++ {
						dictKey[s] = superstring[(int(filtered[i])+s)*2+1]
					}
					binary.BigEndian.PutUint64(dictVal, score)
					if err := dictCollector.Collect(dictKey, dictVal); err != nil {
						log.Error("processSuperstring", "collect", err)
					}
					prevSkipped = false //nolint
					break
				}
			}
		*/
	}
}
