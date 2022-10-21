package rcompress

// #include "02_dict.h"
// #include "03_encoder.h"
// #include "rutils.h"
import "C"

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"unsafe"

	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/sais"
	"github.com/ledgerwatch/log/v3"
)

// superstringLimit limits how large can one "superstring" get before it is processed
// CompressorSequential allocates 7 bytes for each uint of superstringLimit. For example,
// superstingLimit 16m will result in 112Mb being allocated for various arrays
const superstringLimit = (16 * 1024 * 1024) / 2
const minLCP = 5
const maxLCP = 255
const flagEOW byte = 1

var maxWordSoFar = 0

func __assert_true(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

func __assert_false(cond bool, msg string) {
	if cond == true {
		panic(msg)
	}
}

func __panic_if(cond bool, msg string) {
	if cond {
		panic(msg)
	}
}

type superstring struct {
	src   []byte // words them self
	flags []byte // contains end of word flags
}

type prefix_tree struct {
	trie *C.Trie
	mu   sync.Mutex
}

func newPrefixTree() *prefix_tree {
	return &prefix_tree{trie: C.NewTrie()}
}

// type ref_count struct {
// 	refs []int32
// 	mu   sync.Mutex
// }

// func newRefCount(size int) *ref_count {
// 	refs := make([]int32, size)
// 	for i := 0; i < size; i++ {
// 		refs[i] = 0
// 	}
// 	return &ref_count{refs: refs}
// }

// func (this *ref_count) add_count(prefix_count []int32) {
// 	this.mu.Lock()
// 	defer this.mu.Unlock()

// 	for idx, v := range prefix_count {
// 		this.refs[idx] += v
// 	}
// }

type block struct {
	data          []byte
	sizes         []int32
	precompressed []int32
	id            int
}

type precompressed_blocks struct {
	file *AppendFile

	precompressed []int32
	order         []int64 // maps block number to offset in file
	buf           []byte
	total         int
	next          int
}

func new_precompressed_blocks(path string) (*precompressed_blocks, error) {
	file, err := NewAppendFile(path)

	if err != nil {
		return nil, err
	}

	return &precompressed_blocks{
		file:  file,
		order: make([]int64, (1024)),   // TODO, count how many precompressed blocks could be there
		buf:   make([]byte, (1 << 24)), // TOOD, count the max possible buffer
		total: 0,
		next:  0,
	}, nil

}

func (this *precompressed_blocks) get_next() []int32 {

	if len(this.precompressed) == 0 {
		// TODO find out max precompressed block size
		this.precompressed = make([]int32, (1 << 24))
	}

	var a, b, c, d, n int32
	if this.next < this.total {
		offset := this.order[this.next]
		size, err := this.file.read(offset, this.buf)

		__assert_true(err == nil, "err == nil")

		p_idx := 0
		for i := 0; i < size; i += 4 {
			a = int32(this.buf[i]) << 24
			b = int32(this.buf[i+1]) << 16
			c = int32(this.buf[i+2]) << 8
			d = int32(this.buf[i+3])
			n = a | b | c | d
			this.precompressed[p_idx] = n
			p_idx++
		}

		this.next++

		return this.precompressed[:p_idx]
	}
	return this.precompressed[:0]
}

func (this *precompressed_blocks) insert(_block block) error {

	offset, err := this.file.f.Seek(0, 1)
	if err != nil {
		return err
	}
	// __panic_if(err != nil, err.Error())

	this.order[_block.id] = offset

	buf_idx := 0
	var a, b, c, d byte

	for _, n32 := range _block.precompressed {
		a = byte(n32 >> 24)
		b = byte((n32 & 0x00FF0000) >> 16)
		c = byte((n32 & 0x0000FF00 >> 8))
		d = byte(n32 & 0x000000FF)

		this.buf[buf_idx] = a
		this.buf[buf_idx+1] = b
		this.buf[buf_idx+2] = c
		this.buf[buf_idx+3] = d
		buf_idx += 4
	}

	err = this.file.Append(this.buf[:buf_idx])
	if err != nil {
		return err
	}

	this.file.w.Flush()
	this.total++

	return nil
}

// Compressor is the main operating type for performing per-word compression
// After creating a compression, one needs to add superstrings to it, using `AddWord` function
// In order to add word without compression, function `AddUncompressedWord` needs to be used
// Compressor only tracks which words are compressed and which are not until the compressed
// file is created. After that, the user of the file needs to know when to call
// `Next` or `NextUncompressed` function on the decompressor.
// After that, `Compress` function needs to be called to perform the compression
// and eventually create output file
type Compressor struct {
	compressedFile    *AppendFile
	uncompressedFile  *AppendFile
	precompressedFile *precompressed_blocks

	outputFile, tmpOutFilePath string // File where to output the dictionary and compressed data
	precmpFile, preOutFilePath string
	tmpDir                     string // temporary directory to use for ETL when building dictionary
	workers                    int

	// Buffer for "superstring" - transformation of superstrings where each byte of a word, say b,
	// is turned into 2 bytes, 0x01 and b, and two zero bytes 0x00 0x00 are inserted after each word
	// this is needed for using ordinary (one string) suffix sorting algorithm instead of a generalised (many superstrings) suffix
	// sorting algorithm
	words superstring

	superstrings chan superstring

	wg *sync.WaitGroup

	wordsCount      uint64
	emptyWordsCount uint64

	ctx       context.Context
	logPrefix string
	Ratio     CompressionRatio
	trace     bool
	lvl       log.Lvl

	// trie *C.Trie
	ptree *prefix_tree

	byte_count uint32
	count_gt   int
}

func NewCompressor(ctx context.Context, logPrefix, outputFile, tmpDir string, minPatternScore uint64, workers int, lvl log.Lvl) (*Compressor, error) {
	dir2.MustExist(tmpDir)
	dir, fileName := filepath.Split(outputFile)
	tmpOutFilePath := filepath.Join(dir, fileName) + ".tmp"
	// // UncompressedFile - it's intermediate .idt file, outputFile it's final .seg (or .dat) file.
	// // tmpOutFilePath - it's ".seg.tmp" (".idt.tmp") file which will be renamed to .seg file if everything succeed.
	// // It allow atomically create .seg file (downloader will not see partially ready/ non-ready .seg files).
	// // I didn't create ".seg.tmp" file in tmpDir, because I think tmpDir and snapsthoDir may be mounted to different drives
	uncompressedPath := filepath.Join(tmpDir, fileName) + ".idt"
	precompressedPath := filepath.Join(tmpDir, "precompressed") + ".idt"

	compressedFile, err := NewAppendFile(outputFile)
	if err != nil {
		return nil, err
	}

	uncompressedFile, err := NewAppendFile(uncompressedPath)
	if err != nil {
		return nil, err
	}

	precompressedFile, err := new_precompressed_blocks(precompressedPath)

	words := superstring{
		src:   make([]byte, 0, superstringLimit),
		flags: make([]byte, 0, superstringLimit),
	}

	// Collector for dictionary superstrings (sorted by their score)
	superstrings := make(chan superstring, workers*2)
	wg := &sync.WaitGroup{}
	wg.Add(workers)
	ptree := newPrefixTree()

	for i := 0; i < workers; i++ {

		go processSuperstring(superstrings, wg, ptree)
	}

	return &Compressor{
		compressedFile:    compressedFile,
		uncompressedFile:  uncompressedFile,
		precompressedFile: precompressedFile,
		tmpOutFilePath:    tmpOutFilePath,
		outputFile:        outputFile,
		tmpDir:            tmpDir,
		logPrefix:         logPrefix,
		workers:           workers,
		ctx:               ctx,
		words:             words,
		superstrings:      superstrings,

		lvl:      lvl,
		wg:       wg,
		ptree:    ptree,
		count_gt: 0,
	}, nil

}

func (c *Compressor) Close() {
	// c.uncompressedFile.Close()
	// for _, collector := range c.suffixCollectors {
	// 	collector.Close()
	// }
	// c.suffixCollectors = nil

}

func (c *Compressor) SetTrace(trace bool) {
	// c.trace = trace
}

func (c *Compressor) Count() int {
	return int(c.wordsCount)
}

func (c *Compressor) AddWord(word []byte) error {
	c.wordsCount++

	if len(word) > maxWordSoFar {
		maxWordSoFar = len(word)
	}

	if len(word) == 0 {
		c.emptyWordsCount++
	}

	if len(c.words.src)+len(word)+1 > superstringLimit {

		c.byte_count += uint32(len(c.words.src))
		c.count_gt++

		c.superstrings <- c.words

		c.words = superstring{
			src:   make([]byte, 0, superstringLimit),
			flags: make([]byte, 0, superstringLimit),
		}

	}

	for _, a := range word {
		c.words.src = append(c.words.src, a)
		c.words.flags = append(c.words.flags, 0)
	}

	c.words.src = append(c.words.src, 255)
	c.words.flags = append(c.words.flags, flagEOW)

	return c.uncompressedFile.Append(word)
}

func (c *Compressor) AddUncompressedWord(word []byte) error {
	// c.wordsCount++
	// return c.uncompressedFile.AppendUncompressed(word)
	return c.AddWord(word)
}

func pre_compress(dict *C.Dict, trie *C.Trie, completion *sync.WaitGroup, blocksCh chan block, matchChan chan block) {

	defer completion.Done()

	preCompressedWord := make([]int32, maxWordSoFar)

	for _block := range blocksCh {

		var start int32
		for _, w_size := range _block.sizes {
			word := _block.data[start : start+w_size]
			start += w_size

			if w_size > 0 {
				size := C.int(w_size)
				wordPtr := unsafe.Pointer(&word[0])

				prePtr := unsafe.Pointer(&preCompressedWord[0])

				pre_size := int(C.Precompress(dict, trie, (*C.uchar)(wordPtr), size, (*C.int)(prePtr)))

				for _, v := range preCompressedWord[0:pre_size] {
					_block.precompressed = append(_block.precompressed, v)
				}
			}
		}

		matchChan <- _block
	}

}

func count_matches(dict *C.Dict, matchChan chan block, completion *sync.WaitGroup, precmp *precompressed_blocks) {
	defer completion.Done()

	for _block := range matchChan {

		data_ptr := unsafe.Pointer(&_block.data[0])
		data_size := C.int(len(_block.data))
		sizes_ptr := unsafe.Pointer(&_block.sizes[0])
		sizes_size := C.int(len(_block.sizes))
		pre_ptr := unsafe.Pointer(&_block.precompressed[0])
		pre_size := C.int(len(_block.precompressed))

		C.CountMatches(
			dict,
			(*C.uchar)(data_ptr),
			data_size,
			(*C.int)(sizes_ptr),
			sizes_size,
			(*C.int)(pre_ptr),
			pre_size,
		)

		err := precmp.insert(_block)
		__assert_true(err == nil, "err := precmp.insert(_block)")
	}
}

func (c *Compressor) Compress() error {
	c.uncompressedFile.w.Flush()
	// logEvery := time.NewTicker(20 * time.Second)
	// defer logEvery.Stop()
	if len(c.words.src) > 0 {
		c.superstrings <- c.words
	}
	close(c.superstrings)
	c.wg.Wait()

	defer c.uncompressedFile.Close(true)
	defer c.precompressedFile.file.Close(true)
	defer c.compressedFile.Close(false)

	created := 0
	created_ptr := unsafe.Pointer(&created)

	dict := C.BuildStaticDict(c.ptree.trie, (*C.int)(created_ptr))
	defer C.DeleteDict(dict)

	defer C.CloseTrie(c.ptree.trie)

	blockChan := make(chan block, c.workers*2)
	_block := block{
		data:  make([]byte, 0, (1 << 24)),
		sizes: make([]int32, 0, (1 << 16)),
	}
	c.wg.Add(c.workers)

	wg2 := &sync.WaitGroup{}
	matchChan := make(chan block, c.workers*2)

	for i := 0; i < c.workers; i++ {

		go pre_compress(dict, c.ptree.trie, c.wg, blockChan, matchChan)
	}

	go count_matches(dict, matchChan, wg2, c.precompressedFile)

	wg2.Add(1)

	n := 0
	_id := 0

	num_blocks := 0
	if err := c.uncompressedFile.ForEach(func(word []byte) error {

		if len(word)+n > (1 << 24) {

			data_copy := make([]byte, len(_block.data))
			copy(data_copy, _block.data)

			sizes_copy := make([]int32, len(_block.sizes))
			copy(sizes_copy, _block.sizes)

			precompressed_copy := make([]int32, 0, len(_block.data))

			blockChan <- block{
				data:          data_copy,
				sizes:         sizes_copy,
				precompressed: precompressed_copy,
				id:            _id,
			}

			_id++
			_block = block{
				data:  _block.data[:0],
				sizes: _block.sizes[:0],
				id:    _id,
			}
			n = 0
			num_blocks += 1
		}

		_block.data = append(_block.data, word...)
		_block.sizes = append(_block.sizes, int32(len(word)))

		n += len(word)

		return nil
	}); err != nil {
		fmt.Println(err)
	}
	blockChan <- _block
	num_blocks += 1
	close(blockChan)
	c.wg.Wait()

	close(matchChan)
	wg2.Wait()

	C.ReduceDict(dict)

	encoder := C.NewEncoder(dict)
	defer C.DeleteEncoder(encoder)

	dst := make([]byte, (1 << 25))

	header_size := 0
	bytes_written := binary.PutUvarint(dst[header_size:], c.wordsCount)
	header_size += bytes_written
	bytes_written = binary.PutUvarint(dst[header_size:], c.emptyWordsCount)
	header_size += bytes_written
	bytes_written = binary.PutUvarint(dst[header_size:], uint64(num_blocks))
	header_size += bytes_written
	bytes_written = binary.PutUvarint(dst[header_size:], uint64(maxWordSoFar))
	header_size += bytes_written

	c.compressedFile.AppendNoSize(dst[:header_size])

	// fmt.Println("HEADER SIZE: ", header_size)
	// fmt.Println("WORDS_COUNT: ", c.wordsCount)
	// fmt.Println("EMPTY_WORDS_COUNT: ", c.emptyWordsCount)
	// fmt.Println("NUM_BLOCKS: ", num_blocks)
	// fmt.Println("MAX WORD SOFAR: ", maxWordSoFar)

	// WRITE HEADER:
	// num words
	// num blocks
	// maxWordSoFar

	// etc
	// COMPRESS DICT, WRITE IT DOWN TO FILE
	// COMPRESS each block using precompressed data

	dstPtr := unsafe.Pointer(&dst[0])
	encoded_dict_size := int(C.EncodeDict(encoder, (*C.uchar)(dstPtr)))

	c.compressedFile.Append(dst[:encoded_dict_size])

	n = 0
	_id = 0
	num_blocks2 := 0
	_block = block{
		data:  _block.data[:0],
		sizes: _block.sizes[:0],
		id:    _id,
	}
	if err := c.uncompressedFile.ForEach(func(word []byte) error {

		if len(word)+n > (1 << 24) {

			data_copy := make([]byte, len(_block.data))
			copy(data_copy, _block.data)

			sizes_copy := make([]int32, len(_block.sizes))
			copy(sizes_copy, _block.sizes)

			precmp := c.precompressedFile.get_next()

			data_ptr := unsafe.Pointer(&data_copy[0])
			data_size := C.int(len(data_copy))
			sizes_ptr := unsafe.Pointer(&sizes_copy[0])
			sizes_size := C.int(len(sizes_copy))
			pre_ptr := unsafe.Pointer(&precmp[0])
			pre_size := C.int(len(precmp))
			dst_ptr := unsafe.Pointer(&dst[0])

			encoded_block_size := C.EncodeBlock(
				encoder,
				(*C.uchar)(data_ptr),
				data_size,
				(*C.int)(sizes_ptr),
				sizes_size,
				(*C.int)(pre_ptr),
				pre_size,
				(*C.uchar)(dst_ptr),
			)

			c.compressedFile.Append(dst[:int(encoded_block_size)])

			_id++
			_block = block{
				data:  _block.data[:0],
				sizes: _block.sizes[:0],
				id:    _id,
			}

			n = 0
			num_blocks2 += 1
		}

		_block.data = append(_block.data, word...)
		_block.sizes = append(_block.sizes, int32(len(word)))

		n += len(word)

		return nil
	}); err != nil {
		fmt.Println(err)
	}

	precmp := c.precompressedFile.get_next()

	data_ptr := unsafe.Pointer(&_block.data[0])
	data_size := C.int(len(_block.data))
	sizes_ptr := unsafe.Pointer(&_block.sizes[0])
	sizes_size := C.int(len(_block.sizes))
	pre_ptr := unsafe.Pointer(&precmp[0])
	pre_size := C.int(len(precmp))
	dst_ptr := unsafe.Pointer(&dst[0])

	encoded_block_size := C.EncodeBlock(
		encoder,
		(*C.uchar)(data_ptr),
		data_size,
		(*C.int)(sizes_ptr),
		sizes_size,
		(*C.int)(pre_ptr),
		pre_size,
		(*C.uchar)(dst_ptr),
	)

	c.compressedFile.Append(dst[:int(encoded_block_size)])
	num_blocks2 += 1

	__assert_true(num_blocks == num_blocks2, "num_blocks == num_blocks2")
	msg := fmt.Sprintf("num_blocks: %d != int(c.compressedFile.count): %d", num_blocks, c.compressedFile.count-2)
	__assert_true(num_blocks == int(c.compressedFile.count)-2, msg)

	c.compressedFile.w.Flush()

	return nil
}

// in this lcp last value is 0 not the first one
// so lcp[0] -> lcp of 0th and 1st suffixes
func lcpKasai(src []byte, sa, lcp, aux []int32) {
	size := C.int(len(src))
	srcPtr := unsafe.Pointer(&src[0])
	saPtr := unsafe.Pointer(&sa[0])
	lcpPtr := unsafe.Pointer(&lcp[0])
	auxPtr := unsafe.Pointer(&aux[0])

	C.LcpKasai(
		(*C.uchar)(srcPtr),
		(*C.int)(saPtr),
		(*C.int)(lcpPtr),
		(*C.int)(auxPtr),
		size,
	)
}

// processSuperstring is the worker that processes one superstring and puts results
// into the collector, using lock to mutual exclusion. At the end (when the input channel is closed),
// it notifies the waitgroup before exiting, so that the caller known when all work is done
// No error channels for now
func processSuperstring(superstringCh chan superstring, completion *sync.WaitGroup, ptree *prefix_tree) {
	defer completion.Done()

	var sa, lcp, aux []int32

	var i, k int32
	var src_size int

	var sa_curr, sa_next, lcp_curr, lcp_next int32

	// total_by_thread := 0

	inserted := 1
	for superstr := range superstringCh {

		if inserted == 1 {
			src_size = len(superstr.src)

			// atomic.AddInt32(&total_processed, int32(src_size)) // testing only
			// total_by_thread += src_size // testing only

			if cap(sa) < src_size {
				sa = make([]int32, src_size)
				lcp = make([]int32, src_size)
				aux = make([]int32, src_size)
			} else {
				sa = sa[:src_size]
				lcp = sa[:src_size]
				aux = sa[:src_size]
			}

			//start := time.Now()
			if err := sais.Sais(superstr.src, sa); err != nil {
				panic(err)
			}

			lcpKasai(superstr.src, sa, lcp, aux)

			for i = 0; i < int32(src_size); i++ {
				aux[i] = 0
			}

			for i = 0; i < int32(src_size)-1; {

				sa_curr = sa[i]
				lcp_curr = lcp[i]

				k = i + 1
				sa_next = sa[k]
				lcp_next = lcp[k]

				if lcp_curr < minLCP || lcp_curr > maxLCP {
					i++
					continue
				}

				// visited prefixes
				if aux[sa_curr] > 0 || aux[sa_next] > 0 {
					i++
					continue
				}

				if lcp_next > lcp_curr {
					i++
					continue
				}

				if superstr.flags[sa_curr] == flagEOW || superstr.flags[sa_curr+1] == flagEOW ||
					superstr.flags[sa_curr+2] == flagEOW || superstr.flags[sa_curr+3] == flagEOW {
					i++
					continue
				}

				if sa_curr+1 == sa_next {
					i++
					continue
				}

				// __panic_if(sa_curr == sa_next, "sa_curr == sa_next")

				// if sa_curr > sa_next {
				// 	__panic_if(sa_curr-sa_next < lcp_next, "sa_curr-sa_next < lcp_next")
				// }

				// if sa_curr < sa_next {
				// 	__panic_if(sa_next-sa_curr < lcp_curr, "sa_next-sa_curr < lcp_curr")
				// }

				if lcp_curr == lcp_next {
					j := sa_curr
					q := sa_next
					for j < sa_curr+lcp_curr {
						aux[j]++
						aux[q]++
						j++
						q++
					}

					// for j := sa_next; j < int32(src_size) && j < sa_next+lcp_curr; j++ {
					// 	aux[j]++
					// }

					k = k + 1
					sa_next = sa[k]
					lcp_next = lcp[k]

					for lcp_curr == lcp_next {

						for j := sa_next; j < int32(src_size) && j < sa_next+lcp_curr; j++ {
							aux[j]++
						}

						k++
						sa_next = sa[k]
						lcp_next = lcp[k]
					}

					start := sa_curr
					end := sa_curr + lcp_curr
					for j := start; j < end; j++ {
						if superstr.flags[j] == flagEOW {
							end = j
							break
						}
					}

					inserted = add_prefix(superstr.src[start:end], ptree)

					start = end + 1
					end = sa_curr + lcp_curr
					if start+3 < end {
						inserted = add_prefix(superstr.src[start:end], ptree)
					}
				}

				i = k
			}
		}
	}
}

func add_prefix(data []byte, ptree *prefix_tree) int {
	ptree.mu.Lock()
	defer ptree.mu.Unlock()

	size := C.int(len(data))
	dataPtr := unsafe.Pointer(&data[0])

	return int(C.InsertPrefix(ptree.trie, (*C.uchar)(dataPtr), size))
}
