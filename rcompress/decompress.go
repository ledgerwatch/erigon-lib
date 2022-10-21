package rcompress

// #include "05_decoder.h"
import "C"
import (
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/mmap"
)

// Decompressor provides access to the superstrings in a file produced by a compressor
type Decompressor struct {
	compressedFile string
	f              *os.File
	mmapHandle1    []byte                 // mmap handle for unix (this is used to close mmap)
	mmapHandle2    *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	data           []byte                 // slice of correct size for the decompressor to work with

	size            int64
	wordsCount      uint64
	emptyWordsCount uint64
	numBlocks       uint64
	blocksStart     uint64 // Offset of whether the superstrings actually start
	maxWordSize     uint64
	decoder         *C.Decoder
}

func NewDecompressor(compressedFile string) (*Decompressor, error) {

	d := &Decompressor{
		compressedFile: compressedFile,
	}
	var err error
	d.f, err = os.Open(compressedFile)
	if err != nil {
		return nil, err
	}
	var stat os.FileInfo
	if stat, err = d.f.Stat(); err != nil {
		return nil, err
	}
	d.size = stat.Size()
	if d.size < 4 {
		return nil, fmt.Errorf("compressed file is too short: %d", d.size)
	}

	if d.mmapHandle1, d.mmapHandle2, err = mmap.Mmap(d.f, int(d.size)); err != nil {
		return nil, err
	}

	d.data = d.mmapHandle1[:d.size]

	offset := 0

	wordsCount, bytes_read := binary.Uvarint(d.data[offset:])
	offset += bytes_read
	d.wordsCount = wordsCount

	emptyWordsCount, bytes_read := binary.Uvarint(d.data[offset:])
	offset += bytes_read
	d.emptyWordsCount = emptyWordsCount

	numBlocks, bytes_read := binary.Uvarint(d.data[offset:])
	offset += bytes_read
	d.numBlocks = numBlocks

	maxWordSize, bytes_read := binary.Uvarint(d.data[offset:])
	offset += bytes_read
	d.maxWordSize = maxWordSize

	dict_size, bytes_read := binary.Uvarint(d.data[offset:])
	offset += bytes_read

	buf := make([]byte, dict_size)

	if dict_size > 0 {
		copy(buf, d.data[offset:offset+int(dict_size)])
	} else {
		buf = append(buf, 0)
	}

	offset += int(dict_size)

	// fmt.Println("WORDS_COUNT: ", wordsCount)
	// fmt.Println("EMPTY_WORDS_COUNT: ", emptyWordsCount)
	// fmt.Println("NUM_BLOCKS: ", numBlocks)
	// fmt.Println("MAX_WORD_SIZE: ", maxWordSize)
	// fmt.Println("DICT_SIZE: ", dict_size)

	cmp_dict_ptr := unsafe.Pointer(&buf[0])
	cmp_dict_size := C.int(dict_size)

	decoder := C.NewDecoder(
		C.ulonglong(wordsCount),
		C.int(numBlocks),
		(*C.uchar)(cmp_dict_ptr),
		cmp_dict_size,
		C.int(maxWordSize),
	)

	d.decoder = decoder
	// 	d.buf = make([]byte, max_word_size)

	d.blocksStart = uint64(offset)

	return d, nil
}

func (d *Decompressor) Size() int64 {
	return d.size
}

func (d *Decompressor) Close() error {

	defer C.DeleteDecoder(d.decoder)
	if err := mmap.Munmap(d.mmapHandle1, d.mmapHandle2); err != nil {
		return err
	}
	if err := d.f.Close(); err != nil {
		return err
	}
	return nil
}

func (d *Decompressor) FilePath() string { return d.compressedFile }

// WithReadAhead - Expect read in sequential order. (Hence, pages in the given range can be aggressively read ahead, and may be freed soon after they are accessed.)
func (d *Decompressor) WithReadAhead(f func() error) error {
	_ = mmap.MadviseSequential(d.mmapHandle1)
	defer mmap.MadviseRandom(d.mmapHandle1)
	return f()
}

func (d *Decompressor) Count() int           { return int(d.wordsCount) }
func (d *Decompressor) EmptyWordsCount() int { return int(d.emptyWordsCount) }

type tuple struct {
	offset int64
	size   int
}

// Getter represent "reader" or "interator" that can move accross the data of the decompressor
// The full state of the getter can be captured by saving dataP, and dataBit
type Getter struct {
	buf          []byte
	data         []byte
	blockOffsets []tuple
	wordOffsets  []int // starting point of a first word in each block

	currentBlock int   // for sequential decoding
	offset       int64 // for sequential and random access
	fName        string
	trace        bool

	decoder *C.Decoder
}

func (g *Getter) Trace(t bool) { g.trace = t }

func (g *Getter) nextPos(clean bool) uint64 {
	//
	return 0
}

func (g *Getter) nextPattern() []byte {
	return nil
}

func (g *Getter) Size() int {
	return len(g.data)
}

// MakeGetter creates an object that can be used to access superstrings in the decompressor's file
// Getter is not thread-safe, but there can be multiple getters used simultaneously and concurrently
// for the same decompressor
func (d *Decompressor) MakeGetter() *Getter {

	g := &Getter{
		buf:          make([]byte, d.maxWordSize),
		data:         d.data[d.blocksStart:],
		blockOffsets: make([]tuple, 0, d.numBlocks),
		fName:        d.compressedFile,
		decoder:      d.decoder,
	}

	left := d.size - int64(d.blocksStart)
	offset := int64(0)
	for offset < left {
		compressed_block_size, bytes_read := binary.Uvarint(g.data[offset:])
		offset += int64(bytes_read)
		// fmt.Printf("compressed_block_size: %d, offset: %d\n", compressed_block_size, offset)
		g.blockOffsets = append(g.blockOffsets,
			tuple{
				offset: offset,
				size:   int(compressed_block_size),
			},
		)
		offset += int64(compressed_block_size)
	}

	__assert_true(offset == left, "offset == uint64(d.size)")

	g.prepareBlocks() // prepare blocks for decoding (sequential and random access)

	g.offset = g.blockOffsets[0].offset

	return g
}

func (g *Getter) prepareBlocks() {

	var word_start int

	for _, val := range g.blockOffsets {
		offset := val.offset
		size := val.size
		dataPtr := unsafe.Pointer(&g.data[offset])

		word_start = int(C.PrepareNextBlock(
			g.decoder,
			(*C.uchar)(dataPtr),
			C.int(size),
			C.longlong(offset),
		))

		g.wordOffsets = append(g.wordOffsets, word_start)
	}

	// fmt.Println(g.wordOffsets)
}

// offset has to be a starting point of a word
func (g *Getter) Reset(offset uint64) {
	// unimplemented TODO
}

func (g *Getter) HasNext() bool {

	// TODO:
	// this should not have to call C function

	hasNext := int(C.HasNext(g.decoder))
	if hasNext == 1 {
		return true
	}
	return false
}

// Next extracts a compressed word from current offset in the file
// and appends it to the given buf, returning the result of appending
// After extracting next word, it moves to the beginning of the next one
// Sequential read
func (g *Getter) Next(buf []byte) ([]byte, uint64) {

	// TODO:
	// 1. Do not pass word_size to C function
	//    - instead create buffer of int16, so that -1 means the END_OF_WORD

	// 2. Next has to decode from the current offset, see `reset` method

	bufPtr := unsafe.Pointer(&g.buf[0])
	var word_size C.int // this will be moved to the heap
	offset := int64(C.Next(g.decoder, (*C.uchar)(bufPtr), &word_size))

	if offset == -1 {
		return nil, (1 << 63)
	}

	if buf != nil {
		if len(buf)+int(word_size) > cap(buf) {
			newBuf := make([]byte, len(buf)+int(word_size))
			copy(newBuf, buf)
			buf = newBuf
		}
		copy(buf, g.buf[:int(word_size)])
		return buf, uint64(offset)
	} else {

		if word_size == 0 {
			return buf, uint64(offset)
		}

		return g.buf[:int(word_size)], uint64(offset)
	}

}

func (g *Getter) NextUncompressed() ([]byte, uint64) {
	return g.Next(nil)
}

// Random read, use only when the exact starting offset of the word is known
func (g *Getter) NextAt(buf []byte, offset int64) ([]byte, uint64) {
	return nil, 0
}

// Skip moves offset to the next word and returns the new offset.
func (g *Getter) Skip() uint64 {

	bufPtr := unsafe.Pointer(&g.buf[0])
	var word_size C.int // this will be moved to the heap
	offset := int64(C.Next(g.decoder, (*C.uchar)(bufPtr), &word_size))

	if offset == -1 {
		return (1 << 63)
	}

	return uint64(offset)
}

func (g *Getter) SkipUncompressed() uint64 {
	return g.Skip()
}

// Match returns true and next offset if the word at current offset fully matches the buf
// returns false and current offset otherwise.
func (g *Getter) Match(buf []byte) (bool, uint64) {

	dst_ptr := unsafe.Pointer(&g.buf[0])
	var word_size C.int // this will be moved to the heap
	offset := int64(C.Next(g.decoder, (*C.uchar)(dst_ptr), &word_size))

	if len(buf) != int(word_size) {
		return false, uint64(offset)
	}

	for i := 0; i < int(word_size); i++ {
		if buf[i] != g.buf[i] {
			return false, uint64(offset)
		}
	}

	return true, uint64(offset)
}

// MatchPrefix only checks if the word at the current offset has a buf prefix. Does not move offset to the next word.
func (g *Getter) MatchPrefix(prefix []byte) bool {

	size := len(prefix)
	var prefix_ptr unsafe.Pointer
	if size > 0 {
		prefix_ptr = unsafe.Pointer(&prefix[0])
	}

	prefix_size := C.int(size)

	result := C.Match(g.decoder, (*C.uchar)(prefix_ptr), prefix_size)

	if result == 1 {
		return true
	}

	return false
}
