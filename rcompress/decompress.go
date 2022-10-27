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
	blocksStart     uint64
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
	startOffset uint64
	dataOffset  uint64
	size        int
}

// Getter represent "reader" or "interator" that can move accross the data of the decompressor
// The full state of the getter can be captured by saving dataP, and dataBit
type Getter struct {
	buf          []byte
	decoded      []int16
	data         []byte
	blockOffsets []tuple
	wordOffsets  []int // starting point of a first word in each block

	numBlocks    int
	currentBlock int    // for sequential decoding
	offset       uint64 // for sequential and random access

	fName string
	trace bool

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
		decoded:      make([]int16, d.maxWordSize),
		data:         d.data[d.blocksStart:],
		blockOffsets: make([]tuple, 0, d.numBlocks),
		fName:        d.compressedFile,
		decoder:      d.decoder,

		numBlocks: int(d.numBlocks),
	}

	left := uint64(d.size) - d.blocksStart
	offset := uint64(0)

	for offset < left {

		compressed_block_size, bytes_read := binary.Uvarint(g.data[offset:])
		startOffset := offset
		offset += uint64(bytes_read)
		dataOffset := offset
		size := int(compressed_block_size)
		// fmt.Printf("compressed_block_size: %d, offset: %d\n", compressed_block_size, offset)
		g.blockOffsets = append(g.blockOffsets, tuple{
			startOffset,
			dataOffset,
			size,
		})
		offset += uint64(compressed_block_size)
	}

	__assert_true(offset == left, "offset == uint64(d.size)")

	g.prepareBlocks() // prepare block for decoding (sequential and random access)

	g.offset = uint64(g.blockOffsets[0].startOffset)

	return g
}

func (g *Getter) prepareBlocks() {

	var word_start int

	for _, val := range g.blockOffsets {
		// startOffset := val.startOffset
		dataOffset := val.dataOffset
		size := val.size
		dataPtr := unsafe.Pointer(&g.data[dataOffset])

		word_start = int(C.PrepareNextBlock(
			g.decoder,
			(*C.uchar)(dataPtr),
			C.int(size),
			C.longlong(dataOffset),
		))

		g.wordOffsets = append(g.wordOffsets, word_start)
	}

	// fmt.Println(g.wordOffsets)
}

// offset has to be a starting point of a word
func (g *Getter) Reset(offset uint64) {
	g.offset = offset
}

func (g *Getter) HasNext() bool {
	if g.offset < uint64(len(g.data)) {
		return true
	}
	return false
}

// TODO: make this one more effective
func findBlockNum(g *Getter) int {
	var blockNum int

	for i, tup := range g.blockOffsets {

		if g.offset == tup.startOffset {
			g.offset = tup.dataOffset
			blockNum = i
			break
		}

		if g.offset < tup.startOffset {
			__assert_true(i > 0, "i > 0")
			blockNum = i - 1
			break
		}

		if g.offset > tup.dataOffset {
			blockNum = i
		}
	}

	return blockNum
}

// Next extracts a compressed word from current offset in the file
// and appends it to the given buf, returning the result of appending
// After extracting next word, it moves to the beginning of the next one
func (g *Getter) Next(buf []byte) ([]byte, uint64) {

	dPtr := unsafe.Pointer(&g.decoded[0])

	blockNum := findBlockNum(g)

	// int64_t NextAt(Decoder *decoder, int64_t offset, int block_num, short *dst)
	// returns next offset to the starting point of a word
	offset := int64(C.NextAt(g.decoder, (C.longlong)(g.offset), (C.int)(blockNum), (*C.short)(dPtr)))

	if offset == -1 {
		g.offset = 0
		return nil, (1 << 63)
	}

	// next starting offset (this has to be valid offset! otherwise behavior is unpredicted)
	g.offset = uint64(offset)

	g.buf = g.buf[:0]
	for _, v := range g.decoded {
		if v == -1 {
			break
		}
		__assert_true(v <= 255, "v <= 255")
		__assert_true(v >= 0, "v >= 0")
		g.buf = append(g.buf, byte(v))
	}

	if buf != nil {
		if cap(buf) < len(g.buf) {
			newBuf := make([]byte, len(g.buf))
			buf = newBuf
		}
		copy(buf, g.buf)
		return buf, g.offset
	} else {
		if len(g.buf) == 0 {
			return nil, g.offset
		}
		return g.buf, g.offset
	}

}

func (g *Getter) NextUncompressed() ([]byte, uint64) {
	return g.Next(nil)
}

// Skip moves offset to the next word and returns the new offset.
func (g *Getter) Skip() uint64 {

	dPtr := unsafe.Pointer(&g.decoded[0])

	blockNum := findBlockNum(g)

	// int64_t NextAt(Decoder *decoder, int64_t offset, int block_num, short *dst)
	// returns next offset to the starting point
	offset := int64(C.NextAt(g.decoder, (C.longlong)(g.offset), (C.int)(blockNum), (*C.short)(dPtr)))

	if offset == -1 {
		g.offset = 0
		return (1 << 63) // invalid offset
	}
	g.offset = uint64(offset)
	return uint64(g.offset)
}

func (g *Getter) SkipUncompressed() uint64 {
	return g.Skip()
}

// Match returns true and next offset if the word at current offset fully matches the buf
// returns false and current offset otherwise.
func (g *Getter) Match(buf []byte) (bool, uint64) {

	dPtr := unsafe.Pointer(&g.decoded[0])

	blockNum := findBlockNum(g)

	offset := int64(C.NextAt(g.decoder, (C.longlong)(g.offset), (C.int)(blockNum), (*C.short)(dPtr)))

	if offset == -1 {
		g.offset = 0
		return false, (1 << 63) // invalid offset
	}

	for i, v := range g.decoded {
		if v == -1 {
			break
		}
		__assert_true(v <= 255, "v <= 255")
		__assert_true(v >= 0, "v >= 0")

		if buf[i] != byte(v) {
			return false, g.offset
		}
	}

	g.offset = uint64(offset)
	return true, g.offset
}

// MatchPrefix only checks if the word at the current offset has a buf prefix. Does not move offset to the next word.
func (g *Getter) MatchPrefix(prefix []byte) bool {

	dPtr := unsafe.Pointer(&g.decoded[0])

	blockNum := findBlockNum(g)

	offset := int64(C.NextAt(g.decoder, (C.longlong)(g.offset), (C.int)(blockNum), (*C.short)(dPtr)))

	if offset == -1 {
		return false
	}

	if len(prefix) > len(g.decoded) {
		return false
	}

	for i := 0; i < len(prefix); i++ {

		v := g.decoded[i]

		if v == -1 {
			return false
		}

		__assert_true(v <= 255, "v <= 255")
		__assert_true(v >= 0, "v >= 0")

		if prefix[i] != byte(v) {
			return false
		}
	}

	return true
}
