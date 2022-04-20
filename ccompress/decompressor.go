package ccompress

/*
#include <stdlib.h>
#include "decompressor.h"
*/
import "C"
import (
	"unsafe"
)

type Decompressor struct {
	dcmp *C.CDecompressor
}

type Getter struct {
	buf  []byte
	dcmp *C.CDecompressor
}

func NewDecompressor(compressedFile string) (*Decompressor, error) {
	file_name := C.CString(compressedFile)

	defer func() {
		C.free(unsafe.Pointer(file_name))
	}()

	dcmp := C.CNewDecompressor(file_name)

	return &Decompressor{dcmp}, nil
}

func (d *Decompressor) Size() int64 {
	size := C.CSize(d.dcmp)
	return int64(size)
}

func (d *Decompressor) Close() error {
	C.CCloseDecompressor(d.dcmp)
	return nil
}

func (d *Decompressor) MakeGetter() *Getter {
	buf := make([]byte, MAX_WORD_SIZE)
	return &Getter{dcmp: d.dcmp, buf: buf}
}

// Next extracts a compressed word from current offset in the file
// and appends it to the given buf, returning the result of appending
// After extracting next word, it moves to the beginning of the next one
func (g *Getter) Next(buf []byte) ([]byte, uint64) {
	size := uint64(C.CNext(g.dcmp, (*C.uchar)(&g.buf[0])))
	if size > 0 {
		if buf != nil {
			copy(buf, g.buf[:size])
			return nil, size
		} else {
			return g.buf[:size], size
		}
	}
	return nil, 0
}

func (g *Getter) HasNext() bool {
	result := int(C.CHasNext(g.dcmp))
	return result == 1
}

func (g *Getter) Skip() uint64 {
	result := int(C.CSkip(g.dcmp))
	if result > 0 {
		return uint64(result)
	}
	return 0
}

// Match returns true and next offset if the word at current offset fully matches the buf
// returns false and current offset otherwise.
func (g *Getter) Match(buf []byte) (bool, uint64) {
	size := C.int(len(buf))
	result := uint64(C.CMatch(g.dcmp, (*C.uchar)(&buf[0]), size))
	if result == 1 {
		return true, 0
	}

	return false, 0
}

// MatchPrefix only checks if the word at the current offset has a buf prefix. Does not move offset to the next word.
func (g *Getter) MatchPrefix(buf []byte) bool {
	size := C.int(len(buf))
	if size > 0 {
		result := uint64(C.CMatchPrefix(g.dcmp, (*C.uchar)(&buf[0]), size))
		return result == 1
	}
	return true
}

func (g *Getter) Reset(offset uint64) {
	C.CReset(g.dcmp)
}
