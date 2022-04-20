package ccompress

/*
#include <stdlib.h>
#include "compressor.h"
*/
import "C"
import (
	"context"
	"fmt"
	"unsafe"
)

const MAX_WORD_SIZE = (1 << 24) - 1

type Compressor struct {
	cmp *C.CCompressor
}

func NewCompressor(ctx context.Context, logPrefix, outputFile, tmpDir string, minPatternScore uint64, workers int) (*Compressor, error) {
	c_out_file := C.CString(outputFile)

	defer func() {
		C.free(unsafe.Pointer(c_out_file))
	}()

	cmp := C.CNewCompressor(c_out_file)

	return &Compressor{cmp}, nil
}

func (c *Compressor) AddWord(word []byte) error {

	size := len(word)
	if size > MAX_WORD_SIZE {
		return fmt.Errorf("word is exceeding max word size limig")
	}

	C.CAddWord(c.cmp, (*C.uchar)(&word[0]), C.int(size))
	return nil
}

func (c *Compressor) Compress() error {
	C.CCompress(c.cmp)
	return nil
}

func (c *Compressor) Close() {
	C.CCloseCompressor(c.cmp)
}
