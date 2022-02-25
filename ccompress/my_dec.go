package ccompress

/*
#include "decompress.h"

#include <stdlib.h>
*/
import "C"
import "unsafe"

// type huffmanNodePos struct {
// 	zero *huffmanNodePos
// 	one  *huffmanNodePos
// 	pos  uint64
// }

// type huffmanNodePattern struct {
// 	zero    *huffmanNodePattern
// 	one     *huffmanNodePattern
// 	pattern []byte
// }

// // Decompressor provides access to the superstrings in a file produced by a compressor
// type Decompressor struct {
// 	compressedFile string
// 	f              *os.File
// 	mmapHandle1    []byte                 // mmap handle for unix (this is used to close mmap)
// 	mmapHandle2    *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
// 	data           []byte                 // slice of correct size for the decompressor to work with
// 	dict           *huffmanNodePattern
// 	posDict        *huffmanNodePos
// 	wordsStart     uint64 // Offset of whether the superstrings actually start
// 	count          uint64
// 	size           int64
// }

type Dcmp struct {
	decompress (*C.decompress)
}

func NewDcmp(compressedFile string) *Dcmp {

	file_name := C.CString(compressedFile)
	defer C.free(unsafe.Pointer(file_name))

	decompress := C.init_decompressor(file_name)
	return &Dcmp{decompress}
}

func (d *Dcmp) MyFunc() int {
	return int(C.my_func(d.decompress))
}

func (d *Dcmp) Close() {
	C.close_decompressor(d.decompress)
}
