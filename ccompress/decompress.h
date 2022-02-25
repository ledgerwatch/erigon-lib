#ifndef _DECOMPRESS_H
#define _DECOMPRESS_H 1

#include <inttypes.h>
#include <stdio.h>

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

typedef unsigned char byte;

typedef struct decompress {
    int t;
    int fd;     // file descriptor
    byte *data; // mapped data

    uint64_t word_start;
    uint64_t count;
    size_t size;
} decompress;

extern decompress *init_decompressor(const char *file_name);
extern int my_func(decompress *d);

extern void close_decompressor(decompress *d);

#endif /* _DECOMPRESS_H*/