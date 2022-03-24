package ccompress

/*
// #cgo CFLAGS: -g -Wall
#include <stdlib.h>
#include "c_api.h"
*/
import "C"
import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/mmap"
)

// const MIN = 1    // min input
// const MAX = 3000 // max inp

// func rand_word() []byte {
// 	word_len := rand.Intn(MAX-MIN) + MIN
// 	_bytes := make([]byte, word_len)

// 	min_byte := rand.Intn(127)
// 	max_byte := rand.Intn(255-127) + 127

// 	// fmt.Println(min_byte, max_byte)

// 	// min_byte :=
// 	// max_byte := 127

// 	for i := 0; i < word_len; i++ {
// 		_bytes[i] = byte(rand.Intn(max_byte-min_byte) + min_byte)
// 	}

// 	return _bytes
// }

// func prepareBytes(num_words int) [][]byte {
// 	data := make([][]byte, num_words)
// 	for i := 0; i < num_words; i += 1 {
// 		data[i] = rand_word()
// 	}
// 	return data
// }

/*----------------------- COMPRESSOR -----------------------*/

type Compressor struct {
	idt_path string
}

func NewCompressor(ctx context.Context, logPrefix, outputFile, tmpDir string, minPatternScore uint64, workers int) (*Compressor, error) {
	_, fileName := filepath.Split(outputFile)
	ext := filepath.Ext(fileName)
	uncompressedPath := filepath.Join(tmpDir, fileName[:len(fileName)-len(ext)]) + ".idt"
	out := C.CString(outputFile)
	idt := C.CString(uncompressedPath)
	r := int(C.new_compressor((*C.char)(out), (*C.char)(idt)))
	if r != 0 {
		return nil, fmt.Errorf("could not open file")
	}

	C.free(unsafe.Pointer(out))
	C.free(unsafe.Pointer(idt))
	return &Compressor{idt_path: uncompressedPath}, nil
}

func (c *Compressor) AddWord(word []byte) error {
	l := len(word)
	if l > 0x00FF_FFFF {
		return fmt.Errorf("max supported word size is: 0x00FFFFFF")
	}
	C.add_word((*C.uchar)(&word[0]), C.int(l))
	return nil
}

func (c *Compressor) Compress() error {
	C.compress()
	return nil
}

func (c *Compressor) Close() {
	C.close_compressor()
	os.Remove(c.idt_path)
}

/*----------------------- DECOMPRESSOR -----------------------*/

type Decompressor struct {
	compressedFile string
	f              *os.File
	mmapHandle1    []byte                 // mmap handle for unix (this is used to close mmap)
	mmapHandle2    *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	size           int64
	max_word       int
}

type Getter struct {
	buf []byte
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
	if d.size < 10 {
		return nil, fmt.Errorf("compressed file is too short")
	}
	if d.mmapHandle1, d.mmapHandle2, err = mmap.Mmap(d.f, int(d.size)); err != nil {
		return nil, err
	}

	result := int(C.new_decompressor((*C.uchar)(&d.mmapHandle1[0]), C.int(d.size)))
	if result == -1 {
		return nil, fmt.Errorf("compressed file is too short")
	}

	d.max_word = result

	return d, nil
}

func (d *Decompressor) Size() int64 {
	return d.size
}

func (d *Decompressor) Close() error {
	if err := mmap.Munmap(d.mmapHandle1, d.mmapHandle2); err != nil {
		return err
	}
	if err := d.f.Close(); err != nil {
		return err
	}

	C.close_decompressor()
	return nil
}

func (d *Decompressor) MakeGetter() *Getter {
	buf := make([]byte, d.max_word)
	return &Getter{buf}
}

// Next extracts a compressed word from current offset in the file
// and appends it to the given buf, returning the result of appending
// After extracting next word, it moves to the beginning of the next one
func (g *Getter) Next(buf []byte) ([]byte, uint64) {
	bytes_written := C.next((*C.uchar)(&g.buf[0]))
	return g.buf[:bytes_written], uint64(bytes_written)
}

func (g *Getter) HasNext() bool {
	if int(C.has_next()) == 1 {
		return true
	}
	return false
}

// func main() {

// 	f2_name := "compressed_original"
// 	file2 := filepath.Join(".", f2_name)
// 	c2, err := compress.NewCompressor(context.Background(), f2_name, file2, ".", 100, 4)
// 	if err != nil {
// 		fmt.Println("Error C2:", err)
// 		return
// 	}
// 	defer c2.Close()

// 	f1_name := "this_file"
// 	file1 := filepath.Join(".", f1_name)
// 	c1, err := NewCompressor(context.Background(), f2_name, file1, ".", 100, 4)
// 	if err != nil {
// 		fmt.Println("Error C1:", err)
// 		return
// 	}

// 	size := 100
// 	total_bytes := 0
// 	data := prepareBytes(size)

// 	for _, w := range data {
// 		c2.AddWord(w)
// 		c1.AddWord(w)
// 		total_bytes += len(w)
// 	}

// 	if err := c2.Compress(); err != nil {
// 		fmt.Println("Error C2 compress: ", err)
// 		return
// 	}

// 	if err := c1.Compress(); err != nil {
// 		fmt.Println("Error C2 compress: ", err)
// 		return
// 	}
// 	c1.Close()

// 	dcmp, err := NewDecompressor(f1_name)
// 	// fmt.Println("Created Decompressor")
// 	if err != nil {
// 		fmt.Println("Error: NewDecompressor")
// 	}

// 	getter := dcmp.MakeGetter()
// 	// fmt.Println(len(getter.buf))

// 	if !getter.HasNext() {
// 		fmt.Println("ERROR HAS_NEXT")
// 	}

// 	count := 0
// 	for getter.HasNext() {

// 		word, _ := getter.Next(nil)
// 		e_size := len(data[count])
// 		g_size := len(word)

// 		// s1 := string(data[count])
// 		// s2 := string(word)

// 		min := -1
// 		if e_size > g_size {
// 			min = g_size
// 		} else {
// 			min = e_size
// 		}

// 		for i := 0; i < min; i++ {
// 			if word[i] != data[count][i] {
// 				fmt.Printf("COUNT: %d, Expected: %d, Got: %d, idx: %d\n", count, data[count][i], word[i], i)
// 				fmt.Println(data[count])
// 				return
// 			}
// 		}

// 		// if e_size != g_size {
// 		// 	fmt.Printf("Expected size: %d, got: %d", e_size, g_size)

// 		// 	for i := 0; i < min; i++ {
// 		// 		e_b := data[count][i]
// 		// 		g_b := word[i]
// 		// 		if e_b != g_b {
// 		// 			fmt.Printf("Expected size: %d, got: %d", e_b, g_b)
// 		// 		}
// 		// 	}
// 		// }

// 		count++
// 	}
// 	if count != size {
// 		fmt.Printf("Expected: %d, got: %d\n", size, count)
// 	}
// 	fmt.Println("Total bytes: ", total_bytes)
// 	// i++
// 	// word, _ = getter.Next(nil)
// 	// fmt.Println(len(words[i]), len(word))
// 	// i++
// 	// word, _ = getter.Next(nil)
// 	// fmt.Println(len(words[i]), len(word))
// 	// for i := 0; i < size; i++ {

// 	// }

// 	// if words[0] != string(word) {
// 	// 	fmt.Printf("Expected: %s, Got: %s\n", words[0], string(word))
// 	// }
// 	// for i, v := range words {
// 	// 	fmt.Println(i, v)
// 	// }

// 	defer dcmp.Close()
// }
