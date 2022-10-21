package rcompress

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ledgerwatch/erigon-lib/etl"
)

type CompressionRatio float64

func (r CompressionRatio) String() string { return fmt.Sprintf("%.2f", r) }

func Ratio(f1, f2 string) (CompressionRatio, error) {
	s1, err := os.Stat(f1)
	if err != nil {
		return 0, err
	}
	s2, err := os.Stat(f2)
	if err != nil {
		return 0, err
	}
	return CompressionRatio(float64(s1.Size()) / float64(s2.Size())), nil
}

type AppendFile struct {
	filePath string
	f        *os.File
	w        *bufio.Writer
	count    uint64
	buf      []byte
}

func NewAppendFile(filePath string) (*AppendFile, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, etl.BufIOSize)
	return &AppendFile{filePath: filePath, f: f, w: w, buf: make([]byte, 128)}, nil
}

func OpenAppendFile(filePath string) (*AppendFile, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return &AppendFile{filePath: filePath, f: f, w: nil, buf: make([]byte, (1 << 24))}, nil
}

func (f *AppendFile) Close(remove bool) {
	f.w.Flush()
	//f.f.Sync()
	f.f.Close()
	if remove {
		os.Remove(f.filePath)
	}
}
func (f *AppendFile) Append(v []byte) error {
	f.count++

	// n := binary.PutUvarint(f.buf, 2*uint64(len(v)))
	n := binary.PutUvarint(f.buf, uint64(len(v)))
	if _, e := f.w.Write(f.buf[:n]); e != nil {
		return e
	}
	if len(v) > 0 {
		if _, e := f.w.Write(v); e != nil {
			return e
		}
	}
	return nil
}

func (f *AppendFile) AppendNoSize(v []byte) error {
	f.count++

	if len(v) > 0 {
		if _, e := f.w.Write(v); e != nil {
			return e
		}
	}
	return nil
}

// func (f *AppendFile) AppendUncompressed(v []byte) error {
// 	f.count++
// 	// For Append words, the length prefix is shifted to make lowest bit one
// 	n := binary.PutUvarint(f.buf, 2*uint64(len(v))+1)
// 	if _, e := f.w.Write(f.buf[:n]); e != nil {
// 		return e
// 	}
// 	if len(v) > 0 {
// 		if _, e := f.w.Write(v); e != nil {
// 			return e
// 		}
// 	}
// 	return nil
// }

// ForEach - Read keys from the file and generate superstring (with extra byte 0x1 prepended to each character, and with 0x0 0x0 pair inserted between keys and values)
// We only consider values with length > 2, because smaller values are not compressible without going into bits
func (f *AppendFile) ForEach(walker func(v []byte) error) error {
	_, err := f.f.Seek(0, 0)
	if err != nil {
		return err
	}
	r := bufio.NewReaderSize(f.f, etl.BufIOSize)
	buf := make([]byte, 4096)
	l, e := binary.ReadUvarint(r)
	for ; e == nil; l, e = binary.ReadUvarint(r) {
		// extract lowest bit of length prefix as "Append" flag and shift to obtain correct length
		// compressed := (l & 1) == 0
		// l >>= 1
		if len(buf) < int(l) {
			buf = make([]byte, l)
		}
		if _, e = io.ReadFull(r, buf[:l]); e != nil {
			return e
		}

		if err := walker(buf[:l]); err != nil {
			return err
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}

	return nil
}

func (f *AppendFile) read(offset int64, buf []byte) (int, error) {

	_, err := f.f.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

	r := bufio.NewReaderSize(f.f, etl.BufIOSize)
	l, e := binary.ReadUvarint(r)
	if e != nil {
		return 0, e
	}
	// l >>= 1
	return io.ReadFull(r, buf[:l])
}
