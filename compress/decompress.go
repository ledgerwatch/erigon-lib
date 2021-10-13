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
	"bufio"
	"encoding/binary"
	"os"
)

// Decompressor provides access to the words in a file produced by a compressor
type Decompressor struct {
	compressedFile string
	f              *os.File
	mmapHandle1    []byte                      // mmap handle for unix (this is used to close mmap)
	mmapHandle2    *[maxMapSize]byte           // mmap handle for windows (this is used to close mmap)
	data           []byte                      // slice of correct size for the decompressor to work with
	numBuf         [binary.MaxVarintLen64]byte // Buffer for producing var int deserialisation
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
	size := int(stat.Size())
	if d.mmapHandle1, d.mmapHandle2, err = mmap(d.f, size); err != nil {
		return nil, err
	}
	d.data = d.mmapHandle1[:size]
	dictSize := binary.BigEndian.Uint64(d.data[:8])
	var dict Dictionary
	dict.rootOffset = binary.BigEndian.Uint64(d.data[8:16])
	dict.cutoff = binary.BigEndian.Uint64(d.data[16:24])
	dict.data = d.data[24 : 24+dictSize]
	pos := 24 + dictSize
	dictSize = binary.BigEndian.Uint64(d.data[pos : pos+8])
	var posDict Dictionary
	posDict.rootOffset = binary.BigEndian.Uint64(d.data[pos+8 : pos+16])
	posDict.cutoff = binary.BigEndian.Uint64(d.data[pos+16 : pos+24])
	posDict.data = d.data[pos+24 : pos+24+dictSize]
	return d, nil
}

func (d *Decompressor) Close() error {
	if err := munmap(d.mmapHandle1, d.mmapHandle2); err != nil {
		return err
	}
	if err := d.f.Close(); err != nil {
		return err
	}
	return nil
}

type Dictionary struct {
	data       []byte
	rootOffset uint64
	cutoff     uint64
}

type DictionaryState struct {
	r      *bufio.Reader
	d      *Dictionary
	posD   *Dictionary
	offset uint64
	b      byte
	mask   byte
}

func (ds *DictionaryState) zero() bool {
	ds.offset, _ = binary.Uvarint(ds.d.data[ds.offset:])
	return ds.offset < ds.d.cutoff
}

func (ds *DictionaryState) one() bool {
	_, n := binary.Uvarint(ds.d.data[ds.offset:])
	ds.offset, _ = binary.Uvarint(ds.d.data[ds.offset+uint64(n):])
	return ds.offset < ds.d.cutoff
}

func (ds *DictionaryState) posZero() bool {
	ds.offset, _ = binary.Uvarint(ds.posD.data[ds.offset:])
	return ds.offset < ds.posD.cutoff
}

func (ds *DictionaryState) posOne() bool {
	_, n := binary.Uvarint(ds.posD.data[ds.offset:])
	ds.offset, _ = binary.Uvarint(ds.posD.data[ds.offset+uint64(n):])
	return ds.offset < ds.posD.cutoff
}

func (ds *DictionaryState) pattern() []byte {
	l, n := binary.Uvarint(ds.d.data[ds.offset:])
	return ds.d.data[ds.offset+uint64(n) : ds.offset+uint64(n)+l]
}

func (ds *DictionaryState) pos() uint64 {
	pos, _ := binary.Uvarint(ds.posD.data[ds.offset:])
	return pos
}

func (ds *DictionaryState) NextPos(clean bool) (uint64, error) {
	if clean {
		ds.mask = 0
	}
	ds.offset = ds.posD.rootOffset
	for {
		if ds.mask == 0 {
			ds.mask = 1
			var e error
			if ds.b, e = ds.r.ReadByte(); e != nil {
				return 0, e
			}
		}
		if ds.b&ds.mask == 0 {
			ds.mask <<= 1
			if ds.posZero() {
				break
			}
		} else {
			ds.mask <<= 1
			if ds.posOne() {
				break
			}
		}
	}
	return ds.pos(), nil
}

func (ds *DictionaryState) NextPattern() ([]byte, error) {
	ds.offset = ds.d.rootOffset
	for {
		if ds.mask == 0 {
			ds.mask = 1
			var e error
			if ds.b, e = ds.r.ReadByte(); e != nil {
				return nil, e
			}
		}
		if ds.b&ds.mask == 0 {
			ds.mask <<= 1
			if ds.zero() {
				break
			}
		} else {
			ds.mask <<= 1
			if ds.one() {
				break
			}
		}
	}
	return ds.pattern(), nil
}

// Decompress extracts a compressed word from given offset in the file
// and appends it to the given buf, returning the result of appending
func (d Decompressor) Decompress(offset uint64, buf []byte) ([]byte, error) {
	return nil, nil
}
