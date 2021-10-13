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
	"encoding/binary"
	"os"
)

// Decompressor provides access to the words in a file produced by a compressor
type Decompressor struct {
	compressedFile string
	f              *os.File
	mmapHandle1    []byte            // mmap handle for unix (this is used to close mmap)
	mmapHandle2    *[maxMapSize]byte // mmap handle for windows (this is used to close mmap)
	data           []byte            // slice of correct size for the decompressor to work with
	dict           Dictionary
	posDict        Dictionary
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
	d.dict.rootOffset = binary.BigEndian.Uint64(d.data[8:16])
	d.dict.cutoff = binary.BigEndian.Uint64(d.data[16:24])
	d.dict.data = d.data[24 : 24+dictSize]
	pos := 24 + dictSize
	dictSize = binary.BigEndian.Uint64(d.data[pos : pos+8])
	d.posDict.rootOffset = binary.BigEndian.Uint64(d.data[pos+8 : pos+16])
	d.posDict.cutoff = binary.BigEndian.Uint64(d.data[pos+16 : pos+24])
	d.posDict.data = d.data[pos+24 : pos+24+dictSize]
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
	data        []byte
	dataP       int
	patternDict *Dictionary
	posDict     *Dictionary
	offset      uint64
	b           byte
	mask        byte
}

func (ds *DictionaryState) zero() bool {
	ds.offset, _ = binary.Uvarint(ds.patternDict.data[ds.offset:])
	return ds.offset < ds.patternDict.cutoff
}

func (ds *DictionaryState) one() bool {
	_, n := binary.Uvarint(ds.patternDict.data[ds.offset:])
	ds.offset, _ = binary.Uvarint(ds.patternDict.data[ds.offset+uint64(n):])
	return ds.offset < ds.patternDict.cutoff
}

func (ds *DictionaryState) posZero() bool {
	ds.offset, _ = binary.Uvarint(ds.posDict.data[ds.offset:])
	return ds.offset < ds.posDict.cutoff
}

func (ds *DictionaryState) posOne() bool {
	_, n := binary.Uvarint(ds.posDict.data[ds.offset:])
	ds.offset, _ = binary.Uvarint(ds.posDict.data[ds.offset+uint64(n):])
	return ds.offset < ds.posDict.cutoff
}

func (ds *DictionaryState) pattern() []byte {
	l, n := binary.Uvarint(ds.patternDict.data[ds.offset:])
	return ds.patternDict.data[ds.offset+uint64(n) : ds.offset+uint64(n)+l]
}

func (ds *DictionaryState) pos() uint64 {
	pos, _ := binary.Uvarint(ds.posDict.data[ds.offset:])
	return pos
}

func (ds *DictionaryState) NextPos(clean bool) uint64 {
	if clean {
		ds.mask = 0
	}
	ds.offset = ds.posDict.rootOffset
	for {
		if ds.mask == 0 {
			ds.mask = 1
			ds.b = ds.data[ds.dataP]
			ds.dataP++
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
	return ds.pos()
}

func (ds *DictionaryState) NextPattern() []byte {
	ds.offset = ds.patternDict.rootOffset
	for {
		if ds.mask == 0 {
			ds.mask = 1
			ds.b = ds.data[ds.dataP]
			ds.dataP++
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
	return ds.pattern()
}

// Decompress extracts a compressed word from given offset in the file
// and appends it to the given buf, returning the result of appending
func (d Decompressor) Decompress(offset uint64, buf []byte) ([]byte, error) {
	ds := DictionaryState{patternDict: &d.dict, posDict: &d.posDict, data: d.data[offset:], dataP: 0}
	uncovered := make([]int, 0, 256)
	var word []byte
	l := ds.NextPos(true)
	if l > 0 {
		if int(l) > len(word) {
			word = make([]byte, l)
		}
		var pos uint64
		var lastPos int
		var lastUncovered int
		uncovered = uncovered[:0]
		for pos = ds.NextPos(false /* clean */); pos != 0; pos = ds.NextPos(false) {
			intPos := lastPos + int(pos) - 1
			lastPos = intPos
			var pattern []byte
			pattern = ds.NextPattern()
			copy(word[intPos:], pattern)
			if intPos > lastUncovered {
				uncovered = append(uncovered, lastUncovered, intPos)
			}
			lastUncovered = intPos + len(pattern)
		}
		if int(l) > lastUncovered {
			uncovered = append(uncovered, lastUncovered, int(l))
		}
		// Uncovered characters
		offset += uint64(ds.dataP)
		for i := 0; i < len(uncovered); i += 2 {
			copy(word[uncovered[i]:uncovered[i+1]], d.data[offset:])
			offset += uint64(uncovered[i+1] - uncovered[i])
		}
		buf = append(buf, word[:l]...)
	}
	return buf, nil
}
