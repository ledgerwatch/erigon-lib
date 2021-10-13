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
	"os"
)

// Decompressor provides access to the words in a file produced by a compressor
type Decompressor struct {
	compressedFile string
	f              *os.File
	mmap           []byte
	data           []byte
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
	var data *[maxMapSize]byte
	size := int(stat.Size())
	if d.mmap, data, err = mmap(d.f, size); err != nil {
		return nil, err
	}
	d.data = data[:size]

	return d, nil
}

func (d *Decompressor) Close() error {
	if err := munmap(d.mmap); err != nil {
		return err
	}
	if err := d.f.Close(); err != nil {
		return err
	}
	return nil
}
