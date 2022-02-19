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

package etl

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

type dataProvider interface {
	Next() ([]byte, []byte, error)
	Dispose() uint64 // Safe for repeated call, doesn't return error - means defer-friendly
}

type fileDataProvider struct {
	file       *os.File
	reader     io.Reader
	byteReader io.ByteReader // Different interface to the same object as reader
	resultBuf  [][]byte
}

// FlushToDisk - `doFsync` is true only for 'critical' collectors (which should not loose).
func FlushToDisk(b Buffer, tmpdir string, doFsync, noLogs bool) (dataProvider, error) {
	if b.Len() == 0 {
		return nil, nil
	}
	// if we are going to create files in the system temp dir, we don't need any
	// subfolders.
	if tmpdir != "" {
		if err := os.MkdirAll(tmpdir, 0755); err != nil {
			return nil, err
		}
	}

	bufferFile, err := ioutil.TempFile(tmpdir, "erigon-sortable-buf-")
	if err != nil {
		return nil, err
	}
	if doFsync {
		defer bufferFile.Sync() //nolint:errcheck
	}

	w := bufio.NewWriterSize(bufferFile, BufIOSize)
	defer w.Flush() //nolint:errcheck

	defer func() {
		b.Reset() // run it after buf.flush and file.sync
		if !noLogs {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info(
				"Flushed buffer file",
				"name", bufferFile.Name(),
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
		}
	}()

	if err = b.Write(w); err != nil {
		return nil, fmt.Errorf("error writing entries to disk: %w", err)
	}

	return &fileDataProvider{file: bufferFile, reader: nil, resultBuf: make([][]byte, 2)}, nil
}

func (p *fileDataProvider) Next() ([]byte, []byte, error) {
	if p.reader == nil {
		_, err := p.file.Seek(0, 0)
		if err != nil {
			return nil, nil, err
		}
		r := bufio.NewReaderSize(p.file, BufIOSize)
		p.reader = r
		p.byteReader = r

	}
	return readElementFromDisk(p.resultBuf, p.reader, p.byteReader)
}

func (p *fileDataProvider) Dispose() uint64 {
	info, _ := os.Stat(p.file.Name())
	_ = p.file.Close()
	_ = os.Remove(p.file.Name())
	if info == nil {
		return 0
	}
	return uint64(info.Size())
}

func (p *fileDataProvider) String() string {
	return fmt.Sprintf("%T(file: %s)", p, p.file.Name())
}

func readElementFromDisk(resultBuf [][]byte, r io.Reader, br io.ByteReader) ([]byte, []byte, error) {
	n, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		return nil, nil, fmt.Errorf("buffer too small for key")
	} else if n < 0 {
		return nil, nil, fmt.Errorf("value overflow for key")
	}
	key := make([]byte, n)
	if _, err = io.ReadFull(r, key); err != nil {
		return nil, nil, err
	}
	n, err = binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		return nil, nil, fmt.Errorf("buffer too small for value")
	} else if n < 0 {
		return nil, nil, fmt.Errorf("value overflow for value")
	}
	value := make([]byte, n)
	if _, err = io.ReadFull(r, value); err != nil {
		return nil, nil, err
	}
	return key, value, err
}

type memoryDataProvider struct {
	buffer       Buffer
	currentIndex int
}

func KeepInRAM(buffer Buffer) dataProvider {
	return &memoryDataProvider{buffer, 0}
}

func (p *memoryDataProvider) Next() ([]byte, []byte, error) {
	if p.currentIndex >= p.buffer.Len() {
		return nil, nil, io.EOF
	}
	key, value := p.buffer.Get(p.currentIndex)
	p.currentIndex++
	return key, value, nil
}

func (p *memoryDataProvider) Dispose() uint64 {
	return 0 /* doesn't take space on disk */
}

func (p *memoryDataProvider) String() string {
	return fmt.Sprintf("%T(buffer.Len: %d)", p, p.buffer.Len())
}
