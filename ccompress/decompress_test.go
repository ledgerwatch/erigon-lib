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

package ccompress

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/mmap"
)

func prepareLoremDict(t *testing.T) *Decompressor {
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for k, w := range loremStrings {
		if err = c.AddWord([]byte(fmt.Sprintf("%s %d", w, k))); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.Compress(); err != nil {
		t.Fatal(err)
	}
	var d *Decompressor
	if d, err = NewDecompressor(file); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDecompressSkip(t *testing.T) {
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		if i%2 == 0 {
			g.Skip()
		} else {
			word, _ := g.Next(nil)
			expected := fmt.Sprintf("%s %d", w, i)
			if string(word) != expected {
				t.Errorf("expected %s, got (hex) %s", expected, word)
			}
		}
		i++
	}
}

func TestDecompressMatchOK(t *testing.T) {
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		if i%2 != 0 {
			expected := fmt.Sprintf("%s %d", w, i)
			ok, _ := g.Match([]byte(expected))
			if !ok {
				t.Errorf("expexted match with %s", expected)
			}
		} else {
			word, _ := g.Next(nil)
			expected := fmt.Sprintf("%s %d", w, i)
			if string(word) != expected {
				t.Errorf("expected %s, got (hex) %s", expected, word)
			}
		}
		i++
	}
}

func TestDecompressMatchNotOK(t *testing.T) {
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	skipCount := 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := fmt.Sprintf("%s %d", w, i+1)

		ok, _ := g.Match([]byte(expected))
		if ok {
			t.Errorf("not expexted match with %s", expected)
		} else {
			g.Skip()
			skipCount++
		}
		i++
	}
	if skipCount != i {
		t.Errorf("something wrong with match logic")
	}
}

func TestDecompressMatchPrefix(t *testing.T) {
	d := prepareDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	skipCount := 0
	for g.HasNext() {
		expected := fmt.Sprintf("longlongwords %d", i)
		l := len(expected)
		if i < l {
			l = i
		}
		ok := g.MatchPrefix([]byte(expected)[:l])
		expectedLen := 13
		switch {
		case ok && l < expectedLen: // good case
		case !ok && l >= expectedLen: // good case
		case ok && l >= expectedLen:
			t.Errorf("not expexted match prefix with %s", expected)
		case !ok && l < expectedLen:
			t.Errorf("not expexted not matched prefix with %s", expected)
		}
		g.Skip()
		skipCount++
		i++
	}
	if skipCount != i {
		t.Errorf("something wrong with match prefix logic")
	}
}

/* --------------------- TESTS ADDED -------------------- */

const LARGE_FILE_IN = "/mnt/mx500_0/goerli/snapshots/v1-004000-004500-transactions.seg" // ~1GB file

const DIR_OUT = "/mnt/500GB_HDD/"

func _rand_int_range(min, max int) int64 {
	return int64(rand.Intn(max-min) + min)
}

// maps large file and splits it to random number of words with sizes
// range from 1 to (65535 * 4) and compresses those words with ccompress
func prepareLargeDictC(t *testing.T) *Decompressor {

	file := filepath.Join(DIR_OUT, "compressed_cc")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, DIR_OUT, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	f, err := os.Open(LARGE_FILE_IN)
	if err != nil {
		t.Fatal(err)
	}
	var stat os.FileInfo
	if stat, err = f.Stat(); err != nil {
		t.Fatal(err)
	}
	size := stat.Size()
	var data1 []byte
	if data1, _, err = mmap.Mmap(f, int(size)); err != nil {
		t.Fatal(err)
	}

	num_words := size / 65535 // approximate total number of words
	max_word_size := 65535 * 4
	min_word_size := 1
	var start int64
	for i := 0; i < int(num_words); i++ {
		part := _rand_int_range(min_word_size, max_word_size)
		if start+part < size {
			if err = c.AddWord(data1[start : start+part]); err != nil {
				t.Fatal(err)
			}
			start += part
		} else {
			break
		}
	}

	if start < size {
		if err = c.AddWord(data1[start:]); err != nil {
			t.Fatal(err)
		}
	}

	if err = c.Compress(); err != nil {
		t.Fatal(err)
	}

	var d *Decompressor
	if d, err = NewDecompressor(file); err != nil {
		t.Fatal(err)
	}

	return d
}

// uses original compress.
// maps large file and splits it to random number of words with sizes
// range from 1 to (65535 * 4) and compresses those words with original
// compress
func prepareLargeDictOriginal(t *testing.T) *compress.Decompressor {

	file := filepath.Join(DIR_OUT, "compressed_original")
	t.Name()
	c, err := compress.NewCompressor(context.Background(), t.Name(), file, DIR_OUT, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	f, err := os.Open(LARGE_FILE_IN)
	if err != nil {
		t.Fatal(err)
	}
	var stat os.FileInfo
	if stat, err = f.Stat(); err != nil {
		t.Fatal(err)
	}
	size := stat.Size()
	var data1 []byte
	if data1, _, err = mmap.Mmap(f, int(size)); err != nil {
		t.Fatal(err)
	}

	num_words := size / 65535 // approximate total number of words
	max_word_size := 65535 * 4
	min_word_size := 1
	var start int64
	for i := 0; i < int(num_words); i++ {
		part := _rand_int_range(min_word_size, max_word_size)
		if start+part < size {
			if err = c.AddWord(data1[start : start+part]); err != nil {
				t.Fatal(err)
			}
			start += part
		} else {
			break
		}
	}

	if start < size {
		if err = c.AddWord(data1[start:]); err != nil {
			t.Fatal(err)
		}
	}

	if err = c.Compress(); err != nil {
		t.Fatal(err)
	}

	var d *compress.Decompressor
	if d, err = compress.NewDecompressor(file); err != nil {
		t.Fatal(err)
	}

	return d
}

func TestDecompressLargeNextC(t *testing.T) {
	d := prepareLargeDictC(t)
	defer d.Close()

	// output file for every word in compressed file
	file_to_compare := filepath.Join(DIR_OUT, "decompressed_cc")
	temp_f, err := os.Create(file_to_compare)
	if err != nil {
		t.Fatal(err)
	}

	g := d.MakeGetter()
	for g.HasNext() {
		word, _ := g.Next(nil)
		temp_f.Write(word)
	}

	temp_f.Close()

	// original file that was used in prepareLargeDict
	f, err := os.Open(LARGE_FILE_IN)
	if err != nil {
		t.Fatal(err)
	}
	var stat os.FileInfo
	if stat, err = f.Stat(); err != nil {
		t.Fatal(err)
	}
	size := stat.Size()
	var data1 []byte
	if data1, _, err = mmap.Mmap(f, int(size)); err != nil {
		t.Fatal(err)
	}

	// output file we wrote every decompressed word
	cmp_file, err := os.Open(file_to_compare)
	if err != nil {
		t.Fatal(err)
	}
	var stat2 os.FileInfo
	if stat2, err = cmp_file.Stat(); err != nil {
		t.Fatal(err)
	}
	size2 := stat2.Size()
	var data2 []byte
	if data2, _, err = mmap.Mmap(cmp_file, int(size2)); err != nil {
		t.Fatal(err)
	}

	// compare sizes first
	if size2 != size {
		t.Errorf("Expected size: %d, got size: %d\n", size, size2)
	}

	var i int64
	for ; i < size; i++ {
		// compare every byte in it
		if data1[i] != data2[i] {
			t.Errorf("Expected byte: %d, got byte: %d\n At index: %d", data1[i], data2[i], i)
		}
	}
}

func TestDecompressLargeNextOriginal(t *testing.T) {
	d := prepareLargeDictOriginal(t)
	defer d.Close()

	file_to_compare := filepath.Join(DIR_OUT, "decompressed_original")
	temp_f, err := os.Create(file_to_compare)
	if err != nil {
		t.Fatal(err)
	}

	g := d.MakeGetter()
	for g.HasNext() {
		word, _ := g.Next(nil)
		temp_f.Write(word)
	}

	temp_f.Close()

	f, err := os.Open(LARGE_FILE_IN)
	if err != nil {
		t.Fatal(err)
	}
	var stat os.FileInfo
	if stat, err = f.Stat(); err != nil {
		t.Fatal(err)
	}
	size := stat.Size()
	var data1 []byte
	if data1, _, err = mmap.Mmap(f, int(size)); err != nil {
		t.Fatal(err)
	}

	cmp_file, err := os.Open(file_to_compare)
	if err != nil {
		t.Fatal(err)
	}
	var stat2 os.FileInfo
	if stat2, err = cmp_file.Stat(); err != nil {
		t.Fatal(err)
	}
	size2 := stat2.Size()
	var data2 []byte
	if data2, _, err = mmap.Mmap(cmp_file, int(size2)); err != nil {
		t.Fatal(err)
	}

	if size2 != size {
		t.Errorf("Expected size: %d, got size: %d\n", size, size2)
	}

	var i int64
	for ; i < size; i++ {
		if data1[i] != data2[i] {
			t.Errorf("Expected byte: %d, got byte: %d\n At index: %d", data1[i], data2[i], i)
		}
	}
}

func TestSizeC(t *testing.T) {
	d := prepareLargeDictC(t)
	defer d.Close()
	t.Logf("Compressed size: %d", d.Size())
	t.Error("This test will fail")
}

func TestSizeOriginal(t *testing.T) {
	d := prepareLargeDictOriginal(t)
	defer d.Close()
	t.Logf("Compressed size: %d", d.Size())
	t.Error("This test will fail")
}

const lorem = `Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et
dolore magna aliqua Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
consequat Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur
Excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum`

var loremStrings = strings.Split(lorem, " ")
