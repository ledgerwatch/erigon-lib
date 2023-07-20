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
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestCompressEmptyDict(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 100, 1, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err = c.AddWord([]byte("word")); err != nil {
		t.Fatal(err)
	}
	if err = c.Compress(); err != nil {
		t.Fatal(err)
	}
	var d *Decompressor
	if d, err = NewDecompressor(file); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	g := d.MakeGetter()
	if !g.HasNext() {
		t.Fatalf("expected a word")
	}
	word, _ := g.Next(nil)
	if string(word) != "word" {
		t.Fatalf("expeced word, got (hex) %x", word)
	}
	if g.HasNext() {
		t.Fatalf("not expecting anything else")
	}
}

// nolint
func checksum(file string) uint32 {
	hasher := crc32.NewIEEE()
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err := io.Copy(hasher, f); err != nil {
		panic(err)
	}
	return hasher.Sum32()
}

func prepareDict(t *testing.T) *Decompressor {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 100; i++ {
		if err = c.AddWord(nil); err != nil {
			panic(err)
		}
		if err = c.AddWord([]byte("long")); err != nil {
			t.Fatal(err)
		}
		if err = c.AddWord([]byte("word")); err != nil {
			t.Fatal(err)
		}
		if err = c.AddWord([]byte(fmt.Sprintf("%d longlongword %d", i, i))); err != nil {
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

func TestCompressDict1(t *testing.T) {
	d := prepareDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	g.Reset(0)
	for g.HasNext() {
		// next word is `nil`
		cmp, _, nextOffset := g.MatchPrefix([]byte("long"), 0)
		require.False(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte(""), nextOffset)
		require.True(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte{}, nextOffset)
		require.True(t, cmp == 0)
		word, _ := g.Next(nil)
		require.Nil(t, word)

		// next word is `long`
		cmp, _, nextOffset = g.MatchPrefix([]byte("long"), nextOffset)
		require.True(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte("longlong"), nextOffset)
		require.False(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte("wordnotmatch"), nextOffset)
		require.False(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte("longnotmatch"), nextOffset)
		require.False(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte{}, nextOffset)
		require.True(t, cmp == 0)
		_, _ = g.Next(nil)

		// next word is `word`
		cmp, _, nextOffset = g.MatchPrefix([]byte("long"), nextOffset)
		require.False(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte("longlong"), nextOffset)
		require.False(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte("word"), nextOffset)
		require.True(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte(""), nextOffset)
		require.True(t, cmp == 0)
		cmp, _, _ = g.MatchPrefix(nil, nextOffset)
		require.True(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte("wordnotmatch"), nextOffset)
		require.False(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte("longnotmatch"), nextOffset)
		require.False(t, cmp == 0)
		_, _ = g.Next(nil)

		// next word is `longlongword %d`
		expectPrefix := fmt.Sprintf("%d long", i)

		cmp, _, nextOffset = g.MatchPrefix([]byte(fmt.Sprintf("%d", i)), nextOffset)
		require.True(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte(expectPrefix), nextOffset)
		require.True(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte(expectPrefix+"long"), nextOffset)
		require.True(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte(expectPrefix+"longword "), nextOffset)
		require.True(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte("wordnotmatch"), nextOffset)
		require.False(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte("longnotmatch"), nextOffset)
		require.False(t, cmp == 0)
		cmp, _, nextOffset = g.MatchPrefix([]byte{}, nextOffset)
		require.True(t, cmp == 0)
		word, _ = g.Next(nil)
		expected := fmt.Sprintf("%d longlongword %d", i, i)
		if string(word) != expected {
			t.Errorf("expected %s, got (hex) [%s]", expected, word)
		}
		i++
	}

	if cs := checksum(d.filePath); cs != 3153486123 {
		// it's ok if hash changed, but need re-generate all existing snapshot hashes
		// in https://github.com/ledgerwatch/erigon-snapshot
		t.Errorf("result file hash changed, %d", cs)
	}
}
