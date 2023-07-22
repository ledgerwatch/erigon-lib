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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func prepareLoremDict(t *testing.T) *Decompressor {
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
			// ok, _ := g.Match([]byte(expected))
			result := g.Match([]byte(expected))
			if result != 0 {
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

func TestDecompressMatchOKUncompressed(t *testing.T) {
	d := prepareLoremDictUncompressed(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		if i%2 != 0 {
			expected := fmt.Sprintf("%s %d", w, i)
			// ok, _ := g.Match([]byte(expected))
			result := g.Match([]byte(expected))
			if result != 0 {
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

func prepareStupidDict(t *testing.T, size int) *Decompressor {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed2")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < size; i++ {
		if err = c.AddWord([]byte(fmt.Sprintf("word-%d", i))); err != nil {
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

func prepareStupidDictUncompressed(t *testing.T, size int) *Decompressor {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed2")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < size; i++ {
		if err = c.AddUncompressedWord([]byte(fmt.Sprintf("word-%d", i))); err != nil {
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

func TestDecompressMatchOKCondensed(t *testing.T) {
	condensePatternTableBitThreshold = 4
	d := prepareStupidDict(t, 10000)
	defer func() { condensePatternTableBitThreshold = 9 }()
	defer d.Close()

	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		if i%2 != 0 {
			expected := fmt.Sprintf("word-%d", i)
			// ok, _ := g.Match([]byte(expected))
			result := g.Match([]byte(expected))
			if result != 0 {
				t.Errorf("expexted match with %s", expected)
			}
		} else {
			word, _ := g.Next(nil)
			expected := fmt.Sprintf("word-%d", i)
			if string(word) != expected {
				t.Errorf("expected %s, got (hex) %s", expected, word)
			}
		}
		i++
	}
}

func TestDecompressMatchOKCondensedUncompressed(t *testing.T) {
	condensePatternTableBitThreshold = 4
	d := prepareStupidDictUncompressed(t, 10000)
	defer func() { condensePatternTableBitThreshold = 9 }()
	defer d.Close()

	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		if i%2 != 0 {
			expected := fmt.Sprintf("word-%d", i)
			// ok, _ := g.Match([]byte(expected))
			result := g.Match([]byte(expected))
			if result != 0 {
				t.Errorf("expexted match with %s", expected)
			}
		} else {
			word, _ := g.Next(nil)
			expected := fmt.Sprintf("word-%d", i)
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
		// ok, _ := g.Match([]byte(expected))
		result := g.Match([]byte(expected))
		if result == 0 {
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

func TestDecompressMatchNotOKUncompressed(t *testing.T) {
	d := prepareLoremDictUncompressed(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		if i%2 != 0 {
			expected := fmt.Sprintf("%s %d", w, i)
			// ok, _ := g.Match([]byte(expected))
			result := g.Match([]byte(expected))
			if result != 0 {
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

func TestDecompressMatchPrefix(t *testing.T) {
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	skipCount := 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := []byte(fmt.Sprintf("%s %d", w, i+1))
		expected = expected[:len(expected)/2]
		match, _, _ := g.MatchPrefix(expected)
		if !match {
			t.Errorf("expexted match with %s", expected)
		}
		g.Skip()
		skipCount++
		i++
	}
	if skipCount != i {
		t.Errorf("something wrong with match logic")
	}
	g.Reset(0)
	skipCount = 0
	i = 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := []byte(fmt.Sprintf("%s %d", w, i+1))
		expected = expected[:len(expected)/2]
		if len(expected) > 0 {
			expected[len(expected)-1]++
			match, _, _ := g.MatchPrefix(expected)
			if match {
				t.Errorf("not expexted match with %s", expected)
			}
		}
		g.Skip()
		skipCount++
		i++
	}
}

func TestDecompressMatchPrefixUncompressed(t *testing.T) {
	d := prepareLoremDictUncompressed(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	skipCount := 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := []byte(fmt.Sprintf("%s %d", w, i+1))
		expected = expected[:len(expected)/2]
		match, _, _ := g.MatchPrefix(expected)
		if !match {
			t.Errorf("expexted match with %s", expected)
		}
		g.Skip()
		skipCount++
		i++
	}
	if skipCount != i {
		t.Errorf("something wrong with match logic")
	}
	g.Reset(0)
	skipCount = 0
	i = 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := []byte(fmt.Sprintf("%s %d", w, i+1))
		expected = expected[:len(expected)/2]
		if len(expected) > 0 {
			expected[len(expected)-1]++
			match, _, _ := g.MatchPrefix(expected)
			if match {
				t.Errorf("not expexted match with %s", expected)
			}
		}
		g.Skip()
		skipCount++
		i++
	}
}

func prepareLoremDictUncompressed(t *testing.T) *Decompressor {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "uncompressed")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for k, w := range loremStrings {
		if err = c.AddUncompressedWord([]byte(fmt.Sprintf("%s %d", w, k))); err != nil {
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

func TestUncompressed(t *testing.T) {
	d := prepareLoremDictUncompressed(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := []byte(fmt.Sprintf("%s %d", w, i+1))
		expected = expected[:len(expected)/2]
		actual, _ := g.NextUncompressed()
		if bytes.Equal(expected, actual) {
			t.Errorf("expected %s, actual %s", expected, actual)
		}
		i++
	}
}

const lorem = `Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et
dolore magna aliqua Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
consequat Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur
Excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum`

var loremStrings = strings.Split(lorem, " ")

func TestDecompressTorrent(t *testing.T) {
	t.Skip()

	fpath := "/mnt/data/chains/mainnet/snapshots/v1-014000-014500-transactions.seg"
	st, err := os.Stat(fpath)
	require.NoError(t, err)
	fmt.Printf("file: %v, size: %d\n", st.Name(), st.Size())

	condensePatternTableBitThreshold = 9
	fmt.Printf("bit threshold: %d\n", condensePatternTableBitThreshold)
	d, err := NewDecompressor(fpath)

	require.NoError(t, err)
	defer d.Close()

	getter := d.MakeGetter()
	_ = getter

	for getter.HasNext() {
		_, sz := getter.Next(nil)
		// fmt.Printf("%x\n", buf)
		require.NotZero(t, sz)
	}
}

const N = 100

var WORDS = [N][]byte{}
var WORD_FLAGS = [N]bool{} // false - uncompressed word, true - compressed word
var INPUT_FLAGS = []int{}  // []byte or nil input

func randWord() []byte {
	size := rand.Intn(256) // size of the word
	word := make([]byte, size)
	for i := 0; i < size; i++ {
		word[i] = byte(rand.Intn(256))
	}
	return word
}

func generateRandWords() {
	for i := 0; i < N-2; i++ {
		WORDS[i] = randWord()
	}
	// make sure we have at least 2 emtpy []byte
	WORDS[N-2] = []byte{}
	WORDS[N-1] = []byte{}
}

func randIntInRange(min, max int) int {
	return (rand.Intn(max-min) + min)
}

func prepareRandomDict(t *testing.T) *Decompressor {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "complex")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	c.DisableFsync()
	defer c.Close()

	rand.Seed(time.Now().UnixNano())
	generateRandWords()

	idx := 0
	for idx < N {
		n := rand.Intn(2)
		switch n {
		case 0: // input case
			word := WORDS[idx]
			m := rand.Intn(2)
			if m == 1 {
				if err = c.AddWord(word); err != nil {
					t.Fatal(err)
				}
				WORD_FLAGS[idx] = true
			} else {
				if err = c.AddUncompressedWord(word); err != nil {
					t.Fatal(err)
				}
			}
			idx++
			INPUT_FLAGS = append(INPUT_FLAGS, n)
		case 1: // nil word
			if err = c.AddWord(nil); err != nil {
				t.Fatal(err)
			}
			INPUT_FLAGS = append(INPUT_FLAGS, n)
		default:
			t.Fatal(fmt.Errorf("case %d\n", n))
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

func TestDecompressRandomDict(t *testing.T) {
	d := prepareRandomDict(t)
	defer d.Close()

	if d.wordsCount != uint64(len(INPUT_FLAGS)) {
		t.Fatalf("TestDecompressRandomDict: d.wordsCount != len(INPUT_FLAGS)")
	}

	g := d.MakeGetter()

	word_idx := 0
	input_idx := 0
	total := 0
	var match bool
	// check for existing and non existing keys
	for g.HasNext() {
		pos := g.dataP
		if INPUT_FLAGS[input_idx] == 0 { // []byte input
			notExpected := string(WORDS[word_idx]) + "z"
			result := g.Match([]byte(notExpected))
			if result == 0 {
				t.Fatalf("not expected match: %s\n got: %s\n", notExpected, WORDS[word_idx])
			}

			expected := WORDS[word_idx]
			result = g.Match(expected)
			if result != 0 {
				g.Reset(pos)
				word, _ := g.Next(nil)
				t.Fatalf("expected match: %s\n got: %s\n", expected, word)
			}
			word_idx++
		} else { // nil input

			notExpected := []byte{0}
			result := g.Match(notExpected)
			if result == 0 {
				t.Fatal("not expected match []byte{0} with nil\n")
			}

			expected := []byte{}
			result = g.Match(nil)
			if result != 0 {
				g.Reset(pos)
				word, _ := g.Next(nil)
				t.Fatalf("expected match: %s\n got: %s\n", expected, word)
			}
		}
		input_idx++
		total++
	}
	if total != int(d.wordsCount) {
		t.Fatalf("expected word count: %d, got %d\n", int(d.wordsCount), total)
	}

	// TODO: check for non existing keys, suffixes, prefixes
	g.Reset(0)

	word_idx = 0
	input_idx = 0
	// check for existing and non existing prefixes
	var notExpected = []byte{2, 3, 4}
	for g.HasNext() {

		if INPUT_FLAGS[input_idx] == 0 { // []byte input
			expected := WORDS[word_idx]
			prefix_size := len(expected) / 2
			if len(expected)/2 > 3 {
				prefix_size = randIntInRange(3, len(expected)/2)
			}
			expected = expected[:prefix_size]
			if len(expected) > 0 {
				match, _, _ = g.MatchPrefix(expected)
				if !match {
					t.Errorf("expected match with %s", expected)
				}
				expected[len(expected)-1]++
				match, _, _ = g.MatchPrefix(expected)
				if match {
					t.Errorf("not expected match with %s", expected)
				}
			} else {
				match, _, _ := g.MatchPrefix([]byte{})
				if !match {
					t.Error("expected match with empty []byte")
				}
				match, _, _ = g.MatchPrefix(notExpected)
				if match {
					t.Error("not expected empty []byte to match with []byte{2, 3, 4}")
				}
			}
			word_idx++
		} else { // nil input
			match, _, _ = g.MatchPrefix(nil)
			if !match {
				t.Error("expected match with nil")
			}
			match, _, _ = g.MatchPrefix(notExpected)
			if match {
				t.Error("not expected nil to match with []byte{2, 3, 4}")
			}
		}

		g.Skip()
		input_idx++
	}

	g.Reset(0)

	word_idx = 0
	input_idx = 0
	// check for existing and non existing suffixes
	notExpected = []byte{2, 3, 4}
	for g.HasNext() {

		if INPUT_FLAGS[input_idx] == 0 { // []byte input
			suffix := WORDS[word_idx]
			if len(suffix) > 1 {
				prefix := suffix[:len(suffix)/2]
				suffix = suffix[len(suffix)/2:]
				equal := reflect.DeepEqual(prefix, suffix)
				// check existing suffixes
				match, _, _ = g.MatchPrefix(suffix)
				if match { // suffix has to be equal to prefix
					if !equal {
						t.Fatalf("MatchPrefix(suffix) expected match: prefix is unequal to suffix %v != %v, full slice %v\n", prefix, suffix, WORDS[word_idx])
					}
				} else { // suffix has not to be the same as prefix
					if equal {
						t.Fatalf("MatchPrefix(suffix) expected unmatch: prefix is equal to suffix %v != %v, full slice %v\n", prefix, suffix, WORDS[word_idx])
					}
				}

				if len(suffix) > 0 {
					suffix[0]++
					match, _, _ = g.MatchPrefix(suffix)
					if match {
						t.Fatalf("MatchPrefix(suffix) not expected match: prefix is unequal to suffix %v != %v, full slice %v\n", prefix, suffix, WORDS[word_idx])
					}
				}

				g.Skip()
			} else {
				if g.Match(suffix) != 0 {
					t.Fatal("Match(suffix): expected match suffix")
				}
			}
			word_idx++
		} else { // nil input
			match, _, _ = g.MatchPrefix(nil)
			if !match {
				t.Error("MatchPrefix(suffix): expected match with nil")
			}
			match, _, _ = g.MatchPrefix(notExpected)
			if match {
				t.Error("MatchPrefix(suffix): not expected nil to match with []byte{2, 3, 4}")
			}
			if g.Match(nil) != 0 {
				t.Errorf("Match(suffix): expected to match with nil")
			}
		}

		input_idx++
	}
}
