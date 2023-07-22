package compress

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/log/v3"
)

func FuzzDecompressMatch(f *testing.F) {
	logger := log.New()
	f.Fuzz(func(t *testing.T, x []byte, pos []byte, workers int8) {
		t.Helper()
		t.Parallel()
		if len(pos) < 1 || workers < 1 {
			t.Skip()
			return
		}
		var a [][]byte
		j := 0
		for i := 0; i < len(pos) && j < len(x); i++ {
			if pos[i] == 0 {
				continue
			}
			next := cmp.Min(j+int(pos[i]*10), len(x)-1)
			bbb := x[j:next]
			a = append(a, bbb)
			j = next
		}

		ctx := context.Background()
		tmpDir := t.TempDir()
		file := filepath.Join(tmpDir, fmt.Sprintf("compressed-%d", rand.Int31()))
		c, err := NewCompressor(ctx, t.Name(), file, tmpDir, 2, int(workers), log.LvlDebug, logger)
		if err != nil {
			t.Fatal(err)
		}
		c.DisableFsync()
		defer c.Close()
		for _, b := range a {
			if err = c.AddWord(b); err != nil {
				t.Fatal(err)
			}
		}
		if err = c.Compress(); err != nil {
			t.Fatal(err)
		}
		c.Close()
		d, err := NewDecompressor(file)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		g := d.MakeGetter()

		word_idx := 0
		// check for existing and non existing keys and prefixes
		for g.HasNext() {
			dataP := g.dataP
			expected := a[word_idx]

			notExpected := make([]byte, len(a[word_idx]))
			copy(notExpected, a[word_idx])

			if len(notExpected) > 0 {
				notExpected[0]++
			} else {
				notExpected = []byte{1, 2, 3}
			}

			prefix := expected
			suffix := expected

			same := true // if prefix and suffix are the same
			var min_size int

			if len(expected) > 1 {
				prefix = expected[:len(expected)/2]
				suffix = expected[len(expected)/2:]
			}

			if len(prefix) > len(suffix) {
				min_size = len(suffix)
			} else {
				min_size = len(prefix)
			}
			for i := 0; i < min_size; i++ {
				if suffix[i] != prefix[i] {
					same = false
					break
				}
			}

			// check for existing prefix match
			match, _, _ := g.MatchPrefix(prefix)
			if !match {
				key, _ := g.Next(nil)
				t.Fatalf("expected match prefix: %v with key: %v\n", prefix, key)
			}

			// check for existing suffix
			match, _, _ = g.MatchPrefix(prefix)
			if match { // suffix has to be equal to prefix
				if !same {
					t.Fatalf("MatchPrefix(suffix) expected match: prefix is unequal to suffix %v != %v, full slice %v\n", prefix, suffix, expected)
				}
			} else {
				if len(expected) < len(suffix) {
					t.Fatal("len(expected) < len(suffix), suffix has to be smaller then key")
				}
				same := true
				// key in file must not have the same prefix as suffix
				for i := 0; i < len(suffix); i++ {
					if suffix[i] != expected[i] {
						same = false
						break
					}
				}
				// if it does have all the same bytes then it's a bug
				if same {
					t.Fatalf("MatchPrefix(suffix) expected unmatch: prefix is equal to suffix %v != %v, full slice %v\n", prefix, suffix, expected)
				}
			}

			if len(suffix) < len(expected) {
				if g.Match(suffix) == 0 {
					t.Fatalf("Match(suffix) expected unmatch: suffix %v != %v\n", suffix, expected)
				}
			}

			// check key for full match
			result := g.Match(expected)
			if result != 0 {
				key, _ := g.Next(nil)
				t.Fatalf("expected match: %v with key: %v\n", expected, key)
			}

			g.Reset(dataP)

			if len(notExpected) > 1 {
				prefix = notExpected[:len(notExpected)/2]
				suffix = notExpected[len(notExpected)/2:]
			} else {
				prefix = notExpected
				suffix = notExpected
			}

			// check for non existing  prefix match
			match, _, _ = g.MatchPrefix(prefix)
			if match {
				key, _ := g.Next(nil)
				t.Fatalf("notExpected full: %v\n not expected match prefix: %v with key: %v\n", notExpected, prefix, key)
			}

			same = true // assume that prefix in file is the same as suffix
			if len(suffix) > len(expected) {
				min_size = len(expected)
			} else {
				min_size = len(suffix)
			}
			for i := 0; i < min_size; i++ {
				if suffix[i] != expected[i] {
					same = false
					break
				}
			}

			// check for non existing suffix
			if len(suffix) > 0 {
				match, _, _ = g.MatchPrefix(prefix)
				if match { // if there is a match
					if !same {
						// if suffix matched prefix
						// then suffix must be the same as prefix in file
						t.Fatalf("MatchPrefix(suffix) expected match: prefix is unequal to suffix %v != %v, full slice %v\n", prefix, suffix, notExpected)
					}
				}
			}

			if g.Match(suffix) == 0 {
				// if suffix is the same as key in file
				if len(suffix) != len(expected) {
					t.Fatalf("suffix matched key in file, but lengths a different: len(suffix) %v != len(expected) %v\n", len(suffix), len(expected))
				}

				for i := 0; i < len(expected); i++ {
					if suffix[i] != expected[i] {
						t.Fatalf("suffix matched key in file, but bytes are unequal suffix: %v != expected: %v\n", suffix, expected)
					}
				}

				g.Reset(dataP)
			}

			// check for non existing key
			result = g.Match(notExpected)
			if result == 0 {
				key, _ := g.Next(nil)
				t.Fatalf("notExpected full: %v\n not expected match: %v with key %v\n", notExpected, notExpected, key)
			}
			g.Skip()
			word_idx++
		}

		for i := 0; i < 10; i++ {
			if g.Match(randWord()) != -2 {
				t.Fatal("expected EOF")
			}
		}
	})

}
