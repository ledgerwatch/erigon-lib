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
			// check for existing prefix match
			prefix := expected[:len(expected)/2]
			if len(expected) > 3 {
				prefix = expected[:2]
			}

			if !g.MatchPrefix(prefix) {
				key, _ := g.Next(nil)
				t.Fatalf("expected match prefix: %v with key: %v\n", prefix, key)
			}

			// check key for full match
			result := g.Match(expected)
			if result != 0 {
				key, _ := g.Next(nil)
				t.Fatalf("expected match: %v with key: %v\n", expected, key)
			}

			g.Reset(dataP)

			notExpected := a[word_idx]
			if len(notExpected) > 0 {
				notExpected[0]++
			} else {
				notExpected = []byte{1, 2, 3}
			}

			if len(notExpected) > 1 {
				prefix = notExpected[:len(notExpected)/2]
			} else {
				prefix = notExpected
			}

			// check for non existing  prefix match
			if g.MatchPrefix(prefix) {
				key, _ := g.Next(nil)
				t.Fatalf("notExpected full: %v\n not expected match prefix: %v with key: %v\n", notExpected, prefix, key)
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
	})
}
