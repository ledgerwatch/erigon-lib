package compress

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/klauspost/compress"
	"github.com/klauspost/compress/huff0"
	"github.com/ledgerwatch/log/v3"
)

func BenchmarkDec2(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "dict")
	_ = os.RemoveAll(dir)
	_ = os.Remove(dir + "/1.dict")
	_ = os.Mkdir(dir, 0777)
	for i := 0; i < 100; i++ {
		for k, w := range loremStrings {
			x := []byte(fmt.Sprintf("%s_%d", w, i))
			if err := os.WriteFile(fmt.Sprintf(dir+"/%d_%d.txt", i, k), x, os.ModePerm); err != nil {
				panic(err)
			}
		}
	}
	cmd := exec.Command("zstd", "--train", "-o", "1.dict", "--maxdict=10240", "-r", dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("%s\n", out)
		panic(err)
	}
	bb, err := os.ReadFile("1.dict")
	if err != nil {
		panic(err)
	}

	s := &huff0.Scratch{OutData: make([]byte, 128), OutTable: make([]byte, 128), Out: make([]byte, 128), MaxDecodedSize: 128}
	enc, bb, err := huff0.ReadTable(bb[8:], s)
	if err != nil {
		panic(err)
	}
	enc.Reuse = huff0.ReusePolicyAllow

	//todo: Support ErrUseRLE
	//todo: Support ErrIncompressible
	res := []byte{}
	offsets := []uint64{}

	for i := 0; i < 1000; i++ {
		for _, w := range loremStrings {
			d := []byte(fmt.Sprintf("%s_%d_%s_%d_%s_%d", w, i, w, i, w, i))
			out, _, err := huff0.Compress1X(d, enc)
			if err != nil {
				if errors.Is(err, huff0.ErrIncompressible) {
					out = d
				} else {
					panic(fmt.Sprintf("%s, %s", d, err))
				}
			}
			res = append(res, out...)
			offsets = append(offsets, uint64(len(res)))
		}
	}

	//fmt.Printf("a: %d\n", len(offsets))
	s.Reuse = huff0.ReusePolicyMust
	var remain []byte

	b.Run("all", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var prev uint64
			for _, o := range offsets {
				s, remain, err = huff0.ReadTable(res[prev:o], s)
				s.Out = s.Out[:0:s.MaxDecodedSize]
				dec := s.Decoder()
				_, err := dec.Decompress1X(s.Out, remain)
				if err != nil {
					panic(err)
				}
				prev = o
			}
		}
	})
	b.Run("one", func(b *testing.B) {
		s, remain, err = huff0.ReadTable(res[offsets[0]:offsets[1]], s)
		dec := s.Decoder()
		for i := 0; i < b.N; i++ {
			s.Out = s.Out[:0:s.MaxDecodedSize]
			_, err := dec.Decompress1X(s.Out, remain)
			if err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkDec1(b *testing.B) {
	logger := log.New()
	tmpDir := b.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	b.Name()
	c, err := NewCompressor(context.Background(), b.Name(), file, tmpDir, 1, 2, log.LvlDebug, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 1000; i++ {
		for _, w := range loremStrings {
			d := []byte(fmt.Sprintf("%s_%d_%s_%d_%s_%d", w, i, w, i, w, i))
			if err = c.AddWord(d); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err = c.Compress(); err != nil {
		b.Fatal(err)
	}
	c.Close()

	var d *Decompressor
	if d, err = NewDecompressor(file); err != nil {
		b.Fatal(err)
	}

	var bb []byte

	b.Run("all", func(b *testing.B) {
		g := d.MakeGetter()
		for i := 0; i < b.N; i++ {
			g.Reset(0)
			for g.HasNext() {
				bb, _ = g.Next(bb[:0])
			}
		}
	})
	b.Run("one", func(b *testing.B) {
		g := d.MakeGetter()
		for i := 0; i < b.N; i++ {
			bb, _ = g.Next(bb[:0])
			if !g.HasNext() {
				g.Reset(0)
			}
		}
	})
}
