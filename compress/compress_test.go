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
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func checksum(fileName string) uint32 {
	h := crc32.NewIEEE()
	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err = io.Copy(h, f); err != nil {
		panic(err)
	}
	return h.Sum32()
}

func TestA(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	fn := filepath.Join(tmpDir, "test")
	targetFn := filepath.Join(tmpDir, "test_target")
	in := make([]byte, 1024)

	_ = ioutil.WriteFile(fn, in, 0755)
	err := Compress(ctx, "", fn, targetFn, tmpDir)
	require.NoError(t, err)
	r1 := checksum(targetFn)

	err = ParallelCompress(ctx, "", fn, targetFn, tmpDir, 1)
	require.NoError(t, err)
	r2 := checksum(targetFn)

	fmt.Printf("alex: %d, %d\n", r1, r2)
	_ = ioutil.WriteFile(fn, []byte{1, 2, 3, 4, 5}, 0755)
}

func TestCompressEmptyDict(t *testing.T) {
	tmpDir := t.TempDir()
	file := path.Join(tmpDir, "compressed")
	c, err := NewCompressor(t.Name(), file, tmpDir, 100)
	if err != nil {
		t.Fatal(err)
	}
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
	defer d.Close()
}

func TestCompressDict1(t *testing.T) {
	tmpDir := t.TempDir()
	file := path.Join(tmpDir, "compressed")
	t.Name()
	c, err := NewCompressor(t.Name(), file, tmpDir, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err = c.AddWord([]byte(fmt.Sprintf("longlongword %d", i))); err != nil {
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
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		word, _ := g.Next(nil)
		expected := fmt.Sprintf("longlongword %d", i)
		if string(word) != expected {
			t.Errorf("expected %s, got (hex) %x", expected, word)
		}
		i++
	}
	defer d.Close()
}
