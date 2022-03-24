package ccompress

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/compress"
)

const MIN = 1    // min input
const MAX = 5000 // max input

func rand_word() []byte {
	word_len := rand.Intn(MAX-MIN) + MIN
	_bytes := make([]byte, word_len)

	min_byte := rand.Intn(127)
	max_byte := rand.Intn(255-127) + 127

	for i := 0; i < word_len; i++ {
		_bytes[i] = byte(rand.Intn(max_byte-min_byte) + min_byte)
	}

	return _bytes
}

func TestCompressSimple(t *testing.T) {
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 100, 1)
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
		t.Fatalf("expeced word, got: %s", string(word))
	}
	if g.HasNext() {
		t.Fatalf("not expecting anything else")
	}
}

func prepareDict(t *testing.T) *Decompressor {
	tmpDir := t.TempDir()
	file := path.Join(tmpDir, "compressed")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
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
	return d
}

func TestCompressDict1(t *testing.T) {
	d := prepareDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		word, _ := g.Next(nil)
		expected := fmt.Sprintf("longlongword %d", i)
		if string(word) != expected {
			t.Errorf("expected %s, got (hex) %s", expected, word)
		}
		i++
	}
}

func prepareBytes(num_words int) [][]byte {

	data := make([][]byte, num_words)
	for i := 0; i < num_words; i += 1 {
		data[i] = rand_word()
	}
	return data
}

func TestComparison(t *testing.T) {

	num_words := 1000

	data := prepareBytes(num_words)

	fmt.Println("------------ Data prepared ------------")
	fmt.Println()

	tmpDir := t.TempDir()

	/* ---------------- ORIGINAL COMPRESSOR ---------------- */
	file := path.Join(tmpDir, "compressed_original")
	c_o, err := compress.NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c_o.Close()

	start := time.Now()
	for _, v := range data {
		if err = c_o.AddWord(v); err != nil {
			t.Fatal(err)
		}
	}
	duration := time.Since(start)
	fmt.Printf("Original: AddWord - %dms\n", duration.Milliseconds())

	start = time.Now()
	if err = c_o.Compress(); err != nil {
		t.Fatal(err)
	}
	duration = time.Since(start)
	fmt.Printf("Original: Compress - %dms\n", duration.Milliseconds())
	// original decompressor
	var d_o *compress.Decompressor
	if d_o, err = compress.NewDecompressor(file); err != nil {
		t.Fatal(err)
	}
	defer d_o.Close()

	g := d_o.MakeGetter()
	i := 0
	start = time.Now()
	for g.HasNext() {
		word, _ := g.Next(nil)
		expected := data[i]
		if string(word) != string(expected) {
			t.Errorf("Original: expected %s, got (hex) %s", expected, word)
		}
		i++
	}
	duration = time.Since(start)
	fmt.Printf("Original: Next - %dms\n", duration.Milliseconds())
	if i != num_words {
		t.Errorf("Original: expected num words: %d, got %d", num_words, i)
	}

	fmt.Printf("File Size Original: %d\n", d_o.Size())
	fmt.Println()

	/* ---------------- NEW COMPRESSOR ---------------- */
	file_n := path.Join(tmpDir, "compressed_new")
	c_n, err := NewCompressor(context.Background(), t.Name(), file_n, tmpDir, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c_n.Close()
	start = time.Now()
	for _, v := range data {
		if err = c_n.AddWord(v); err != nil {
			t.Fatal(err)
		}
	}
	duration = time.Since(start)
	fmt.Printf("New: AddWord - %dms\n", duration.Milliseconds())
	start = time.Now()
	if err = c_n.Compress(); err != nil {
		t.Fatal(err)
	}
	duration = time.Since(start)
	fmt.Printf("New: Compress - %dms\n", duration.Milliseconds())

	var d_n *Decompressor
	if d_n, err = NewDecompressor(file_n); err != nil {
		t.Fatal(err)
	}
	defer d_n.Close()

	g_n := d_n.MakeGetter()
	i = 0
	start = time.Now()
	for g_n.HasNext() {
		word, _ := g_n.Next(nil)
		expected := data[i]
		if string(word) != string(expected) {
			fmt.Printf("Expected size: %d, got: %d\n", len(data[i]), len(word))
			t.Errorf("New: expected %v, got (hex) %v", expected, word)
		}
		i++
	}
	duration = time.Since(start)
	fmt.Printf("New: Next - %dms\n", duration.Milliseconds())
	if i != num_words {
		t.Errorf("New: expected num words: %d, got %d", num_words, i)
	}

	fmt.Printf("File Size New: %d\n", d_n.Size())
	fmt.Println()

	t.Errorf("This has to fail!\n")
}
