package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	// "github.com/ledgerwatch/erigon-lib/compress"

	"github.com/ledgerwatch/erigon-lib/compress"
)

const chars = "1234567890QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm"
const max_size = 62

func rand_word() []byte {
	min, max := 4, 10
	word_len := rand.Intn(max-min) + min
	b := make([]byte, word_len)
	for i := 0; i < word_len; i++ {
		b[i] = byte(chars[rand.Intn(max_size)])
	}
	return b
}

func main() {

	f2_name := "compressed_original"
	// min, max := 10, 20

	// stop_at := time.Now().Add(time.Hour * 5)
	times, runs := 0, 1
	for {
		if times == runs { //  stop_at.Before(time.Now())

			break
		} else {
			times++

			// /* ---------- Create test data ---------- */
			// size := rand.Intn(max-min) + min
			// words := make([][]byte, size)

			// total_bytes := 0
			// for i := 0; i < size; i++ {
			// 	word := rand_word()
			// 	fmt.Println(string(word))
			// 	words[i] = word
			// 	total_bytes += len(word)
			// }

			// fmt.Println("TOTAL BYTES: ", total_bytes)

			/* ---------- Compress original ---------- */
			file2 := filepath.Join(".", f2_name)
			c2, err := compress.NewCompressor(context.Background(), f2_name, file2, ".", 100, 4)
			if err != nil {
				fmt.Println("Error C2:", err)
				return
			}

			buf := make([]byte, 756683)
			f, err := os.Open("./c_tests/11.html")
			if err != nil {
				fmt.Println(err)
				return
			}
			f.Read(buf)

			// // now := time.Now()
			// for _, word := range words {
			if err := c2.AddWord(buf); err != nil {
				fmt.Println("Error C2 addword: ", err)
				return
			}
			// }

			if err := c2.Compress(); err != nil {
				fmt.Println("Error C2 compress: ", err)
				return
			}
			// end := time.Since(now)
			// fmt.Println("GO compress: ", end.Milliseconds(), "ms")

			c2.Close()

			// /* ---------- Decompressor test ---------- */
			// var d2 *ccompress.Decompressor
			// if d2, err = ccompress.NewDecompressor(file2); err != nil {
			// 	fmt.Println("Error D2: ", err)
			// 	return
			// }

			// fmt.Println("Compressed file size: ", d2.Size())

			// g2 := d2.MakeGetter()
			// i := 0
			// for g2.HasNext() {
			// 	word, _ := g2.Next(nil)
			// 	expected := words[i]
			// 	if string(word) != string(expected) {
			// 		fmt.Printf("D2 Expected: %s, got: %s\n", expected, word)
			// 	}
			// 	i++
			// }
			// d2.Close()

			// os.Remove(f2_name)
			fmt.Println("-----------------------------------------------")
			time.Sleep(time.Millisecond * 300)
		}

	}

}

// f, err := os.Open("/mnt/mx500_0/goerli/mdbx.dat")
// defer f.Close()
// if err != nil {
// 	fmt.Println("ERR: ", err)
// 	return
// }

// fileinfo, err := f.Stat()
// if err != nil {
// 	fmt.Println(err)
// 	return
// }

// filesize := fileinfo.Size()
// buffer := make([]byte, filesize)

// bytesread, err := f.Read(buffer)
// if err != nil {
// 	fmt.Println(err)
// 	return
// }
// fmt.Println("bytes read: ", bytesread)

// file2 := filepath.Join(".", f2_name)
// c2, err := ccompress.NewCompressor(context.Background(), f2_name, file2, ".", 100, 4)
// if err != nil {
// 	fmt.Println("Error C2:", err)
// 	return
// }
// defer c2.Close()

// fmt.Println(buffer[:100])

// if err := c2.AddWord(buffer[:100_000]); err != nil {
// 	fmt.Println("Error C2 addword: ", err)
// 	return
// }

// if err := c2.Compress(); err != nil {
// 	fmt.Println("Error C2 compress: ", err)
// 	return
// }

// var d2 *ccompress.Decompressor
// if d2, err = ccompress.NewDecompressor(file2); err != nil {
// 	fmt.Println("Error D2: ", err)
// 	return
// }

// fmt.Println("Compressed file size: ", d2.Size())

// g2 := d2.MakeGetter()
// word, _ := g2.Next(nil)
// if len(word) != 100_000 {
// 	fmt.Println("unequal sizes")
// }
// d2.Close()
