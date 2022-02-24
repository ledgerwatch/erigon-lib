package main

import (
	"math/rand"

	"github.com/ledgerwatch/erigon-lib/compress"
)

const chars = "1234567890QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm"
const max_size = 62

func rand_word() []byte {
	min, max := 30, 100
	word_len := rand.Intn(max-min) + min
	b := make([]byte, word_len)
	for i := 0; i < word_len; i++ {
		b[i] = byte(chars[rand.Intn(max_size)])
	}
	return b
}

func main() {

	m := compress.NewMyDec("this_file")

	m.Close()

	// // f1_name := "compressed_cgo"
	// f2_name := "compressed_original"
	// max, min := 10_000, 1_000

	// // stop_at := time.Now().Add(time.Hour * 5)
	// times, runs := 0, 1
	// for {
	// 	if times == runs { // stop_at.Before(time.Now())

	// 		break
	// 	} else {
	// 		times++

	// 		/* ---------- Create test data ---------- */
	// 		size := rand.Intn(max-min) + min
	// 		words := make([][]byte, size)

	// 		fmt.Println("WORDS SIZE: ", size)

	// 		for i := 0; i < size; i++ {
	// 			words[i] = rand_word()
	// 		}

	// 		file2 := filepath.Join(".", f2_name)
	// 		c2, err := compress.NewCompressor(context.Background(), f2_name, file2, ".", 100, 4)
	// 		if err != nil {
	// 			fmt.Println("Error C2:", err)
	// 			return
	// 		}

	// 		now := time.Now()
	// 		for _, word := range words {
	// 			if err := c2.AddWord(word); err != nil {
	// 				fmt.Println("Error C1 addword: ", err)
	// 				return
	// 			}
	// 		}

	// 		if err := c2.Compress(); err != nil {
	// 			fmt.Println("Error C2 compress: ", err)
	// 			return
	// 		}
	// 		end := time.Since(now)
	// 		fmt.Println("GO compress: ", end.Milliseconds(), "ms")

	// 		c2.Close()

	// 		var d2 *compress.Decompressor
	// 		if d2, err = compress.NewDecompressor(file2); err != nil {
	// 			fmt.Println("Error D2: ", err)
	// 			return
	// 		}

	// 		g2 := d2.MakeGetter()
	// 		i := 0
	// 		for g2.HasNext() {
	// 			word, _ := g2.Next(nil)
	// 			expected := words[i]
	// 			if string(word) != string(expected) {
	// 				fmt.Printf("D2 Expected: %s, got: %s\n", expected, word)
	// 			}
	// 			i++
	// 		}
	// 		d2.Close()

	// 		os.Remove(f2_name)
	// 		fmt.Println("-----------------------------------------------")
	// 		time.Sleep(time.Millisecond * 300)
	// 	}

	// }

}
