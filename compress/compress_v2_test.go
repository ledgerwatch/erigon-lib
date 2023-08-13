package compress

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	_ "github.com/klauspost/compress"
	"github.com/klauspost/compress/huff0"
)

func TestName(t *testing.T) {
	_ = os.Remove("1.dict")
	_ = os.Remove("1.dict")
	_ = os.RemoveAll("dict")
	_ = os.Mkdir("dict", os.ModePerm)

	if err := os.WriteFile("dict/1.txt", []byte(`aaaa`), os.ModePerm); err != nil {
		panic(err)
	}
	if err := os.WriteFile("dict/2.txt", []byte(`bbbb`), os.ModePerm); err != nil {
		panic(err)
	}
	if err := os.WriteFile("dict/3.txt", []byte(`cccc`), os.ModePerm); err != nil {
		panic(err)
	}

	cmd := exec.Command("zstd")
	cmd.Args = []string{"--train", "dict/*", "-o", "1.dict"}
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	b, err := os.ReadFile("1.dict")
	if err != nil {
		panic(err)
	}
	fmt.Printf("1.dict: %d, %s\n", len(b), b[8:])
	enc, b, err := huff0.ReadTable(b[8:], nil)
	if err != nil {
		panic(err)
	}
	enc.Reuse = huff0.ReusePolicyAllow
	//enc.WantLogLess = 5
	//enc, err := zstd.NewWriter(nil,zstd.WithEncoderConcurrency(1), zstd.WithEncoderDict(b))
	//if err != nil {
	//	panic(err)
	//}
	//
	//dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderDicts(b))
	//if err != nil {
	//	panic(err)
	//}

	//todo: Support ErrUseRLE
	//todo: Support ErrIncompressible
	data := [][]byte{[]byte(`aaaabbbbcccc`), []byte(`bbbbccccdddd`), []byte(`ccccddddeeee`), []byte(`ddddeeeeffff`)}
	res := []byte{}
	offsets := []uint64{}
	var off uint64
	for _, d := range data {
		out, _, err := huff0.Compress1X(d, enc)
		if err != nil {
			panic(fmt.Sprintf("%s, %s", d, err))
		}
		res = append(res, out...)
		off += uint64(len(out))
		offsets = append(offsets, off)
	}
	fmt.Printf("res: %d -> %d\n", len(data), len(res))
	dec := enc.Decoder()
	var prev uint64
	for _, o := range offsets {
		out, err := dec.Decompress1X(nil, res[prev:o])
		if err != nil {
			panic(err)
		}
		fmt.Printf("decompressed: %s\n", out)
		prev = o
	}
}
