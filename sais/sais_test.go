package sais

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/sais/gsa"
	"github.com/stretchr/testify/assert"
)

func TestSais(t *testing.T) {
	data := []byte{4, 5, 6, 4, 5, 6, 4, 5, 6}
	sa := make([]int32, len(data))
	err := Sais(data, sa)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []int32{6, 3, 0, 7, 4, 1, 8, 5, 2}, sa)

	R := make([][]byte, 0, N)
	for i := 0; i < N; i++ {
		R = append(R, []byte("hihihi"))
	}
	superstring := make([]byte, 0, 1024)
	for _, a := range R {
		for _, b := range a {
			superstring = append(superstring, 1, b)
		}
		superstring = append(superstring, 0, 0)
	}

	//dictKey := make([]byte, 1024)
	//for i := range lcp {
	//	for s := 0; s < 6; s++ {
	//		dictKey[s] = superstring[(int(sa[i])+s)*2+1]
	//	}
	//	fmt.Printf("alex: %s\n", dictKey)
	//}

}

const N = 10_000

func BenchmarkName(b *testing.B) {
	R := make([][]byte, 0, N)
	for i := 0; i < N; i++ {
		R = append(R, []byte("helloworldalexagain"))
	}
	superstring := make([]byte, 0, 1024)

	for _, a := range R {
		for _, b := range a {
			superstring = append(superstring, 1, b)
		}
		superstring = append(superstring, 0, 0)
	}

	sa := make([]int32, len(superstring))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := Sais(superstring, sa)
		if err != nil {
			panic(err)
		}
	}
}
func BenchmarkName2(b *testing.B) {
	R := make([][]byte, 0, N)
	for i := 0; i < N; i++ {
		R = append(R, []byte("helloworldalexagain"))
	}
	str, n := gsa.ConcatAll(R)
	sa := make([]uint, gsa.SaSize(n))
	lcp := make([]int, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = gsa.GSA(str, sa, lcp, nil)
	}
}
