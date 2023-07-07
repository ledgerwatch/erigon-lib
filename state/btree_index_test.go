package state

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestBt(t *testing.T) {
	cast := func(i uint64) []byte {
		k := make([]byte, 90)
		binary.LittleEndian.PutUint64(k, i)
		return k
	}
	cnt, M := 1_000_000, DefaultBtreeM
	bt := newBtAlloc(uint64(cnt), uint64(M), true)
	bt.traverseDfs()
	bt.dataLookup = func(kBuf, vBuf []byte, di uint64) ([]byte, []byte, error) {
		k := cast(di)
		return k, k, nil
	}
	bt.fillSearchMx()
	bt.trace = false

	for i := 0; i < cnt; i++ {
		//fmt.Printf("------ seek: %d\n", i)
		minD, maxD, _, found, err := bt.findNode(cast(uint64(i)))
		if err != nil {
			panic(err)
		}
		if found {
			continue
		}

		if maxD-minD > 2*uint64(M) {
			fmt.Printf("what(%d): %d, %d, %t\n", i, minD, maxD, found)
		}
	}
}
