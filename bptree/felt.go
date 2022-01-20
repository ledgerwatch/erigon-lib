package bptree

import (
	"crypto/sha256"
	"encoding/binary"
)

type Felt uint64

func (v *Felt) Binary() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(*v))
	return b
}

func hash2(bytes1, bytes2 []byte) []byte {
	hashBuilder := sha256.New()
	bytes1Written, _ := hashBuilder.Write(bytes1)
	ensure(bytes1Written == len(bytes1), "hash2: invalid number of bytes1 written")
	bytes2Written, _ := hashBuilder.Write(bytes2)
	ensure(bytes2Written == len(bytes2), "hash2: invalid number of bytes2 written")
	return hashBuilder.Sum(nil)
}

func deref(pointers []*Felt) []Felt {
	pointees := make([]Felt, 0)
	for _, ptr := range pointers {
		if ptr != nil {
			pointees = append(pointees, *ptr)
		} else {
			break
		}
	}
	return pointees
}
