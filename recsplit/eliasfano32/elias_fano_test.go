//go:build gofuzzbeta
// +build gofuzzbeta

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

package recsplit

import (
	"fmt"
	"math"
	"testing"
)

func TestSimple(t *testing.T) {
	v := []uint64{2, 3, 5, 7, 11, 13, 24}
	var minDelta uint64 = math.MaxUint64
	for i := range v {
		if i > 0 && v[i]-v[i-1] < minDelta {
			minDelta = v[i] - v[i-1]
		}
	}
	ef := NewEliasFano(uint64(len(v)), v[len(v)-1], minDelta)
	fmt.Printf("u,l, %d,%d\n", ef.u, ef.l)
	for _, p := range v {
		ef.AddOffset(p)
	}
	ef.Build()

	//ef.l
	//ef.u
	//ef.jump
	fmt.Printf("lowerBitsMask, %b\n", ef.lowerBitsMask)
	fmt.Printf("%b\n", ef.upperBits)
	fmt.Printf("%b\n", ef.lowerBits)
}

func TestName23(t *testing.T) {
	var in []uint64
	for i := uint64(0); i < 1_000_030; i += 7 {
		in = append(in, i)
	}
	numBuckets := len(in) / 2
	cumKeys := make([]uint64, numBuckets+1)
	var minDeltaCumKeys, minDeltaPosition uint64
	position := make([]uint64, numBuckets+1)
	for i, b := range in[:numBuckets] {
		cumKeys[i+1] = cumKeys[i] + uint64(b)
		if i == 0 || uint64(b) < minDeltaCumKeys {
			minDeltaCumKeys = uint64(b)
		}
	}

	for i, b := range in[numBuckets:] {
		position[i+1] = position[i] + uint64(b)
		if i == 0 || uint64(b) < minDeltaPosition {
			minDeltaPosition = uint64(b)
		}
	}
	ef2 := NewEliasFano(uint64(numBuckets+1), position[numBuckets], minDeltaPosition)
	for _, p := range position {
		ef2.AddOffset(p)
	}
	ef2.Build()
	for i := range position {
		_ = ef2.Get(uint64(i))
	}

}

func TestName22(t *testing.T) {
	// Treat each byte of the sequence as difference between previous value and the next
	var ef DoubleEliasFano
	in := []byte{0, 1, 2, 3}
	numBuckets := len(in) / 2
	cumKeys := make([]uint64, numBuckets+1)
	var minDeltaCumKeys, minDeltaPosition uint64
	position := make([]uint64, numBuckets+1)
	for i, b := range in[:numBuckets] {
		cumKeys[i+1] = cumKeys[i] + uint64(b)
		if i == 0 || uint64(b) < minDeltaCumKeys {
			minDeltaCumKeys = uint64(b)
		}
	}

	for i, b := range in[numBuckets:] {
		position[i+1] = position[i] + uint64(b)
		if i == 0 || uint64(b) < minDeltaPosition {
			minDeltaPosition = uint64(b)
		}
	}
	ef1 := NewEliasFano(uint64(numBuckets+1), cumKeys[numBuckets], minDeltaCumKeys)
	for _, c := range cumKeys {
		ef1.AddOffset(c)
	}
	ef1.Build()
	ef2 := NewEliasFano(uint64(numBuckets+1), position[numBuckets], minDeltaPosition)
	for _, p := range position {
		ef2.AddOffset(p)
	}
	ef2.Build()
	ef.Build(cumKeys, position)
	/*
		// Try to read from ef
		for bucket := 0; bucket < numBuckets; bucket++ {
			//cumKey, bitPos := ef.Get2(uint64(bucket))
			cumKey := ef1.Get(uint64(bucket))
			bitPos := ef2.Get(uint64(bucket))
		}
		for bucket := 0; bucket < numBuckets; bucket++ {
			cumKey, cumKeysNext, bitPos := ef.Get3(uint64(bucket))
		}
	*/

	/*
		in := []byte{1, 2, 3, 4, 5}
		ef1 := NewEliasFano(1, uint64(len(in)), 1)
		ef1.AddOffset(1)
		ef1.Build()
		o := ef1.Get(1)
		fmt.Printf("%d\n", o)
	*/
	/*
		ef2 := NewEliasFano(uint64(numBuckets+1), position[numBuckets], minDeltaPosition)
		for _, p := range position {
			ef2.AddOffset(p)
		}
		ef2.Build()
		ef.Build(cumKeys, position)
		// Try to read from ef
		for bucket := 0; bucket < numBuckets; bucket++ {
			cumKey, bitPos := ef.Get2(uint64(bucket))
			if cumKey != cumKeys[bucket] {
				t.Fatalf("bucket %d: cumKey from EF = %d, expected %d", bucket, cumKey, cumKeys[bucket])
			}
			if bitPos != position[bucket] {
				t.Fatalf("bucket %d: position from EF = %d, expected %d", bucket, bitPos, position[bucket])
			}
			cumKey = ef1.Get(uint64(bucket))
			if cumKey != cumKeys[bucket] {
				t.Fatalf("bucket %d: cumKey from EF1 = %d, expected %d", bucket, cumKey, cumKeys[bucket])
			}
			bitPos = ef2.Get(uint64(bucket))
			if bitPos != position[bucket] {
				t.Fatalf("bucket %d: position from EF2 = %d, expected %d", bucket, bitPos, position[bucket])
			}
		}
		for bucket := 0; bucket < numBuckets; bucket++ {
			cumKey, cumKeysNext, bitPos := ef.Get3(uint64(bucket))
			if cumKey != cumKeys[bucket] {
				t.Fatalf("bucket %d: cumKey from EF = %d, expected %d", bucket, cumKey, cumKeys[bucket])
			}
			if bitPos != position[bucket] {
				t.Fatalf("bucket %d: position from EF = %d, expected %d", bucket, bitPos, position[bucket])
			}
			if cumKeysNext != cumKeys[bucket+1] {
				t.Fatalf("bucket %d: cumKeysNext from EF = %d, expected %d", bucket, cumKeysNext, cumKeys[bucket+1])
			}
		}
	*/
}
