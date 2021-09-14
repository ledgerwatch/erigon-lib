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

// Optimal Golomb-Rice parameters for leaves
var bijMemo []uint32 = []uint32{0, 0, 0, 1, 3, 4, 5, 7, 8, 10, 11, 12, 14, 15, 16, 18, 19, 21, 22, 23, 25, 26, 28, 29, 30}

// GolombRice can build up the golomb-rice encoding of the sequeuce of numbers, as well as read the numbers back from it.
type GolombRice struct {
	bitCount int
	data     []uint64
}

// appendUnaryAll adds the unary encoding of specified sequence of numbers to the end of the
// current encoding
func (g *GolombRice) appendUnaryAll(unary []uint32) {
	bitInc := 0
	for _, u := range unary {
		// Each number u uses u+1 bits for its unary representation
		bitInc += int(u) + 1
	}
	targetSize := (g.bitCount + bitInc + 63) / 64
	for len(g.data) < targetSize {
		g.data = append(g.data, 0)
	}

	for _, u := range unary {
		g.bitCount += int(u)
		appendPtr := g.bitCount / 64
		g.data[appendPtr] |= uint64(1) << (g.bitCount & 63)
		g.bitCount++
	}
}

// appendFixed encodes the next value using specified Golomb parameter. Since we are using Golomb-Rice encoding,
// all Golomb parameters are powers of two. Therefore we input log2 of golomb parameter, rather than golomn paramter itself,
// for convinience
func (g *GolombRice) appendFixed(v uint32, log2golomb int) {
	if log2golomb == 0 {
		return
	}
	lowerBits := v & ((uint32(1) << log2golomb) - 1) // Extract the part of the number that will be encoded using truncated binary encoding
	usedBits := g.bitCount & 63                      // How many bits of the last element of b.data is used by previous value
	targetSize := (g.bitCount + log2golomb + 63) / 64
	//fmt.Printf("g.bitCount = %d, log2golomb = %d, targetSize = %d\n", g.bitCount, log2golomb, targetSize)
	for len(g.data) < targetSize {
		g.data = append(g.data, 0)
	}
	appendPtr := g.bitCount / 64 // The index in b.data corresponding to the last element used by previous value, or if previous values fits perfectly, the index of the next free element
	curWord := g.data[appendPtr]
	curWord |= uint64(lowerBits) << usedBits // curWord now contains the new value potentially combined with the part of the previous value
	if usedBits+log2golomb > 64 {
		// New value overflows to the next element
		g.data[appendPtr] = curWord
		appendPtr++
		curWord = uint64(lowerBits) >> (64 - usedBits) // curWord now contains the part of the new value that overflows
	}
	g.data[appendPtr] = curWord
	g.bitCount += log2golomb
}

// bits returns currrent number of bits in the compact encoding of the hash function representation
func (b GolombRice) bits() int {
	return b.bitCount
}
