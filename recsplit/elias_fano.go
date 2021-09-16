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
	"math/bits"
)

const (
	log2q      uint64 = 8
	q          uint64 = 1 << log2q
	qMask      uint64 = q - 1
	superQ     uint64 = 1 << 14
	superQMask uint64 = superQ - 1
	qPerSuperQ uint64 = superQ / q       // 64
	superQSize uint64 = 1 + qPerSuperQ/4 // 1 + 64/4 = 17
)

// DoubleEliasFano can be used to encde a monotone sequence
type DoubleEliasFano struct {
	data                  []uint64
	lowerBits             []uint64
	upperBitsPosition     []uint64
	upperBitsCumKeys      []uint64
	jump                  []uint64
	lowerBitsMaskCumKeys  uint64
	lowerBitsMaskPosition uint64
	numBuckets            uint64
	uCumKeys              uint64
	uPosition             uint64
	lPosition             uint64
	lCumKeys              uint64
	cumKeysMinDelta       int64
	minDiff               int64
	bitsPerKeyFixedPoint  uint64
}

// Build construct double Elias Fano index for two given sequences
func (ef *DoubleEliasFano) Build(cumKeys []uint64, position []uint64) {
	if len(cumKeys) != len(position) {
		panic("len(cumKeys) != len(position)")
	}
	ef.numBuckets = uint64(len(cumKeys) - 1)
	ef.bitsPerKeyFixedPoint = uint64(float64(uint64(1)<<20) * (float64(position[ef.numBuckets]) / (float64(cumKeys[ef.numBuckets]))))
	ef.minDiff = math.MaxInt64 / 2
	ef.cumKeysMinDelta = math.MaxInt64 / 2
	var prevBucketBits int64 = 0
	for i := uint64(1); i <= ef.numBuckets; i++ {
		var nkeysDelta int64 = int64(cumKeys[i]) - int64(cumKeys[i-1])
		if nkeysDelta < ef.cumKeysMinDelta {
			ef.cumKeysMinDelta = nkeysDelta
		}
		var bucketBits int64 = int64(position[i]) - int64((ef.bitsPerKeyFixedPoint*cumKeys[i])>>20)
		if bucketBits-prevBucketBits < ef.minDiff {
			ef.minDiff = bucketBits - prevBucketBits
		}
		prevBucketBits = bucketBits
	}
	ef.uPosition = uint64(int64(position[ef.numBuckets]) - int64((ef.bitsPerKeyFixedPoint*cumKeys[ef.numBuckets])>>20) - int64(ef.numBuckets)*ef.minDiff + 1)
	if ef.uPosition/(ef.numBuckets+1) == 0 {
		ef.lPosition = 0
	} else {
		ef.lPosition = 63 ^ uint64(bits.LeadingZeros64(ef.uPosition/(ef.numBuckets+1)))
	}
	ef.uCumKeys = cumKeys[ef.numBuckets] - ef.numBuckets*uint64(ef.cumKeysMinDelta) + 1
	if ef.uCumKeys/(ef.numBuckets+1) == 0 {
		ef.lCumKeys = 0
	} else {
		ef.lCumKeys = 63 ^ uint64(bits.LeadingZeros64(ef.uCumKeys/(ef.numBuckets+1)))
	}
	if ef.lCumKeys*2+ef.lPosition > 56 {
		panic(fmt.Sprintf("ef.lCumKeys (%d) * 2 + ef.lPosition (%d) > 56", ef.lCumKeys, ef.lPosition))
	}
	ef.lowerBitsMaskCumKeys = (uint64(1) << ef.lCumKeys) - 1
	ef.lowerBitsMaskPosition = (uint64(1) << ef.lPosition) - 1
	wordsLowerBits := int(((ef.numBuckets+1)*(ef.lCumKeys+ef.lPosition)+63)/64 + 1)
	wordsCumKeys := int((ef.numBuckets + 1 + (ef.uCumKeys >> ef.lCumKeys) + 63) / 64)
	wordsPosition := int((ef.numBuckets + 1 + (ef.uPosition >> ef.lPosition) + 63) / 64)
	ef.upperBitsPosition = make([]uint64, wordsPosition)
	jumpWords := ef.jumpSizeWords()
	totalWords := wordsLowerBits + wordsCumKeys + wordsPosition + jumpWords
	ef.data = make([]uint64, totalWords)
	ef.lowerBits = ef.data[:wordsLowerBits]
	ef.upperBitsCumKeys = ef.data[wordsLowerBits : wordsLowerBits+wordsCumKeys]
	ef.upperBitsPosition = ef.data[wordsLowerBits+wordsCumKeys : wordsLowerBits+wordsCumKeys+wordsPosition]
	ef.jump = ef.data[wordsLowerBits+wordsCumKeys+wordsPosition:]

	for i, cumDelta, bitDelta := uint64(0), uint64(0), uint64(0); i <= ef.numBuckets; i, cumDelta, bitDelta = i+1, cumDelta+uint64(ef.cumKeysMinDelta), bitDelta+uint64(ef.minDiff) {
		if ef.lCumKeys != 0 {
			set_bits(ef.lowerBits, i*(ef.lCumKeys+ef.lPosition), int(ef.lCumKeys), (cumKeys[i]-cumDelta)&ef.lowerBitsMaskCumKeys)
		}
		set(ef.upperBitsCumKeys, ((cumKeys[i]-cumDelta)>>ef.lCumKeys)+i)

		pval := int64(position[i]) - int64((ef.bitsPerKeyFixedPoint*cumKeys[i])>>20)
		if ef.lPosition != 0 {
			set_bits(ef.lowerBits, i*(ef.lCumKeys+ef.lPosition)+ef.lCumKeys, int(ef.lPosition), uint64((pval-int64(bitDelta)))&ef.lowerBitsMaskPosition)
		}
		set(ef.upperBitsPosition, ((uint64(pval)-bitDelta)>>ef.lPosition)+i)
	}
	// i iterates over the 64-bit words in the wordCumKeys vector
	// c iterates over bits in the wordCumKeys
	// lastSuperQ is the largest multiple of 2^14 (4096) which is no larger than c
	// c/superQ is the index of the current 4096 block of bits
	// superQSize is how many words is required to encode one block of 4096 bits. It is 17 words which is 1088 bits
	for i, c, lastSuperQ := uint64(0), uint64(0), uint64(0); i < uint64(wordsCumKeys); i++ {
		for b := uint64(0); b < 64; b++ {
			if ef.upperBitsCumKeys[i]&(uint64(1)<<b) != 0 {
				if (c & superQMask) == 0 {
					// When c is multiple of 2^14 (4096)
					lastSuperQ = i*64 + b
					ef.jump[(c/superQ)*(superQSize*2)] = lastSuperQ
				}
				if (c & qMask) == 0 {
					// When c is multiple of 2^8 (256)
					var offset = i*64 + b - lastSuperQ // offset can be either 0, 256, 512, 768, ..., up to 4096-256
					// offset needs to be encoded as 16-bit integer, therefore the following check
					if offset >= (1 << 16) {
						panic("")
					}
					// c % superQ is the bit index inside the group of 4096 bits
					idx16 := 4*((c/superQ)*(superQSize*2)+2) + 2*((c%superQ)/q)
					idx64 := idx16 >> 2
					shift := 16 * (idx16 % 4)
					mask := uint64(0xffff) << shift
					ef.jump[idx64] = (ef.jump[idx64] &^ mask) | (offset << shift)
				}
				c++
			}
		}
	}

	for i, c, lastSuperQ := uint64(0), uint64(0), uint64(0); i < uint64(wordsPosition); i++ {
		for b := uint64(0); b < 64; b++ {
			if ef.upperBitsPosition[i]&(uint64(1)<<b) != 0 {
				if (c & superQMask) == 0 {
					lastSuperQ = i*64 + b
					ef.jump[(c/superQ)*(superQSize*2)+1] = lastSuperQ
				}
				if (c & qMask) == 0 {
					var offset = i*64 + b - lastSuperQ
					if offset >= (1 << 16) {
						panic("")
					}
					idx16 := 4*((c/superQ)*(superQSize*2)+2) + 2*((c%superQ)/q) + 1
					idx64 := idx16 >> 2
					shift := 16 * (idx16 % 4)
					mask := uint64(0xffff) << shift
					ef.jump[idx64] = (ef.jump[idx64] &^ mask) | (offset << shift)
				}
				c++
			}
		}
	}
}

// set_bits assumes that bits are set in monotonic order, so that
// we can skip the masking for the second word
func set_bits(bits []uint64, start uint64, width int, value uint64) {
	mask := (uint64(1)<<width - 1) << start
	bits[start>>6] = (bits[start>>6] &^ mask) | (value << start)
	if int(start)+width > 64 {
		// changes two 64-bit words
		bits[start>>6+1] = value >> (64 - start)
	}
}

func set(bits []uint64, pos uint64) {
	bits[pos>>6] |= uint64(1) << (pos & 63)
}

func (ef DoubleEliasFano) jumpSizeWords() int {
	size := (ef.numBuckets / superQ) * superQSize * 2 // Whole blocks
	if ef.numBuckets%superQ != 0 {
		size += (1 + ((ef.numBuckets%superQ+q-1)/q+3)/4) * 2 // Partial block
	}
	return int(size)
}

// Data returns binary representation of double Ellias-Fano index that has been built
func (ef DoubleEliasFano) Data() []uint64 {
	return ef.data
}

func (ef DoubleEliasFano) Get2(i uint64) (cumKeys uint64, position uint64) {
	posLower := i * (ef.lCumKeys + ef.lPosition)
	var lower uint64
	idx8 := posLower / 8
	idx64 := idx8 / 8
	shift := 8 * (idx8 % 8)
	lower = ef.lowerBits[idx64] << shift
	if shift > 0 {
		lower |= ef.lowerBits[idx64+1] >> (64 - shift)
	}
	lower >>= posLower % 8

	jumpSuperQ := (i / superQ) * superQSize * 2
	jumpInsideSuperQ := (i % superQ) / q
	idx16 := 4*(jumpSuperQ+2) + 2*jumpInsideSuperQ
	idx64 = idx16 / 4
	shift = 16 * (idx16 % 4)
	mask := uint64(0xffff) << shift
	jumpCumKeys := ef.jump[jumpSuperQ] + (ef.jump[idx64]&mask)>>shift
	idx16++
	idx64 = idx16 / 4
	shift = 16 * (idx16 % 4)
	mask = uint64(0xffff) << shift
	jumpPosition := ef.jump[jumpSuperQ+1] + (ef.jump[idx64]&mask)>>shift

	currWordCumKeys := jumpCumKeys / 64
	currWordPosition := jumpPosition / 64
	windowCumKeys := ef.upperBitsCumKeys[currWordCumKeys] & uint64(0xffffffffffffffff) << jumpCumKeys % 64
	windowPosition := ef.upperBitsPosition[currWordPosition] & uint64(0xffffffffffffffff) << jumpPosition % 64
	deltaCumKeys := int(i & qMask)
	deltaPosition := int(i & qMask)

	for bitCount := bits.OnesCount64(windowCumKeys); bitCount <= deltaCumKeys; bitCount = bits.OnesCount64(windowCumKeys) {
		currWordCumKeys++
		windowCumKeys = ef.upperBitsCumKeys[currWordCumKeys]
		deltaCumKeys -= bitCount
	}
	for bitCount := bits.OnesCount64(windowPosition); bitCount <= deltaPosition; bitCount = bits.OnesCount64(windowPosition) {
		currWordPosition++
		windowPosition = ef.upperBitsPosition[currWordPosition]
		deltaPosition -= bitCount
	}

	selectCumKeys := select64(windowCumKeys, deltaCumKeys)
	cumDelta := i * uint64(ef.cumKeysMinDelta)
	cumKeys = ((currWordCumKeys*64+uint64(selectCumKeys)-i)<<ef.lCumKeys | (lower & ef.lowerBitsMaskCumKeys)) + cumDelta

	lower >>= ef.lCumKeys
	bitDelta := i * uint64(ef.minDiff)
	position = ((currWordPosition*64+uint64(select64(windowPosition, deltaPosition))-i)<<ef.lPosition | (lower & ef.lowerBitsMaskPosition)) + bitDelta +
		(ef.bitsPerKeyFixedPoint * cumKeys >> 20)
	return
}

func (ef DoubleEliasFano) Get3(i uint64) (cumKeys uint64, cumKeysNext uint64, position uint64) {
	posLower := i * (ef.lCumKeys + ef.lPosition)
	var lower uint64
	idx8 := posLower / 8
	idx64 := idx8 / 8
	shift := 8 * (idx8 % 8)
	lower = ef.lowerBits[idx64] << shift
	if shift > 0 {
		lower |= ef.lowerBits[idx64+1] >> (64 - shift)
	}
	lower >>= posLower % 8

	jumpSuperQ := (i / superQ) * superQSize * 2
	jumpInsideSuperQ := (i % superQ) / q
	idx16 := 4*(jumpSuperQ+2) + 2*jumpInsideSuperQ
	idx64 = idx16 / 4
	shift = 16 * (idx16 % 4)
	mask := uint64(0xffff) << shift
	jumpCumKeys := ef.jump[jumpSuperQ] + (ef.jump[idx64]&mask)>>shift
	idx16++
	idx64 = idx16 / 4
	shift = 16 * (idx16 % 4)
	mask = uint64(0xffff) << shift
	jumpPosition := ef.jump[jumpSuperQ+1] + (ef.jump[idx64]&mask)>>shift

	currWordCumKeys := jumpCumKeys / 64
	currWordPosition := jumpPosition / 64
	windowCumKeys := ef.upperBitsCumKeys[currWordCumKeys] & uint64(0xffffffffffffffff) << jumpCumKeys % 64
	windowPosition := ef.upperBitsPosition[currWordPosition] & uint64(0xffffffffffffffff) << jumpPosition % 64
	deltaCumKeys := int(i & qMask)
	deltaPosition := int(i & qMask)

	for bitCount := bits.OnesCount64(windowCumKeys); bitCount <= deltaCumKeys; bitCount = bits.OnesCount64(windowCumKeys) {
		currWordCumKeys++
		windowCumKeys = ef.upperBitsCumKeys[currWordCumKeys]
		deltaCumKeys -= bitCount
	}
	for bitCount := bits.OnesCount64(windowPosition); bitCount <= deltaPosition; bitCount = bits.OnesCount64(windowPosition) {
		currWordPosition++
		windowPosition = ef.upperBitsPosition[currWordPosition]
		deltaPosition -= bitCount
	}

	selectCumKeys := select64(windowCumKeys, deltaCumKeys)
	cumDelta := i * uint64(ef.cumKeysMinDelta)
	cumKeys = ((currWordCumKeys*64+uint64(selectCumKeys)-i)<<ef.lCumKeys | (lower & ef.lowerBitsMaskCumKeys)) + cumDelta

	lower >>= ef.lCumKeys
	bitDelta := i * uint64(ef.minDiff)
	position = ((currWordPosition*64+uint64(select64(windowPosition, deltaPosition))-i)<<ef.lPosition | (lower & ef.lowerBitsMaskPosition)) + bitDelta +
		(ef.bitsPerKeyFixedPoint * cumKeys >> 20)

	windowCumKeys &= (uint64(0xffffffffffffffff) << selectCumKeys) << 1
	for windowCumKeys == 0 {
		currWordCumKeys++
		windowCumKeys = ef.upperBitsCumKeys[currWordCumKeys]
	}

	lower >>= ef.lPosition
	cumKeysNext = ((currWordCumKeys*64+uint64(bits.LeadingZeros64(windowCumKeys))-i-1)<<ef.lCumKeys | (lower & ef.lowerBitsMaskCumKeys)) + cumDelta + uint64(ef.cumKeysMinDelta)
	return
}
