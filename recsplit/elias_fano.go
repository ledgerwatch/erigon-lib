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
	"math"
	"math/bits"
)

const (
	log2q      uint64 = 8
	q          uint64 = 1 << log2q
	qMask      uint64 = q - 1
	superQ     uint64 = 1 << 14
	superQMast uint64 = superQ - 1
	qPerSuperQ uint64 = superQ / q
	superQSize uint64 = 1 + qPerSuperQ/4
)

// DoubleEliasFano can be used to encde a monotone sequence
type DoubleEliasFano struct {
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

func NewDoubleEliasFano(cumKeys []uint64, position []uint64) *DoubleEliasFano {
	var ef DoubleEliasFano
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
		var bucketBits int64 = int64(position[i]) - int64(ef.bitsPerKeyFixedPoint*(cumKeys[i]>>20))
		if bucketBits-prevBucketBits < ef.minDiff {
			ef.minDiff = bucketBits - prevBucketBits
		}
		prevBucketBits = bucketBits
	}
	ef.uPosition = uint64(int64(position[ef.numBuckets]) - int64(ef.bitsPerKeyFixedPoint*cumKeys[ef.numBuckets]>>20) - int64(ef.numBuckets)*ef.minDiff + 1)
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
		panic("ef.lCumKeys * 2 + ef.lPosition > 56")
	}
	ef.lowerBitsMaskCumKeys = (uint64(1) << ef.lCumKeys) - 1
	ef.lowerBitsMaskPosition = (uint64(1) << ef.lPosition) - 1
	wordsLowerBits := ((ef.numBuckets+1)*(ef.lCumKeys+ef.lPosition)+63)/64 + 1
	ef.lowerBits = make([]uint64, wordsLowerBits)
	wordsCumKeys := (ef.numBuckets + 1 + (ef.uCumKeys >> ef.lCumKeys) + 63) / 64
	ef.upperBitsCumKeys = make([]uint64, wordsCumKeys)
	wordsPosition := (ef.numBuckets + 1 + (ef.uPosition >> ef.lPosition) + 63) / 64
	ef.upperBitsPosition = make([]uint64, wordsPosition)
	for i, cumDelta, bitDelta := uint64(0), uint64(0), uint64(0); i <= ef.numBuckets; i, cumDelta, bitDelta = i+1, cumDelta+uint64(ef.cumKeysMinDelta), bitDelta+uint64(ef.minDiff) {
		if ef.lCumKeys != 0 {
			set_bits(ef.lowerBits, i*(ef.lCumKeys+ef.lPosition), int(ef.lCumKeys), (cumKeys[i]-cumDelta)&ef.lowerBitsMaskCumKeys)
		}
		set(ef.upperBitsCumKeys, ((cumKeys[i]-cumDelta)>>ef.lCumKeys)+i)

		pval := int64(position[i]) - int64(ef.bitsPerKeyFixedPoint*cumKeys[i]>>20)
		if ef.lPosition != 0 {
			set_bits(ef.lowerBits, i*(ef.lCumKeys+ef.lPosition)+ef.lCumKeys, int(ef.lPosition), uint64((pval-int64(bitDelta)))&ef.lowerBitsMaskPosition)
		}
		set(ef.upperBitsPosition, ((uint64(pval)-bitDelta)>>ef.lPosition)+i)
	}
	jumpWords := ef.jumpSizeWords()
	ef.jump = make([]uint64, jumpWords)
	for i, c, lastSuperQ := uint64(0), uint64(0), uint64(0); i < wordsCumKeys; i++ {
		for b := 0; b < 64; b++ {
			if ef.upperBitsCumKeys[i] & (uint64(1) << b) != 0 {
				if (c & superQMask) == 0 {
					lastSuperQ = i * 64 + b
					jump[(c / superQ) * (superQSize * 2)] = lastSuperQ
				}
				if (c & qMask) == 0 {
					var offset = i * 64 + b - lastSuperQ;
					if offset >= (1 << 16) {
						panic("")
					}
					((uint16_t *)(&jump + (c / superQ) * (superQSize * 2) + 2))[2 * ((c % superQ) / q)] = offset;
				}
				c++;
			}
		}
	}

	for (uint64_t i = 0, c = 0, last_super_q = 0; i < words_position; i++) {
		for (int b = 0; b < 64; b++) {
			if (upper_bits_position[i] & UINT64_C(1) << b) {
				if ((c & super_q_mask) == 0) jump[(c / super_q) * (super_q_size * 2) + 1] = last_super_q = i * 64 + b;
				if ((c & q_mask) == 0) {
					const uint64_t offset = i * 64 + b - last_super_q;
					if (offset >= (1 << 16)) abort();
					((uint16_t *)(&jump + (c / super_q) * (super_q_size * 2) + 2))[2 * ((c % super_q) / q) + 1] = offset;
				}
				c++;
			}
		}
	}
	return &ef
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
