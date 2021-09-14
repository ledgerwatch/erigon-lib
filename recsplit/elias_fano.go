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
			set_bits(ef.lowerBits, i*(l_cum_keys+l_position), l_cum_keys, (cum_keys[i]-cum_delta)&lower_bits_mask_cum_keys)
		}
		set(upper_bits_cum_keys, ((cum_keys[i]-cum_delta)>>l_cum_keys)+i)

		pval := int64(position[i]) - int64(ef.bitsPerKeyFixedPoint*cumKeys[i]>>20)
		if ef.lPosition != 0 {
			set_bits(lower_bits, i*(l_cum_keys+l_position)+l_cum_keys, l_position, (pval-bit_delta)&lower_bits_mask_position)
		}
		set(upper_bits_position, ((uint64(pval)-bitDelta)>>ef.lPosition)+i)
	}
	return &ef
}
