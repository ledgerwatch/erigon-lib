/*
   Copyright 2022 Erigon contributors

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

package eliasfano32

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEliasFano(t *testing.T) {
	offsets := []uint64{1, 4, 6, 8, 10, 14, 16, 19, 22, 34, 37, 39, 41, 43, 48, 51, 54, 58, 62}
	count := uint64(len(offsets))
	maxOffset := offsets[0]
	minDelta := offsets[1] - offsets[0]
	for i, offset := range offsets {
		if offset > maxOffset {
			maxOffset = offset
		}
		if i > 0 {
			if offset-offsets[i-1] < minDelta {
				minDelta = offset - offsets[i-1]
			}
		}
	}
	ef := NewEliasFano(count, 100, 10)
	for _, offset := range offsets {
		ef.AddOffset(offset)
	}
	ef.Build()
	for i, offset := range offsets {
		offset1 := ef.Get(uint64(i))
		assert.Equal(t, offset, offset1, "offset")
	}
}
