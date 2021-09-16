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
	"testing"
)

// gotip test -trimpath -v -fuzz=Fuzz -fuzztime=10s ./recsplit

func FuzzEliasFano(f *testing.F) {
	f.Fuzz(func(t *testing.T, in []byte) {
		if len(in)%2 == 1 {
			t.Skip()
		}
		if len(in) == 0 {
			t.Skip()
		}
		var ef DoubleEliasFano
		// Treat each byte of the sequence as difference between previous value and the next
		cumKeys := make([]uint64, len(in)/2+1)
		position := make([]uint64, len(in)/2+1)
		for i, b := range in[:len(in)/2] {
			cumKeys[i+1] = cumKeys[i] + uint64(b)
		}
		for i, b := range in[len(in)/2:] {
			position[i+1] = position[i] + uint64(b)
		}
		ef.Build(cumKeys, position)
	})
}
