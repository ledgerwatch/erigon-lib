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

package patricia

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

type computePrefixTest struct {
	key       []byte
	bitOffset int
	bits      int
	prefix    uint32
}

var computePrefixTests = []computePrefixTest{
	{[]byte{0x0}, 0, 1, 1},
	{[]byte{0x0}, 0, 2, 2},
	{[]byte{0xff}, 0, 2, 0xff000002},
	{[]byte{0xff}, 1, 2, 0xfe000002},
}

func TestComputePrefix(t *testing.T) {
	for i, testSet := range computePrefixTests {
		require := require.New(t)
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			p := computePrefix(testSet.key, testSet.bitOffset, testSet.bits)
			require.Equal(testSet.prefix, p)
		})
	}
}
