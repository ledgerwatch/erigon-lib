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
	"bytes"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

func TestEliasFanoSeek(t *testing.T) {
	count := uint64(1_000_000)
	maxOffset := count * 123
	ef := NewEliasFano(count, maxOffset)
	for offset := uint64(0); offset < count; offset++ {
		ef.AddOffset(offset * 123)
	}
	ef.Build()
	//v := ef.Get(count - 1)
	//fmt.Printf("v: %d\n", v)
	//fmt.Printf("v: %d\n", len(ef.jump))

	v1, ok1 := ef.Search((count - 1) * 16)
	v2, ok2 := ef.Search3((count - 1) * 16)
	require.Equal(t, ok1, ok2)
	require.Equal(t, v1, v2)
	_, _ = v2, ok2
	require.Fail(t, "")
}

func BenchmarkName(b *testing.B) {
	count := uint64(1_000_000)
	maxOffset := count * 123
	ef := NewEliasFano(count, maxOffset)
	for offset := uint64(0); offset < count; offset++ {
		ef.AddOffset(offset * 123)
	}
	ef.Build()
	b.Run("1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ef.Search((count - 1) * 16)
		}
	})
	b.Run("2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ef.Search3((count - 1) * 16)
		}
	})
}

func TestEliasFano(t *testing.T) {
	offsets := []uint64{1, 4, 6, 8, 10, 14, 16, 19, 22, 34, 37, 39, 41, 43, 48, 51, 54, 58, 62}
	count := uint64(len(offsets))
	maxOffset := offsets[0]
	for _, offset := range offsets {
		if offset > maxOffset {
			maxOffset = offset
		}
	}
	ef := NewEliasFano(count, maxOffset)
	for _, offset := range offsets {
		ef.AddOffset(offset)
	}
	ef.Build()
	for i, offset := range offsets {
		offset1 := ef.Get(uint64(i))
		assert.Equal(t, offset, offset1, "offset")
	}
	v, ok := ef.Search(37)
	assert.True(t, ok, "search1")
	assert.Equal(t, uint64(37), v, "search1")
	v, ok = ef.Search(0)
	assert.True(t, ok, "search2")
	assert.Equal(t, uint64(1), v, "search2")
	_, ok = ef.Search(100)
	assert.False(t, ok, "search3")
	v, ok = ef.Search(11)
	assert.True(t, ok, "search4")
	assert.Equal(t, uint64(14), v, "search4")

	buf := bytes.NewBuffer(nil)
	ef.Write(buf)
	assert.Equal(t, ef.AppendBytes(nil), buf.Bytes())

	ef2, _ := ReadEliasFano(buf.Bytes())
	assert.Equal(t, ef.Min(), ef2.Min())
	assert.Equal(t, ef.Max(), ef2.Max())
	assert.Equal(t, ef2.Max(), Max(buf.Bytes()))
	assert.Equal(t, ef2.Min(), Min(buf.Bytes()))
	assert.Equal(t, ef2.Count(), Count(buf.Bytes()))
}

func TestIterator(t *testing.T) {
	offsets := []uint64{1, 4, 6, 8, 10, 14, 16, 19, 22, 34, 37, 39, 41, 43, 48, 51, 54, 58, 62}
	count := uint64(len(offsets))
	maxOffset := offsets[0]
	for _, offset := range offsets {
		if offset > maxOffset {
			maxOffset = offset
		}
	}
	ef := NewEliasFano(count, maxOffset)
	for _, offset := range offsets {
		ef.AddOffset(offset)
	}
	ef.Build()
	efi := ef.Iterator()
	i := 0
	var values []uint64
	for efi.HasNext() {
		v, _ := efi.Next()
		values = append(values, v)
		assert.Equal(t, offsets[i], v, "iter")
		i++
	}
	iter.ExpectEqualU64(t, iter.ReverseArray(values), ef.ReverseIterator())
}

func TestJump(t *testing.T) {
	count := uint64(1_000_000)
	maxOffset := count * 16
	ef := NewEliasFano(count, maxOffset)
	for offset := uint64(0); offset < count; offset++ {
		ef.AddOffset(offset * 16)
	}
	ef.Build()
	v := ef.Get(count - 1)
	fmt.Printf("v: %d\n", v)
	fmt.Printf("v: %d\n", len(ef.jump))

	v, ok := ef.Search((count - 1) * 16)
	pr := message.NewPrinter(language.English)
	pr.Printf("search res: %d, %t\n", v, ok)
}
