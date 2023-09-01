package rlp_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/stretchr/testify/require"
)

func TestDecoder(t *testing.T) {

	type simple struct {
		Key   string
		Value string
	}

	t.Run("ShortString", func(t *testing.T) {
		t.Run("ToString", func(t *testing.T) {
			bts := []byte{0x83, 'd', 'o', 'g'}
			var s string
			err := rlp.Unmarshal(bts, &s)
			require.NoError(t, err)
			require.EqualValues(t, "dog", s)
		})
		t.Run("ToBytes", func(t *testing.T) {
			bts := []byte{0x83, 'd', 'o', 'g'}
			var s []byte
			err := rlp.Unmarshal(bts, &s)
			require.NoError(t, err)
			require.EqualValues(t, []byte("dog"), s)
		})
		t.Run("ToInt", func(t *testing.T) {
			bts := []byte{0x82, 0x04, 0x00}
			var s int
			err := rlp.Unmarshal(bts, &s)
			require.NoError(t, err)
			require.EqualValues(t, 1024, s)
		})
		t.Run("ToSimpleStruct", func(t *testing.T) {
			bts := []byte{0xc8, 0x83, 'c', 'a', 't', 0x83, 'd', 'o', 'g'}
			var s simple
			err := rlp.Unmarshal(bts, &s)
			require.NoError(t, err)
			require.EqualValues(t, simple{Key: "cat", Value: "dog"}, s)
		})
	})
}
