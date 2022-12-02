package state

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocality(t *testing.T) {
	ctx := context.Background()
	const Module uint64 = 31
	path, db, ii, txs := filledInvIndexOfSize(t, 300, 4, Module)
	mergeInverted(t, db, ii, txs)
	li, _ := NewLocalityIndex(path, path, 4, "inv")
	defer li.Close()
	err := li.BuildMissedIndices(ctx, ii)
	require.NoError(t, err)

	it := ii.MakeContext().iterateKeysLocality(math.MaxUint64)
	require.True(t, it.HasNext())
	key, bitmap, _ := it.Next()
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(key))
	require.Equal(t, uint64(0b11), bitmap)
	require.True(t, it.HasNext())
	key, bitmap, _ = it.Next()
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(key))
	require.Equal(t, uint64(0b11), bitmap)

	var last []byte
	for it.HasNext() {
		key, _, _ = it.Next()
		last = key
	}
	require.Equal(t, Module, binary.BigEndian.Uint64(last))
}
