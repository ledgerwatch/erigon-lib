package state

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocality(t *testing.T) {
	ctx, require := context.Background(), require.New(t)
	const Module uint64 = 31
	path, db, ii, txs := filledInvIndexOfSize(t, 300, 4, Module)
	mergeInverted(t, db, ii, txs)
	li, _ := NewLocalityIndex(path, path, 4, "inv")
	defer li.Close()
	err := li.BuildMissedIndices(ctx, ii)
	require.NoError(err)

	it := ii.MakeContext().iterateKeysLocality(math.MaxUint64)
	require.True(it.HasNext())
	key, bitmap, _ := it.Next()
	require.Equal(uint64(2), binary.BigEndian.Uint64(key))
	require.Equal([]uint64{0, 1}, bitmap)
	require.True(it.HasNext())
	key, bitmap, _ = it.Next()
	require.Equal(uint64(3), binary.BigEndian.Uint64(key))
	require.Equal([]uint64{0, 1}, bitmap)

	var last []byte
	for it.HasNext() {
		key, _, _ = it.Next()
		last = key
	}
	require.Equal(Module, binary.BigEndian.Uint64(last))

	files, err := li.buildFiles(ctx, ii, ii.endTxNumMinimax()/ii.aggregationStep)
	require.NoError(err)
	res, err := files.bm.At(0)
	require.Equal([]uint64{0, 1}, res)
	res, err = files.bm.At(1)
	require.Equal([]uint64{0, 1}, res)
	res, err = files.bm.At(32)
	require.Empty(res)

	fst, snd, ok1, ok2, err := files.bm.First2At(0, 1)
	require.True(ok1)
	require.False(ok2)
	require.Equal(uint64(1), fst)
	require.Equal(0, snd)
	_ = files.bm
}
