package bitmapdb

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/blevesearch/mmap-go"
	"github.com/stretchr/testify/require"
)

func TestFixedSizeBitmaps2(t *testing.T) {
	tmpDir, require := t.TempDir(), require.New(t)
	fPath := tmpDir + "/a.tmp"
	f, err := os.Create(fPath)
	require.NoError(err)
	m, err := mmap.MapRegion(f, 1*os.Getpagesize(), mmap.RDWR, 0, 0)
	require.NoError(err)
	wr := bufio.NewWriter(f)
	_, err = f.Write(make([]byte, os.Getpagesize()))
	wr.Flush()

	require.NoError(err)
	m[1] = 1
	m.Flush()
	//f.Sync()
	m.Unmap()
	//f.Close()

	m2, err := mmap.Map(f, mmap.RDONLY, 0)
	require.NoError(err)
	fmt.Printf("%x\n", m2[1])
}

func TestFixedSizeBitmaps(t *testing.T) {

	tmpDir, require := t.TempDir(), require.New(t)
	must := require.NoError
	idxPath := filepath.Join(tmpDir, "idx.tmp")
	wr, err := NewFixedSizeBitmapsWriter(idxPath, 14, 7)
	require.NoError(err)
	must(wr.AddArray(0, []uint64{3, 9, 11}))
	must(wr.AddArray(1, []uint64{1, 2, 3}))
	must(wr.AddArray(2, []uint64{4, 8, 13}))
	must(wr.AddArray(3, []uint64{1, 13}))
	must(wr.AddArray(4, []uint64{1, 13}))
	must(wr.AddArray(5, []uint64{1, 13}))
	must(wr.AddArray(6, []uint64{0, 9, 13}))
	must(wr.AddArray(7, []uint64{7}))
	require.Error(wr.AddArray(8, []uint64{8}))
	err = wr.Build()
	require.NoError(err)

	bm, err := OpenFixedSizeBitmaps(idxPath, 14)
	require.NoError(err)

	at := func(item uint64) []uint64 {
		n, err := bm.At(item)
		require.NoError(err)
		return n
	}

	require.Equal([]uint64{3, 9, 11}, at(0))
	require.Equal([]uint64{1, 2, 3}, at(1))
	require.Equal([]uint64{4, 8, 13}, at(2))
	require.Equal([]uint64{1, 13}, at(3))
	require.Equal([]uint64{1, 13}, at(4))
	require.Equal([]uint64{1, 13}, at(5))
	require.Equal([]uint64{0, 9, 13}, at(6))
	require.Equal([]uint64{7}, at(7))

	fst, snd, ok, ok2, err := bm.First2At(7)
	require.NoError(err)
	require.Equal(uint64(7), fst)
	require.Equal(uint64(0), snd)
	require.Equal(true, ok)
	require.Equal(false, ok2)

	_, err = bm.At(8)
	require.Error(err)
}
