package state

import (
	"sort"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func Test_mergeEliasFano(t *testing.T) {
	t.Skip()

	firstList := []int{1, 298164, 298163, 13, 298160, 298159}
	sort.Ints(firstList)
	uniq := make(map[int]struct{})

	first := eliasfano32.NewEliasFano(uint64(len(firstList)), uint64(firstList[len(firstList)-1]))
	for _, v := range firstList {
		uniq[v] = struct{}{}
		first.AddOffset(uint64(v))
	}
	first.Build()
	firstBytes := first.AppendBytes(nil)

	fit := first.Iterator()
	for fit.HasNext() {
		v := fit.Next()
		require.Contains(t, firstList, int(v))
	}

	secondList := []int{
		1, 644951, 644995, 682653, 13,
		644988, 644987, 644946, 644994,
		644942, 644945, 644941, 644940,
		644939, 644938, 644792, 644787}
	sort.Ints(secondList)
	second := eliasfano32.NewEliasFano(uint64(len(secondList)), uint64(secondList[len(secondList)-1]))

	for _, v := range secondList {
		second.AddOffset(uint64(v))
		uniq[v] = struct{}{}
	}
	second.Build()
	secondBytes := second.AppendBytes(nil)

	sit := second.Iterator()
	for sit.HasNext() {
		v := sit.Next()
		require.Contains(t, secondList, int(v))
	}

	menc, err := mergeEfs(firstBytes, secondBytes, nil)
	require.NoError(t, err)

	merged, _ := eliasfano32.ReadEliasFano(menc)
	require.NoError(t, err)
	require.EqualValues(t, len(uniq), merged.Count())
	mergedLists := append(firstList, secondList...)
	sort.Ints(mergedLists)
	require.EqualValues(t, mergedLists[len(mergedLists)-1], merged.Max())

	mit := merged.Iterator()
	for mit.HasNext() {
		v := mit.Next()
		require.Contains(t, mergedLists, int(v))
	}
}

func Test2(t *testing.T) {
	buf := []byte{}
	_, err := mergeEfs(common.MustDecodeHex("000000000000000000000000053e5c1d1c5c3e010000000000000000000000000200000000000000010000000000000000000000000000000000000000000000"), common.MustDecodeHex("00000000000000180000000005d00cf4bdbaa23486281c139a3c85a09b9376f3c1528fdbbb0b81d727f0bbeb45ca9456234300cbcf1aedb381e53362f170379a1b750e2edfab2b208dd03c03843442f30c10000000000000000000000000000096a1510808ee00107a00000000000000010000000000000000000000000000000000000000000000"), buf)
	require.NoError(t, err)
}
