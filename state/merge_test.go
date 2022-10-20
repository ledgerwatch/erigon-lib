package state

import (
	"fmt"
	"sort"
	"testing"
	"testing/fstest"

	"github.com/google/btree"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func TestHistFindMergeRange(t *testing.T) {
	ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1,
		files: btree.NewG[*filesItem](32, filesItemLess),
	}
	ffs := fstest.MapFS{
		"test.0-1.ef": {},
		"test.1-2.ef": {},
		"test.0-4.ef": {},
		"test.2-3.ef": {},
		"test.3-4.ef": {},
		"test.4-5.ef": {},
		"test.5-6.ef": {},
		"test.6-8.ef": {},
	}
	files, err := ffs.ReadDir(".")
	require.NoError(t, err)
	ii.scanStateFiles(files)

	ok, from, to := ii.findMergeRange(1, 32)
	fmt.Printf("%t, %d, %d\n", ok, from, to)
	ok, from, to = ii.findMergeRange(4, 32)
	fmt.Printf("%t, %d, %d\n", ok, from, to)
	ok, from, to = ii.findMergeRange(60, 32)
	fmt.Printf("%t, %d, %d\n", ok, from, to)

	f, a := ii.staticFilesInRange(from, to)

	fmt.Printf("%d\n", a)
	for _, ff := range f {
		fmt.Printf("%d-%d\n", ff.startTxNum, ff.endTxNum)
	}

}

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
