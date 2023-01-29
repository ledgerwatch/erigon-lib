package state

import (
	"context"
	"os"
	"testing"

	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

func TestGCReadAfterRemoveFile(t *testing.T) {
	require := require.New(t)
	path, db, h, txs := filledHistory(t)
	collateAndMergeHistory(t, db, h, txs)
	ctx := context.Background()

	t.Run("read after: remove when have reader", func(t *testing.T) {
		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		// - create immutable view
		// - del cold file
		// - read from deleted file
		// - close view
		// - open new view
		// - make sure there is no deleted file
		hc := h.MakeContext()
		_ = hc
		lastOnFs, _ := h.files.Max()
		require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
		h.integrateMergedFiles(nil, []*filesItem{lastOnFs}, nil, nil)
		err = h.deleteFiles(nil, []*filesItem{lastOnFs})
		require.NoError(err)
		require.NotNil(lastOnFs.decompressor)

		lastInView, _ := hc.historyFiles.Max()
		require.Equal(lastInView.startTxNum, lastOnFs.startTxNum)
		require.Equal(lastInView.endTxNum, lastOnFs.endTxNum)
		if lastInView.getter.HasNext() {
			k, _ := lastInView.getter.Next(nil)
			require.Equal(8, len(k))
			v, _ := lastInView.getter.Next(nil)
			require.Equal(8, len(v))
		}
		hc.Close()
		require.Nil(lastOnFs.decompressor)

		nonDeletedOnFs, _ := h.files.Max()
		require.False(nonDeletedOnFs.frozen)
		require.NotNil(nonDeletedOnFs.decompressor) // non-deleted files are not closed

		hc = h.MakeContext()
		newLastInView, _ := hc.historyFiles.Max()
		require.False(lastOnFs.frozen)
		require.False(lastInView.startTxNum == newLastInView.startTxNum && lastInView.endTxNum == newLastInView.endTxNum)

		files, err := os.ReadDir(path)
		require.NoError(err)
		h.files = btree.NewG[*filesItem](32, filesItemLess)
		h.scanStateFiles(files, nil)
		newLastOnFs, _ := h.files.Max()
		require.False(lastOnFs.startTxNum == newLastOnFs.startTxNum && lastInView.endTxNum == newLastOnFs.endTxNum)
		hc.Close()
	})

	t.Run("read after: remove when no readers", func(t *testing.T) {
		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		// - del cold file
		// - new reader must not see deleted file
		lastOnFs, _ := h.files.Max()
		require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
		err = h.deleteFiles(nil, []*filesItem{lastOnFs})
		require.NoError(err)

		require.Nil(lastOnFs.decompressor)
	})

}
