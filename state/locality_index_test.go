package state

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocality(t *testing.T) {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	path, db, ii, txs := filledInvIndexOfSize(t, 400, 4)
	mergeInverted(t, db, ii, txs)
	li, _ := NewLocalityIndex(path, path, 4, "inv")
	err := li.BuildMissedIndices(ctx, ii)
	require.NoError(t, err)

	it := ii.MakeContext().iterateKeysLocality(math.MaxUint64)
	for it.HasNext() {
		a, b, c := it.Next()
		fmt.Printf("a: %x, %d, %d\n", a, b, c)
	}
	//fmt.Printf("a: %d\n", li.file.endTxNum)
	_, _ = db, txs
}
