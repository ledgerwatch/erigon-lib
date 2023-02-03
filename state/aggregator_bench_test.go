package state

import (
	"context"
	"math/rand"
	"os"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

func testDbAndAggregatorBench(b *testing.B, prefixLen int, aggStep uint64) (string, kv.RwDB, *Aggregator) {
	b.Helper()
	path := b.TempDir()
	b.Cleanup(func() { os.RemoveAll(path) })
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	b.Cleanup(db.Close)
	agg, err := NewAggregator(path, path, aggStep)
	require.NoError(b, err)
	b.Cleanup(agg.Close)
	return path, db, agg
}

func BenchmarkAggregator_Processing(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	longKeys := queueKeys(ctx, 64, length.Addr+length.Hash)
	vals := queueKeys(ctx, 53, length.Hash)

	aggStep := uint64(100_00)
	_, db, agg := testDbAndAggregatorBench(b, length.Addr, aggStep)

	tx, err := db.BeginRw(ctx)
	require.NoError(b, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	agg.SetTx(tx)
	defer agg.StartWrites().FinishWrites()
	require.NoError(b, err)
	agg.StartWrites()
	defer agg.FinishWrites()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := <-longKeys
		val := <-vals
		txNum := uint64(i)
		agg.SetTxNum(txNum)
		err := agg.WriteAccountStorage(key[:length.Addr], key[length.Addr:], val)
		require.NoError(b, err)
		err = agg.FinishTx()
		require.NoError(b, err)
	}
}

func queueKeys(ctx context.Context, seed, ofSize uint64) <-chan []byte {
	rnd := rand.New(rand.NewSource(int64(seed)))
	keys := make(chan []byte, 1)
	go func() {
		for {
			if ctx.Err() != nil {
				break
			}
			bb := make([]byte, ofSize)
			rnd.Read(bb)

			keys <- bb
		}
		close(keys)
	}()
	return keys
}
