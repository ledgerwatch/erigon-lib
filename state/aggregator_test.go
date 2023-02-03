package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

func testDbAndAggregator(t *testing.T, prefixLen int, aggStep uint64) (string, kv.RwDB, *Aggregator) {
	t.Helper()
	path := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(path) })
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(filepath.Join(path, "db4")).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(db.Close)
	agg, err := NewAggregator(path, path, aggStep)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	return path, db, agg
}

func TestAggregator_Merge(t *testing.T) {
	_, db, agg := testDbAndAggregator(t, 0, 100)

	tx, err := db.BeginRwAsync(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)

	defer agg.StartWrites().FinishWrites()
	txs := uint64(10000)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite, otherMaxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)

		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)
		//keys[txNum-1] = append(addr, loc...)

		buf := EncodeAccountBytes(1, uint256.NewInt(0), nil, 0)
		err = agg.UpdateAccountData(addr, buf)
		require.NoError(t, err)

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		if txNum%135 == 0 {
			err = agg.UpdateCommitmentData([]byte("otherroothash"), v[:])
			otherMaxWrite = txNum
		} else {
			err = agg.UpdateCommitmentData([]byte("roothash"), v[:])
			maxWrite = txNum
		}
		require.NoError(t, err)
		require.NoError(t, agg.FinishTx())
	}
	err = agg.Flush(context.Background())
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	// Check the history
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	dc := agg.MakeContext()
	defer dc.Close()
	v, err := dc.ReadCommitment([]byte("roothash"), roTx)
	require.NoError(t, err)

	require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))

	v, err = dc.ReadCommitment([]byte("otherroothash"), roTx)
	require.NoError(t, err)

	require.EqualValues(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
}

// here we create a bunch of updates for further aggregation.
// FinishTx should merge underlying files several times
// Expected that:
// - we could close first aggregator and open another with previous data still available
// - new aggregator SeekCommitment must return txNum equal to amount of total txns
func TestAggregator_RestartOnDatadir(t *testing.T) {
	aggStep := uint64(50)
	path, db, agg := testDbAndAggregator(t, 0, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)
	defer agg.StartWrites().FinishWrites()

	var latestCommitTxNum uint64

	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	txs := (aggStep / 2) * 19
	t.Logf("step=%d tx_count=%d", aggStep, txs)
	var aux [8]byte
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)
		binary.BigEndian.PutUint64(aux[:], txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)
		//keys[txNum-1] = append(addr, loc...)

		buf := EncodeAccountBytes(1, uint256.NewInt(0), nil, 0)
		err = agg.UpdateAccountData(addr, buf)
		require.NoError(t, err)

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		err = agg.UpdateCommitmentData([]byte("key"), aux[:])
		require.NoError(t, err)
		maxWrite = txNum

		require.NoError(t, agg.FinishTx())
	}
	err = agg.Flush(context.Background())
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	// Start another aggregator on same datadir
	anotherAgg, err := NewAggregator(path, path, aggStep)
	require.NoError(t, err)
	defer anotherAgg.Close()

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()

	anotherAgg.SetTx(rwTx)
	startTx := anotherAgg.EndTxNumMinimax()
	sstartTx, err := anotherAgg.SeekCommitment()
	require.NoError(t, err)
	require.GreaterOrEqual(t, sstartTx, startTx)
	require.GreaterOrEqual(t, sstartTx, latestCommitTxNum)
	_ = sstartTx
	rwTx.Rollback()
	rwTx = nil

	// Check the history
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	dc := anotherAgg.MakeContext()
	defer dc.Close()
	v, err := dc.ReadCommitment([]byte("key"), roTx)
	require.NoError(t, err)

	require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregator_RestartOnFiles(t *testing.T) {
	aggStep := uint64(1000)

	path, db, agg := testDbAndAggregator(t, 0, aggStep)
	defer db.Close()
	_ = path

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)
	defer agg.StartWrites().FinishWrites()

	txs := aggStep * 5
	t.Logf("step=%d tx_count=%d", aggStep, txs)

	rnd := rand.New(rand.NewSource(0))
	keys := make([][]byte, txs)

	for txNum := uint64(1); txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)

		buf := EncodeAccountBytes(txNum, uint256.NewInt(1000000000000), nil, 0)
		err = agg.UpdateAccountData(addr, buf[:])
		require.NoError(t, err)

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		keys[txNum-1] = append(addr, loc...)

		err = agg.FinishTx()
		require.NoError(t, err)
	}

	err = tx.Commit()
	require.NoError(t, err)
	tx = nil
	db.Close()
	db = nil
	agg = nil

	require.NoError(t, os.RemoveAll(filepath.Join(path, "db4")))

	newDb, err := mdbx.NewMDBX(log.New()).InMem(filepath.Join(path, "db4")).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).Open()
	require.NoError(t, err)
	t.Cleanup(newDb.Close)

	newTx, err := newDb.BeginRw(context.Background())
	require.NoError(t, err)
	defer newTx.Rollback()

	newAgg, err := NewAggregator(path, path, aggStep)
	require.NoError(t, err)
	defer newAgg.Close()

	newAgg.SetTx(newTx)

	latestTx, err := newAgg.SeekCommitment()
	require.NoError(t, err)
	t.Logf("seek to latest_tx=%d", latestTx)

	ctx := newAgg.MakeContext()
	defer ctx.Close()
	miss := uint64(0)
	for i, key := range keys {
		stored, err := ctx.ReadAccountData(key[:length.Addr], newTx)
		require.NoError(t, err)
		if len(stored) == 0 {
			if uint64(i+1) >= txs-aggStep {
				continue // finishtx always stores last agg step in db which we deleteelete, so miss is expected
			}
			miss++
			fmt.Printf("%x [%d/%d]", key, miss, i+1) // txnum starts from 1
			continue
		}

		nonce, _, _ := DecodeAccountBytes(stored)
		require.EqualValues(t, i+1, nonce)

		storedV, err := ctx.ReadAccountStorage(key[:length.Addr], key[length.Addr:], newTx)
		require.NoError(t, err)
		require.EqualValues(t, key[0], storedV[0])
		require.EqualValues(t, key[length.Addr], storedV[1])
	}
	require.NoError(t, err)

}

func TestAggregator_ReplaceCommittedKeys(t *testing.T) {
	aggStep := uint64(10000)

	path, db, agg := testDbAndAggregator(t, 0, aggStep)
	defer db.Close()
	_ = path

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)
	defer agg.StartWrites().FinishWrites()

	var latestCommitTxNum uint64
	commit := func(txn uint64) error {
		err = tx.Commit()
		require.NoError(t, err)
		tx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		t.Logf("commit to db txn=%d", txn)

		atomic.StoreUint64(&latestCommitTxNum, txn)
		agg.SetTx(tx)
		return nil
	}

	roots := agg.AggregatedRoots()
	txs := aggStep / 2 * 50
	t.Logf("step=%d tx_count=%d", aggStep, txs)

	rnd := rand.New(rand.NewSource(0))
	keys := make([][]byte, txs/2)

	for txNum := uint64(1); txNum <= txs/2; txNum++ {
		agg.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)
		keys[txNum-1] = append(addr, loc...)

		buf := EncodeAccountBytes(1, uint256.NewInt(0), nil, 0)
		err = agg.UpdateAccountData(addr, buf)
		require.NoError(t, err)

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		err = agg.FinishTx()
		require.NoError(t, err)
		select {
		case <-roots:
			require.NoError(t, commit(txNum))
		default:
			continue
		}
	}

	half := txs / 2
	for txNum := txs/2 + 1; txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)

		addr, loc := keys[txNum-1-half][:length.Addr], keys[txNum-1-half][length.Addr:]

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		err = agg.FinishTx()
		require.NoError(t, err)
	}

	err = tx.Commit()
	tx = nil

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)

	ctx := agg.storage.MakeContext()
	defer ctx.Close()
	for _, key := range keys {
		storedV, err := ctx.Get(key[:length.Addr], key[length.Addr:], tx)
		require.NoError(t, err)
		require.EqualValues(t, key[0], storedV[0])
		require.EqualValues(t, key[length.Addr], storedV[1])
	}
	require.NoError(t, err)
}

func Test_EncodeCommitmentState(t *testing.T) {
	cs := commitmentState{
		txNum:     rand.Uint64(),
		trieState: make([]byte, 1024),
	}
	n, err := rand.Read(cs.trieState)
	require.NoError(t, err)
	require.EqualValues(t, len(cs.trieState), n)

	buf, err := cs.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	var dec commitmentState
	err = dec.Decode(buf)
	require.NoError(t, err)
	require.EqualValues(t, cs.txNum, dec.txNum)
	require.EqualValues(t, cs.trieState, dec.trieState)
}

func Test_BtreeIndex_Seek(t *testing.T) {
	tmp := t.TempDir()
	args := BtIndexWriterArgs{
		IndexFile:   path.Join(tmp, "1M.bt"),
		TmpDir:      tmp,
		KeyCount:    1_000,
		EtlBufLimit: 0,
	}
	iw, err := NewBtIndexWriter(args)
	require.NoError(t, err)

	defer iw.Close()
	defer os.RemoveAll(tmp)

	rnd := rand.New(rand.NewSource(0))
	keys := make([]byte, 52)
	lookafter := make([][]byte, 0)
	for i := 0; i < args.KeyCount; i++ {
		n, err := rnd.Read(keys[:52])
		require.EqualValues(t, n, 52)
		require.NoError(t, err)

		err = iw.AddKey(keys[:], uint64(i))
		require.NoError(t, err)

		if i%1000 < 5 {
			lookafter = append(lookafter, common.Copy(keys))
		}
	}

	require.NoError(t, iw.Build())
	iw.Close()

	bt, err := OpenBtreeIndex(args.IndexFile, "", 4)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), args.KeyCount)

	idx := NewBtIndexReader(bt)

	for i := 0; i < len(lookafter); i += 5 {
		cur, err := idx.Seek(lookafter[i])
		require.NoError(t, err)
		//require.EqualValues(t, lookafter[i], cur.key)
		require.EqualValues(t, uint64(i), cur.Value())
		for j := 0; j < 5; j++ {
			//require.EqualValues(t, lookafter[i+j], idx.Key())
			require.EqualValues(t, uint64(i+j), cur.Value())
			cur.Next()
		}
	}

	bt.Close()
}

func Test_InitBtreeIndex(t *testing.T) {
	tmp := t.TempDir()
	args := BtIndexWriterArgs{
		IndexFile: path.Join(tmp, "100k.bt"),
		TmpDir:    tmp,
		KeyCount:  100,
		KeySize:   52,
	}
	iw, err := NewBtIndexWriter(args)
	require.NoError(t, err)

	defer iw.Close()
	defer os.RemoveAll(tmp)

	rnd := rand.New(rand.NewSource(0))
	keys := make([]byte, args.KeySize)
	values := make([]byte, 300)

	comp, err := compress.NewCompressor(context.Background(), "cmp", path.Join(tmp, "100k.v2"), tmp, compress.MinPatternScore, 1, log.LvlDebug)
	require.NoError(t, err)

	for i := 0; i < args.KeyCount; i++ {
		// n, err := rnd.Read(keys[:52])
		// require.EqualValues(t, n, 52)
		// require.NoError(t, err)

		n, err := rnd.Read(values[:rnd.Intn(300)])
		require.NoError(t, err)

		err = comp.AddWord(values[:n])
		require.NoError(t, err)
	}

	err = comp.Compress()
	require.NoError(t, err)
	comp.Close()

	decomp, err := compress.NewDecompressor(path.Join(tmp, "100k.v2"))
	require.NoError(t, err)

	getter := decomp.MakeGetter()
	getter.Reset(0)

	var pos uint64
	for i := 0; i < args.KeyCount; i++ {
		if !getter.HasNext() {
			t.Fatalf("not enough values at %d", i)
			break
		}
		pos = getter.Skip()
		// getter.Next(values[:0])

		n, err := rnd.Read(keys[:args.KeySize])
		require.EqualValues(t, n, args.KeySize)
		require.NoError(t, err)

		err = iw.AddKey(keys[:], uint64(pos))
		require.NoError(t, err)
	}
	decomp.Close()

	require.NoError(t, iw.Build())
	iw.Close()

	bt, err := OpenBtreeIndex(args.IndexFile, path.Join(tmp, "100k.v2"), 4)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), args.KeyCount)
	bt.Close()
}

func Test_BtreeIndex_Allocation(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	m := 2
	for i := 5; i < 24; i++ {
		t.Run(fmt.Sprintf("%d", m<<i), func(t *testing.T) {
			for j := 0; j < 10; j++ {
				count := rnd.Intn(1000000000)
				bt := newBtAlloc(uint64(count), uint64(m)<<i)
				require.GreaterOrEqual(t, bt.N, uint64(count))
				if count < m*4 {
					continue
				}

				require.LessOrEqual(t, float64(bt.N-uint64(count))/float64(bt.N), 0.1)
			}
		})
	}
}

func Benchmark_BtreeIndex_Allocation(b *testing.B) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		now := time.Now()
		count := rnd.Intn(1000000000)
		bt := newBtAlloc(uint64(count), uint64(1<<12))
		bt.traverseDfs()
		fmt.Printf("alloc %v\n", time.Now().Sub(now))
	}
}

func Benchmark_BtreeIndex_Search(b *testing.B) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	now := time.Now()
	max := 100000000
	count := rnd.Intn(max)
	bt := newBtAlloc(uint64(count), uint64(1<<11))
	bt.traverseDfs()
	fmt.Printf("alloc %v\n", time.Now().Sub(now))

	for i := 0; i < b.N; i++ {
		bt.search(uint64(i % max))
	}
}
