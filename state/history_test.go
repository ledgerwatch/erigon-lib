/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func testDbAndHistory(t *testing.T) (string, kv.RwDB, *History) {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	keysTable := "Keys"
	indexTable := "Index"
	valsTable := "Vals"
	settingsTable := "Settings"
	db := mdbx.NewMDBX(logger).Path(path).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:     kv.TableCfgItem{Flags: kv.DupSort},
			indexTable:    kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:     kv.TableCfgItem{},
			settingsTable: kv.TableCfgItem{},
		}
	}).MustOpen()
	ii, err := NewHistory(path, 16 /* aggregationStep */, "hist" /* filenameBase */, keysTable, indexTable, valsTable, settingsTable, false /* compressVals */)
	require.NoError(t, err)
	return path, db, ii
}

func TestHistoryCollationBuild(t *testing.T) {
	_, db, h := testDbAndHistory(t)
	defer db.Close()
	defer h.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	h.SetTx(tx)

	h.SetTxNum(2)
	err = h.AddPrevValue([]byte("key1"), nil, nil)
	require.NoError(t, err)

	h.SetTxNum(3)
	err = h.AddPrevValue([]byte("key2"), nil, nil)
	require.NoError(t, err)

	h.SetTxNum(6)
	err = h.AddPrevValue([]byte("key1"), nil, []byte("value1.1"))
	require.NoError(t, err)
	err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.1"))
	require.NoError(t, err)

	h.SetTxNum(7)
	err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.2"))
	require.NoError(t, err)
	err = h.AddPrevValue([]byte("key3"), nil, nil)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	c, err := h.collate(0, 0, 8, roTx)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(c.historyPath, "hist.0-1.v"))
	require.Equal(t, 6, c.historyCount)
	require.Equal(t, 3, len(c.indexBitmaps))
	require.Equal(t, []uint64{7}, c.indexBitmaps["key3"].ToArray())
	require.Equal(t, []uint64{3, 6, 7}, c.indexBitmaps["key2"].ToArray())
	require.Equal(t, []uint64{2, 6}, c.indexBitmaps["key1"].ToArray())

	sf, err := h.buildFiles(0, c)
	require.NoError(t, err)
	defer sf.Close()
	var valWords []string
	g := sf.historyDecomp.MakeGetter()
	g.Reset(0)
	for g.HasNext() {
		w, _ := g.Next(nil)
		valWords = append(valWords, string(w))
	}
	require.Equal(t, []string{"", "value1.1", "", "value2.1", "value2.2", ""}, valWords)
	require.Equal(t, 6, int(sf.historyIdx.KeyCount()))
	g = sf.efHistoryDecomp.MakeGetter()
	g.Reset(0)
	var keyWords []string
	var intArrs [][]uint64
	for g.HasNext() {
		w, _ := g.Next(nil)
		keyWords = append(keyWords, string(w))
		w, _ = g.Next(w[:0])
		ef, _ := eliasfano32.ReadEliasFano(w)
		var ints []uint64
		it := ef.Iterator()
		for it.HasNext() {
			ints = append(ints, it.Next())
		}
		intArrs = append(intArrs, ints)
	}
	require.Equal(t, []string{"key1", "key2", "key3"}, keyWords)
	require.Equal(t, [][]uint64{{2, 6}, {3, 6, 7}, {7}}, intArrs)
	r := recsplit.NewIndexReader(sf.efHistoryIdx)
	for i := 0; i < len(keyWords); i++ {
		offset := r.Lookup([]byte(keyWords[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, keyWords[i], string(w))
	}
	r = recsplit.NewIndexReader(sf.historyIdx)
	g = sf.historyDecomp.MakeGetter()
	var vi int
	for i := 0; i < len(keyWords); i++ {
		ints := intArrs[i]
		for j := 0; j < len(ints); j++ {
			var txKey [8]byte
			binary.BigEndian.PutUint64(txKey[:], ints[j])
			offset := r.Lookup2(txKey[:], []byte(keyWords[i]))
			g.Reset(offset)
			w, _ := g.Next(nil)
			require.Equal(t, valWords[vi], string(w))
			vi++
		}
	}
}

func TestHistoryAfterPrune(t *testing.T) {
	_, db, h := testDbAndHistory(t)
	defer db.Close()
	defer h.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	h.SetTx(tx)

	h.SetTxNum(2)
	err = h.AddPrevValue([]byte("key1"), nil, nil)
	require.NoError(t, err)

	h.SetTxNum(3)
	err = h.AddPrevValue([]byte("key2"), nil, nil)
	require.NoError(t, err)

	h.SetTxNum(6)
	err = h.AddPrevValue([]byte("key1"), nil, []byte("value1.1"))
	require.NoError(t, err)
	err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.1"))
	require.NoError(t, err)

	h.SetTxNum(7)
	err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.2"))
	require.NoError(t, err)
	err = h.AddPrevValue([]byte("key3"), nil, nil)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	c, err := h.collate(0, 0, 16, roTx)
	require.NoError(t, err)

	sf, err := h.buildFiles(0, c)
	require.NoError(t, err)
	defer sf.Close()

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	h.SetTx(tx)

	h.integrateFiles(sf, 0, 16)

	err = h.prune(0, 0, 16)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	h.SetTx(tx)

	for _, table := range []string{h.indexKeysTable, h.historyValsTable, h.indexTable} {
		var cur kv.Cursor
		cur, err = tx.Cursor(table)
		require.NoError(t, err)
		defer cur.Close()
		var k []byte
		k, _, err = cur.First()
		require.NoError(t, err)
		require.Nil(t, k, table)
	}
}

func filledHistory(t *testing.T) (string, kv.RwDB, *History, uint64) {
	t.Helper()
	path, db, h := testDbAndHistory(t)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	h.SetTx(tx)
	txs := uint64(1000)
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var prevVal [32][]byte
	for txNum := uint64(1); txNum <= txs; txNum++ {
		h.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			if txNum%keyNum == 0 {
				valNum := txNum / keyNum
				var k [8]byte
				var v [8]byte
				binary.BigEndian.PutUint64(k[:], keyNum)
				binary.BigEndian.PutUint64(v[:], valNum)
				err = h.AddPrevValue(k[:], nil, prevVal[keyNum])
				require.NoError(t, err)
				prevVal[keyNum] = v[:]
			}
		}
		if txNum%10 == 0 {
			err = tx.Commit()
			require.NoError(t, err)
			tx, err = db.BeginRw(context.Background())
			require.NoError(t, err)
			h.SetTx(tx)
		}
	}
	err = tx.Commit()
	require.NoError(t, err)
	tx = nil
	return path, db, h, txs
}

func checkHistoryHistory(t *testing.T, db kv.RwDB, h *History, txs uint64) {
	t.Helper()
	// Check the history
	hc := h.MakeContext()
	for txNum := uint64(0); txNum <= txs; txNum++ {
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			valNum := txNum / keyNum
			var k [8]byte
			var v [8]byte
			label := fmt.Sprintf("txNum=%d, keyNum=%d", txNum, keyNum)
			//fmt.Printf("label=%s\n", label)
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], valNum)
			val, ok, _, err := hc.GetNoState(k[:], txNum+1)
			//require.Equal(t, ok, txNum < 976)
			if ok {
				require.NoError(t, err, label)
				if txNum >= keyNum {
					require.Equal(t, v[:], val, label)
				} else {
					require.Equal(t, []byte{}, val, label)
				}
			}
		}
	}
}

func TestHistoryHistory(t *testing.T) {
	_, db, h, txs := filledHistory(t)
	defer db.Close()
	defer h.Close()
	var tx kv.RwTx
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/h.aggregationStep-1; step++ {
		func() {
			roTx, err := db.BeginRo(context.Background())
			require.NoError(t, err)
			c, err := h.collate(step, step*h.aggregationStep, (step+1)*h.aggregationStep, roTx)
			roTx.Rollback()
			require.NoError(t, err)
			sf, err := h.buildFiles(step, c)
			require.NoError(t, err)
			h.integrateFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)
			tx, err = db.BeginRw(context.Background())
			require.NoError(t, err)
			h.SetTx(tx)
			err = h.prune(step, step*h.aggregationStep, (step+1)*h.aggregationStep)
			require.NoError(t, err)
			err = tx.Commit()
			require.NoError(t, err)
			tx = nil
		}()
	}
	checkHistoryHistory(t, db, h, txs)
}

func collateAndMergeHistory(t *testing.T, db kv.RwDB, h *History, txs uint64) {
	t.Helper()
	var tx kv.RwTx
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/h.aggregationStep-1; step++ {
		func() {
			roTx, err := db.BeginRo(context.Background())
			require.NoError(t, err)
			defer roTx.Rollback()
			c, err := h.collate(step, step*h.aggregationStep, (step+1)*h.aggregationStep, roTx)
			require.NoError(t, err)
			roTx.Rollback()
			sf, err := h.buildFiles(step, c)
			require.NoError(t, err)
			h.integrateFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)
			tx, err = db.BeginRw(context.Background())
			require.NoError(t, err)
			h.SetTx(tx)
			err = h.prune(step, step*h.aggregationStep, (step+1)*h.aggregationStep)
			require.NoError(t, err)
			err = tx.Commit()
			require.NoError(t, err)
			tx = nil
			var r HistoryRanges
			maxEndTxNum := h.endTxNumMinimax()
			maxSpan := uint64(16 * 16)
			for r = h.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = h.findMergeRange(maxEndTxNum, maxSpan) {
				indexOuts, historyOuts, _ := h.staticFilesInRange(r)
				indexIn, historyIn, err := h.mergeFiles(indexOuts, historyOuts, r, maxSpan)
				require.NoError(t, err)
				h.integrateMergedFiles(indexOuts, historyOuts, indexIn, historyIn)
				err = h.deleteFiles(indexOuts, historyOuts)
				require.NoError(t, err)
			}
		}()
	}
}

func TestHistoryMergeFiles(t *testing.T) {
	_, db, h, txs := filledHistory(t)
	defer db.Close()
	defer h.Close()

	collateAndMergeHistory(t, db, h, txs)
	checkHistoryHistory(t, db, h, txs)
}

func TestHistoryScanFiles(t *testing.T) {
	path, db, h, txs := filledHistory(t)
	defer db.Close()
	defer func() {
		h.Close()
	}()
	var err error
	var tx kv.RwTx
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	collateAndMergeHistory(t, db, h, txs)
	// Recreate domain and re-scan the files
	txNum := h.txNum
	h.Close()
	h, err = NewHistory(path, h.aggregationStep, h.filenameBase, h.indexKeysTable, h.indexTable, h.historyValsTable, h.settingsTable, h.compressVals)
	require.NoError(t, err)
	h.SetTxNum(txNum)
	// Check the history
	checkHistoryHistory(t, db, h, txs)
}
