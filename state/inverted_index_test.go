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
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func testDbAndInvertedIndex(t *testing.T) (kv.RwDB, *InvertedIndex) {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	keysTable := "Keys"
	indexTable := "Index"
	db := mdbx.NewMDBX(logger).Path(path).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:  kv.TableCfgItem{Flags: kv.DupSort},
			indexTable: kv.TableCfgItem{Flags: kv.DupSort},
		}
	}).MustOpen()
	ii, err := NewInvertedIndex(path, 16 /* aggregationStep */, "inv" /* filenameBase */, keysTable, indexTable)
	require.NoError(t, err)
	return db, ii
}

func TestInvIndexCollationBuild(t *testing.T) {
	db, ii := testDbAndInvertedIndex(t)
	defer db.Close()
	defer ii.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	ii.SetTx(tx)

	ii.SetTxNum(2)
	err = ii.Add([]byte("key1"))
	require.NoError(t, err)

	ii.SetTxNum(3)
	err = ii.Add([]byte("key2"))
	require.NoError(t, err)

	ii.SetTxNum(6)
	err = ii.Add([]byte("key1"))
	require.NoError(t, err)
	err = ii.Add([]byte("key3"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	bs, err := ii.collate(0, 7, roTx)
	require.NoError(t, err)
	require.Equal(t, 3, len(bs))
	require.Equal(t, []uint64{3}, bs["key2"].ToArray())
	require.Equal(t, []uint64{2, 6}, bs["key1"].ToArray())
	require.Equal(t, []uint64{6}, bs["key3"].ToArray())

	sf, err := ii.buildFiles(0, bs)
	require.NoError(t, err)
	defer sf.Close()
	g := sf.decomp.MakeGetter()
	g.Reset(0)
	var words []string
	var intArrs [][]uint64
	for g.HasNext() {
		w, _ := g.Next(nil)
		words = append(words, string(w))
		w, _ = g.Next(w[:0])
		ef, _ := eliasfano32.ReadEliasFano(w)
		var ints []uint64
		it := ef.Iterator()
		for it.HasNext() {
			ints = append(ints, it.Next())
		}
		intArrs = append(intArrs, ints)
	}
	require.Equal(t, []string{"key1", "key2", "key3"}, words)
	require.Equal(t, [][]uint64{{2, 6}, {3}, {6}}, intArrs)
	r := recsplit.NewIndexReader(sf.index)
	for i := 0; i < len(words); i++ {
		offset := r.Lookup([]byte(words[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, words[i], string(w))
	}
}

func TestInvIndexAfterPrune(t *testing.T) {
	db, ii := testDbAndInvertedIndex(t)
	defer db.Close()
	defer ii.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	ii.SetTx(tx)

	ii.SetTxNum(2)
	err = ii.Add([]byte("key1"))
	require.NoError(t, err)

	ii.SetTxNum(3)
	err = ii.Add([]byte("key2"))
	require.NoError(t, err)

	ii.SetTxNum(6)
	err = ii.Add([]byte("key1"))
	require.NoError(t, err)
	err = ii.Add([]byte("key3"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	bs, err := ii.collate(0, 16, roTx)
	require.NoError(t, err)

	sf, err := ii.buildFiles(0, bs)
	require.NoError(t, err)
	defer sf.Close()

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	ii.SetTx(tx)

	ii.integrateFiles(sf, 0, 16)

	err = ii.prune(0, 16)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	ii.SetTx(tx)

	for _, table := range []string{ii.keysTable, ii.indexTable} {
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
