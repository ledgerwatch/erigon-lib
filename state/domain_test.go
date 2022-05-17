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
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func testDbAndDomain(t *testing.T) (kv.RwDB, *Domain) {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	keysTable := "Keys"
	valsTable := "Vals"
	historyKeysTable := "HistoryKeys"
	historyValsTable := "HistoryVals"
	historyValsCount := "HistoryValsCount"
	indexTable := "Index"
	db := mdbx.NewMDBX(logger).Path(path).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:        kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:        kv.TableCfgItem{},
			historyKeysTable: kv.TableCfgItem{Flags: kv.DupSort},
			historyValsTable: kv.TableCfgItem{},
			historyValsCount: kv.TableCfgItem{},
			indexTable:       kv.TableCfgItem{Flags: kv.DupSort},
		}
	}).MustOpen()
	d, err := NewDomain(path, 16 /* aggregationStep */, "base" /* filenameBase */, keysTable, valsTable, historyKeysTable, historyValsTable, historyValsCount, indexTable)
	require.NoError(t, err)
	return db, d
}

func TestCollation(t *testing.T) {
	db, d := testDbAndDomain(t)
	defer db.Close()
	defer d.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)

	d.SetTxNum(2)
	err = d.Put([]byte("key1"), []byte("value1.1"))
	require.NoError(t, err)

	d.SetTxNum(3)
	err = d.Put([]byte("key2"), []byte("value2.1"))
	require.NoError(t, err)

	d.SetTxNum(6)
	err = d.Put([]byte("key1"), []byte("value1.2"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	c, err := d.collate(0, 0, 7, roTx)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(c.valuesPath, "base-values.0-16.dat"))
	require.Equal(t, 2, c.valuesCount)
	require.True(t, strings.HasSuffix(c.historyPath, "base-history.0-16.dat"))
	require.Equal(t, 3, c.historyCount)
	require.Equal(t, 2, len(c.indexBitmaps))
	require.Equal(t, []uint64{3}, c.indexBitmaps["key2"].ToArray())
	require.Equal(t, []uint64{2, 6}, c.indexBitmaps["key1"].ToArray())

	sf, err := d.buildFiles(0, c)
	defer sf.Close()
	require.NoError(t, err)
	g := sf.valuesDecomp.MakeGetter()
	g.Reset(0)
	var words []string
	for g.HasNext() {
		w, _ := g.Next(nil)
		words = append(words, string(w))
	}
	require.Equal(t, []string{"key1", "value1.2", "key2", "value2.1"}, words)
	// Check index
	require.Equal(t, 2, int(sf.valuesIdx.KeyCount()))
	r := recsplit.NewIndexReader(sf.valuesIdx)
	for i := 0; i < len(words); i += 2 {
		offset := r.Lookup([]byte(words[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, words[i], string(w))
		w, _ = g.Next(nil)
		require.Equal(t, words[i+1], string(w))
	}
	g = sf.historyDecomp.MakeGetter()
	g.Reset(0)
	words = words[:0]
	for g.HasNext() {
		w, _ := g.Next(nil)
		words = append(words, string(w))
	}
	require.Equal(t, []string{"\x00\x00\x00\x00\x00\x00\x00\x02key1", "", "\x00\x00\x00\x00\x00\x00\x00\x03key2", "", "\x00\x00\x00\x00\x00\x00\x00\x06key1", "value1.2"}, words)
	require.Equal(t, 3, int(sf.historyIdx.KeyCount()))
	r = recsplit.NewIndexReader(sf.historyIdx)
	for i := 0; i < len(words); i += 2 {
		offset := r.Lookup([]byte(words[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, words[i], string(w))
		w, _ = g.Next(nil)
		require.Equal(t, words[i+1], string(w))
	}
}

func TestIteration(t *testing.T) {
	db, d := testDbAndDomain(t)
	defer db.Close()
	defer d.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)

	d.SetTxNum(2)
	err = d.Put([]byte("addr1loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1loc3"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3loc2"), []byte("value1"))
	require.NoError(t, err)

	var keys []string
	err = d.IteratePrefix([]byte("addr2"), func(k []byte) {
		keys = append(keys, string(k))
	})
	require.NoError(t, err)
	require.Equal(t, []string{"addr2loc1", "addr2loc2"}, keys)
}
