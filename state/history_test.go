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
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func testDbAndHistory(t *testing.T) (string, kv.RwDB, History) {
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
	ii, err := NewHistory(path, 16 /* aggregationStep */, "hist" /* filenameBase */, keysTable, indexTable, valsTable, settingsTable)
	require.NoError(t, err)
	return path, db, ii
}

func TestHisyoryCollationBuild(t *testing.T) {
	_, db, h := testDbAndHistory(t)
	defer db.Close()
	defer h.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	h.SetTx(tx)

	h.SetTxNum(2)
	err = h.AddPrevValue([]byte("key1"), nil)
	require.NoError(t, err)

	h.SetTxNum(3)
	err = h.AddPrevValue([]byte("key2"), nil)
	require.NoError(t, err)

	h.SetTxNum(6)
	err = h.AddPrevValue([]byte("key1"), []byte("value1.1"))
	require.NoError(t, err)
	err = h.AddPrevValue([]byte("key2"), []byte("value2.1"))
	require.NoError(t, err)

	h.SetTxNum(7)
	err = h.AddPrevValue([]byte("key2"), []byte("value2.2"))
	require.NoError(t, err)
	err = h.AddPrevValue([]byte("key3"), nil)
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
	require.Equal(t, []string{"", "", "value1.1", "value2.1", "value2.2", ""}, valWords)
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
