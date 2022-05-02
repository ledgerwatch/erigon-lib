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
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestDomian(t *testing.T) {
	path := t.TempDir()
	logger := log.New()
	valuesTable := "Values"
	keysTable := "Keys"
	historyTable := "History"
	indexTable := "Index"
	blockTxTable := "BlockTx"
	db := mdbx.NewMDBX(logger).Path(path).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			valuesTable:  kv.TableCfgItem{},
			keysTable:    kv.TableCfgItem{Flags: kv.DupSort},
			historyTable: kv.TableCfgItem{},
			indexTable:   kv.TableCfgItem{Flags: kv.DupSort},
			blockTxTable: kv.TableCfgItem{},
		}
	}).MustOpen()
	defer db.Close()

	d := NewDomain(path, 16 /* aggregationStep */, "base" /* filenameBase */, valuesTable, keysTable, historyTable, indexTable, blockTxTable)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)

	d.SetBlockNum(1, 1)
	d.SetTxNum(2)
	err = d.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	d.SetTxNum(3)
	err = d.Put([]byte("key2"), []byte("value2.1"))
	require.NoError(t, err)

	d.SetBlockNum(2, 5)
	d.SetTxNum(6)
	err = d.Put([]byte("key1"), []byte("value1.2"))
	require.NoError(t, err)

}
