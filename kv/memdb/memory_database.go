/*
   Copyright 2021 Erigon contributors

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

package memdb

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
)

func New(l kv.Label, tmpDir string) kv.RwDB {
	switch l {
	case kv.SentryDB:
		return mdbx.NewMDBX(log.New()).InMem(tmpDir).Label(kv.SentryDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.SentryTablesCfg }).MustOpen()
	case kv.DownloaderDB:
		return mdbx.NewMDBX(log.New()).InMem(tmpDir).Label(kv.DownloaderDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).MustOpen()
	case kv.TxPoolDB:
		return mdbx.NewMDBX(log.New()).InMem(tmpDir).Label(kv.TxPoolDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).MustOpen()
	case kv.ReconstDB:
		return mdbx.NewMDBX(log.New()).InMem(tmpDir).Label(kv.ReconstDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.ReconTablesCfg }).MustOpen()
	default:
		return mdbx.NewMDBX(log.New()).InMem(tmpDir).MustOpen()
	}
}

func NewTestDB(l kv.Label, tb testing.TB) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	tb.Helper()
	db := New(l, tmpDir)
	tb.Cleanup(db.Close)
	return db
}

func BeginRw(tb testing.TB, db kv.RwDB) kv.RwTx {
	tb.Helper()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return tx
}

func NewTestTx(tb testing.TB) (kv.RwDB, kv.RwTx) {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := New(kv.ChainDB, tmpDir)
	tb.Cleanup(db.Close)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return db, tx
}
