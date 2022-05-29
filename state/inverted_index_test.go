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
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
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
	ii, err := NewInvertedIndex(path, 16 /* aggregationStep */, "base" /* filenameBase */, keysTable, indexTable)
	require.NoError(t, err)
	return db, ii
}

func TestCreation(t *testing.T) {
	db, ii := testDbAndInvertedIndex(t)
	defer db.Close()
	defer ii.Close()
}
