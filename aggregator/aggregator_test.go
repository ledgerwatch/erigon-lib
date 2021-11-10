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

package aggregator

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
)

func int160(i uint64) []byte {
	b := make([]byte, 20)
	binary.BigEndian.PutUint64(b[12:], i)
	return b
}

func int256(i uint64) []byte {
	b := make([]byte, 32)
	binary.BigEndian.PutUint64(b[24:], i)
	return b
}

func TestAggregator(t *testing.T) {
	tmpDir := t.TempDir()
	db := memdb.New()
	defer db.Close()
	a, err := NewAggregator(tmpDir, 16, 4)
	if err != nil {
		t.Fatal(err)
	}
	var rwTx kv.RwTx
	if rwTx, err = db.BeginRw(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer rwTx.Rollback()

	var w *Writer
	if w, err = a.MakeStateWriter(rwTx, 0); err != nil {
		t.Fatal(err)
	}
	var account1 = int256(1)
	if err = w.UpdateAccountData(int160(1), account1); err != nil {
		t.Fatal(err)
	}
	if err = w.Finish(); err != nil {
		t.Fatal(err)
	}
	a.Close()
}
