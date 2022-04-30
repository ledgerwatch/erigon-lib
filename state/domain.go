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
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/kv"
)

// Domain is a part of the state (examples are Accounts, Storage, Code)
type Domain struct {
	aggregationStep uint64
	filenameBase    string
	valuesTable     string
	keysTable       string
	historyTable    string
	indexTable      string
	tx              kv.RwTx
	blockNum        uint64
	txNum           uint64
}

func NewDomain(
	aggregationStep uint64,
	filenameBase string,
	valuesTable string,
	keysTable string,
	historyTable string,
	indexTable string,
) *Domain {
	return &Domain{
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
		valuesTable:     valuesTable,
		keysTable:       keysTable,
		historyTable:    historyTable,
		indexTable:      indexTable,
	}
}

func (d *Domain) SetTx(tx kv.RwTx) {
	d.tx = tx
}

func (d *Domain) SetBlockNum(blockNum uint64) {
	d.blockNum = blockNum
}

func (d *Domain) SetTxNum(txNum uint64) {
	d.txNum = txNum
}

func (d *Domain) Get(key []byte) ([]byte, error) {
	var invertedStep [8]byte
	binary.BigEndian.PutUint64(invertedStep[:], ^(d.blockNum / d.aggregationStep))
	keyCursor, err := d.tx.CursorDupSort(d.keysTable)
	if err != nil {
		return nil, err
	}
	foundInvStep, err := keyCursor.SeekBothRange(key, invertedStep[:])
	if err != nil {
		return nil, err
	}
	if foundInvStep == nil {
		// TODO connect search in files here
		return nil, nil
	}
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	copy(keySuffix[len(key):], foundInvStep)
	v, err := d.tx.GetOne(d.valuesTable, keySuffix)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (d *Domain) Put(key, val []byte) error {
	invertedStep := ^(d.blockNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	if err := d.tx.Put(d.valuesTable, keySuffix, val); err != nil {
		return err
	}
	if err := d.tx.Put(d.keysTable, key, keySuffix[len(key):]); err != nil {
		return err
	}
	return nil
}

func (d *Domain) Delete(key []byte) error {
	invertedStep := ^(d.blockNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	if err := d.tx.Delete(d.valuesTable, keySuffix, nil); err != nil {
		return err
	}
	if err := d.tx.Put(d.keysTable, key, keySuffix[len(key):]); err != nil {
		return err
	}
	return nil
}
