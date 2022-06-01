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
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
)

// Reconstruction of the aggregator in another package, `aggregator`

type Aggregator struct {
	aggregationStep uint64
	accounts        *Domain
	storage         *Domain
	code            *Domain
	logAccounts     *InvertedIndex
	logTopics       *InvertedIndex
	tracesFrom      *InvertedIndex
	tracesTo        *InvertedIndex
}

func NewAggregator(
	dir string,
	aggregationStep uint64,
) (*Aggregator, error) {
	a := &Aggregator{
		aggregationStep: aggregationStep,
	}
	closeAgg := true
	defer func() {
		if closeAgg {
			if a.accounts != nil {
				a.accounts.Close()
			}
			if a.storage != nil {
				a.storage.Close()
			}
			if a.code != nil {
				a.code.Close()
			}
			if a.logAccounts != nil {
				a.logAccounts.Close()
			}
			if a.logTopics != nil {
				a.logTopics.Close()
			}
			if a.tracesFrom != nil {
				a.tracesFrom.Close()
			}
			if a.tracesTo != nil {
				a.tracesTo.Close()
			}
		}
	}()
	var err error
	if a.accounts, err = NewDomain(dir, aggregationStep, "accounts", kv.AccountKeys, kv.AccountVals, kv.AccountHistoryKeys, kv.AccountHistoryVals, kv.AccountSettings, kv.AccountIdx, 0 /* prefixLen */); err != nil {
		return nil, err
	}
	if a.storage, err = NewDomain(dir, aggregationStep, "storage", kv.StorageKeys, kv.StorageVals, kv.StorageHistoryKeys, kv.StorageHistoryVals, kv.StorageSettings, kv.StorageIdx, 20 /* prefixLen */); err != nil {
		return nil, err
	}
	if a.code, err = NewDomain(dir, aggregationStep, "code", kv.CodeKeys, kv.CodeVals, kv.CodeHistoryKeys, kv.CodeHistoryVals, kv.CodeSettings, kv.CodeIdx, 0 /* prefixLen */); err != nil {
		return nil, err
	}
	if a.logAccounts, err = NewInvertedIndex(dir, aggregationStep, "logaddrs", kv.LogAddressKeys, kv.LogAddressIdx); err != nil {
		return nil, err
	}
	if a.logTopics, err = NewInvertedIndex(dir, aggregationStep, "logtopics", kv.LogTopicsKeys, kv.LogTopicsIdx); err != nil {
		return nil, err
	}
	if a.tracesFrom, err = NewInvertedIndex(dir, aggregationStep, "tracesfrom", kv.TracesFromKeys, kv.TracesFromIdx); err != nil {
		return nil, err
	}
	if a.tracesTo, err = NewInvertedIndex(dir, aggregationStep, "tracesto", kv.TracesToKeys, kv.TracesToIdx); err != nil {
		return nil, err
	}
	closeAgg = false
	return a, nil
}

func (a *Aggregator) Close() {
	if a.accounts != nil {
		a.accounts.Close()
	}
	if a.storage != nil {
		a.storage.Close()
	}
	if a.code != nil {
		a.code.Close()
	}
	if a.logAccounts != nil {
		a.logAccounts.Close()
	}
	if a.logTopics != nil {
		a.logTopics.Close()
	}
	if a.tracesFrom != nil {
		a.tracesFrom.Close()
	}
	if a.tracesTo != nil {
		a.tracesTo.Close()
	}
}

func (a *Aggregator) SetTx(tx kv.RwTx) {
	a.accounts.SetTx(tx)
	a.storage.SetTx(tx)
	a.code.SetTx(tx)
	a.logAccounts.SetTx(tx)
	a.logTopics.SetTx(tx)
	a.tracesFrom.SetTx(tx)
	a.tracesTo.SetTx(tx)
}

func (a *Aggregator) SetTxNum(txNum uint64) {
	a.accounts.SetTxNum(txNum)
	a.storage.SetTxNum(txNum)
	a.code.SetTxNum(txNum)
	a.logAccounts.SetTxNum(txNum)
	a.logTopics.SetTxNum(txNum)
	a.tracesFrom.SetTxNum(txNum)
	a.tracesTo.SetTxNum(txNum)
}

type AggCollation struct {
	accounts    Collation
	storage     Collation
	code        Collation
	logAccounts map[string]*roaring64.Bitmap
	logTopics   map[string]*roaring64.Bitmap
	tracesFrom  map[string]*roaring64.Bitmap
	tracesTo    map[string]*roaring64.Bitmap
}

func (a *Aggregator) collate(step uint64, txFrom, txTo uint64, roTx kv.Tx) (AggCollation, error) {
	var ac AggCollation
	var err error
	closeColl := true
	defer func() {
		if closeColl {
			ac.accounts.Close()
			ac.storage.Close()
			ac.code.Close()
		}
	}()
	if ac.accounts, err = a.accounts.collate(step, txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.storage, err = a.storage.collate(step, txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.storage, err = a.storage.collate(step, txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.logAccounts, err = a.logAccounts.collate(txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.logTopics, err = a.logTopics.collate(txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.tracesFrom, err = a.tracesFrom.collate(txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.tracesTo, err = a.tracesTo.collate(txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	closeColl = false
	return ac, nil
}

type AggStaticFiles struct {
	accounts    StaticFiles
	storage     StaticFiles
	code        StaticFiles
	logAccounts InvertedFiles
	logTopics   InvertedFiles
	tracesFrom  InvertedFiles
	tracesTo    InvertedFiles
}

func (sf AggStaticFiles) Close() {
	sf.accounts.Close()
	sf.storage.Close()
	sf.code.Close()
	sf.logAccounts.Close()
	sf.logTopics.Close()
	sf.tracesFrom.Close()
	sf.tracesTo.Close()
}

func (a *Aggregator) buildFiles(step uint64, collation AggCollation) (AggStaticFiles, error) {
	var sf AggStaticFiles
	var err error
	closeFiles := true
	defer func() {
		if closeFiles {
			sf.accounts.Close()
			sf.storage.Close()
			sf.code.Close()
			sf.logAccounts.Close()
			sf.logTopics.Close()
			sf.tracesFrom.Close()
			sf.tracesTo.Close()
		}
	}()
	if sf.accounts, err = a.accounts.buildFiles(step, collation.accounts); err != nil {
		return AggStaticFiles{}, err
	}
	if sf.storage, err = a.storage.buildFiles(step, collation.storage); err != nil {
		return AggStaticFiles{}, err
	}
	if sf.code, err = a.code.buildFiles(step, collation.code); err != nil {
		return AggStaticFiles{}, err
	}
	if sf.logAccounts, err = a.logAccounts.buildFiles(step, collation.logAccounts); err != nil {
		return AggStaticFiles{}, err
	}
	if sf.logTopics, err = a.logTopics.buildFiles(step, collation.logTopics); err != nil {
		return AggStaticFiles{}, err
	}
	if sf.tracesFrom, err = a.tracesFrom.buildFiles(step, collation.tracesFrom); err != nil {
		return AggStaticFiles{}, err
	}
	if sf.tracesTo, err = a.tracesTo.buildFiles(step, collation.tracesTo); err != nil {
		return AggStaticFiles{}, err
	}
	closeFiles = false
	return sf, nil
}

func (a *Aggregator) integrateFiles(sf AggStaticFiles, txNumFrom, txNumTo uint64) {
	a.accounts.integrateFiles(sf.accounts, txNumFrom, txNumTo)
	a.storage.integrateFiles(sf.storage, txNumFrom, txNumTo)
	a.code.integrateFiles(sf.code, txNumFrom, txNumTo)
	a.logAccounts.integrateFiles(sf.logAccounts, txNumFrom, txNumTo)
	a.logTopics.integrateFiles(sf.logTopics, txNumFrom, txNumTo)
	a.tracesFrom.integrateFiles(sf.tracesFrom, txNumFrom, txNumTo)
	a.tracesTo.integrateFiles(sf.tracesTo, txNumFrom, txNumTo)
}

func (a *Aggregator) prune(step uint64, txFrom, txTo uint64) error {
	if err := a.accounts.prune(step, txFrom, txTo); err != nil {
		return err
	}
	if err := a.storage.prune(step, txFrom, txTo); err != nil {
		return err
	}
	if err := a.code.prune(step, txFrom, txTo); err != nil {
		return err
	}
	if err := a.logAccounts.prune(txFrom, txTo); err != nil {
		return err
	}
	if err := a.logTopics.prune(txFrom, txTo); err != nil {
		return err
	}
	if err := a.tracesFrom.prune(txFrom, txTo); err != nil {
		return err
	}
	if err := a.tracesTo.prune(txFrom, txTo); err != nil {
		return err
	}
	return nil
}

func (a *Aggregator) endTxNumMinimax() uint64 {
	min := a.accounts.endTxNumMinimax()
	if txNum := a.storage.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.code.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.logAccounts.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.logTopics.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.tracesFrom.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.tracesTo.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	return min
}

type SelectedStaticFiles struct {
	accounts     [][NumberOfTypes]*filesItem
	accountsI    int
	storage      [][NumberOfTypes]*filesItem
	storageI     int
	code         [][NumberOfTypes]*filesItem
	codeI        int
	logAccounts  []*filesItem
	logAccountsI int
	logTopics    []*filesItem
	logTopicsI   int
	tracesFrom   []*filesItem
	tracesFromI  int
	tracesTo     []*filesItem
	tracesToI    int
}

func (a *Aggregator) staticFilesInRange(startTxNum, endTxNum uint64) SelectedStaticFiles {
	var sf SelectedStaticFiles
	sf.accounts, sf.accountsI = a.accounts.staticFilesInRange(startTxNum, endTxNum)
	sf.storage, sf.storageI = a.storage.staticFilesInRange(startTxNum, endTxNum)
	sf.code, sf.codeI = a.code.staticFilesInRange(startTxNum, endTxNum)
	sf.logAccounts, sf.logAccountsI = a.logAccounts.staticFilesInRange(startTxNum, endTxNum)
	sf.logTopics, sf.logTopicsI = a.logTopics.staticFilesInRange(startTxNum, endTxNum)
	sf.tracesFrom, sf.tracesFromI = a.tracesFrom.staticFilesInRange(startTxNum, endTxNum)
	sf.tracesTo, sf.tracesToI = a.tracesTo.staticFilesInRange(startTxNum, endTxNum)
	return sf
}

func (a *Aggregator) ReadAccountData(addr []byte, roTx kv.Tx) ([]byte, error) {
	return a.accounts.Get(addr, roTx)
}
