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
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
)

// Reconstruction of the aggregator in another package, `aggregator`

type Aggregator struct {
	aggregationStep uint64
	accounts        *Domain
	storage         *Domain
	code            *Domain
	commitment      *DomainCommitted
	logAddrs        *InvertedIndex
	logTopics       *InvertedIndex
	tracesFrom      *InvertedIndex
	tracesTo        *InvertedIndex
	txNum           uint64
	blockNum        uint64
	commitFn        func(txNum uint64) error
	rwTx            kv.RwTx
	stats           FilesStats
}

func NewAggregator(
	dir string,
	aggregationStep uint64,
) (*Aggregator, error) {

	a := &Aggregator{aggregationStep: aggregationStep}

	closeAgg := true
	defer func() {
		if closeAgg {
			a.Close()
		}
	}()
	err := os.MkdirAll(dir, 0764)
	if err != nil {
		return nil, err
	}
	if a.accounts, err = NewDomain(dir, aggregationStep, "accounts", kv.AccountKeys, kv.AccountVals, kv.AccountHistoryKeys, kv.AccountHistoryVals, kv.AccountSettings, kv.AccountIdx, 0 /* prefixLen */, false /* compressVals */); err != nil {
		return nil, err
	}
	if a.storage, err = NewDomain(dir, aggregationStep, "storage", kv.StorageKeys, kv.StorageVals, kv.StorageHistoryKeys, kv.StorageHistoryVals, kv.StorageSettings, kv.StorageIdx, 20 /* prefixLen */, false /* compressVals */); err != nil {
		return nil, err
	}
	if a.code, err = NewDomain(dir, aggregationStep, "code", kv.CodeKeys, kv.CodeVals, kv.CodeHistoryKeys, kv.CodeHistoryVals, kv.CodeSettings, kv.CodeIdx, 0 /* prefixLen */, true /* compressVals */); err != nil {
		return nil, err
	}

	commitd, err := NewDomain(dir, aggregationStep, "commitment", kv.CommitmentKeys, kv.CommitmentVals, kv.CommitmentHistoryKeys, kv.CommitmentHistoryVals, kv.CommitmentSettings, kv.CommitmentIdx, 0 /* prefixLen */, false /* compressVals */)
	if err != nil {
		return nil, err
	}
	a.commitment = NewCommittedDomain(commitd, CommitmentModeDirect)

	if a.logAddrs, err = NewInvertedIndex(dir, aggregationStep, "logaddrs", kv.LogAddressKeys, kv.LogAddressIdx); err != nil {
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

func (a *Aggregator) GetAndResetStats() DomainStats {
	stats := DomainStats{}
	stats.Accumulate(a.accounts.GetAndResetStats())
	stats.Accumulate(a.storage.GetAndResetStats())
	stats.Accumulate(a.code.GetAndResetStats())
	stats.Accumulate(a.commitment.GetAndResetStats())
	return stats
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
	if a.commitment != nil {
		a.commitment.Close()
	}

	if a.logAddrs != nil {
		a.logAddrs.Close()
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
	a.rwTx = tx
	a.accounts.SetTx(tx)
	a.storage.SetTx(tx)
	a.code.SetTx(tx)
	a.commitment.SetTx(tx)
	a.logAddrs.SetTx(tx)
	a.logTopics.SetTx(tx)
	a.tracesFrom.SetTx(tx)
	a.tracesTo.SetTx(tx)
}

func (a *Aggregator) SetTxNum(txNum uint64) {
	a.txNum = txNum
	a.accounts.SetTxNum(txNum)
	a.storage.SetTxNum(txNum)
	a.code.SetTxNum(txNum)
	a.commitment.SetTxNum(txNum)
	a.logAddrs.SetTxNum(txNum)
	a.logTopics.SetTxNum(txNum)
	a.tracesFrom.SetTxNum(txNum)
	a.tracesTo.SetTxNum(txNum)
}

func (a *Aggregator) SetBlockNum(bn uint64) { a.blockNum = bn }

func (a *Aggregator) SetWorkers(i int) {
	a.accounts.workers = i
	a.storage.workers = i
	a.code.workers = i
	a.commitment.workers = i
	a.logAddrs.workers = i
	a.logTopics.workers = i
	a.tracesFrom.workers = i
	a.tracesTo.workers = i
}

func (a *Aggregator) SetCommitmentMode(mode CommitmentMode) {
	a.commitment.mode = mode
}

type AggCollation struct {
	accounts   Collation
	storage    Collation
	code       Collation
	commitment Collation
	logAddrs   map[string]*roaring64.Bitmap
	logTopics  map[string]*roaring64.Bitmap
	tracesFrom map[string]*roaring64.Bitmap
	tracesTo   map[string]*roaring64.Bitmap
}

func (c AggCollation) Close() {
	c.accounts.Close()
	c.storage.Close()
	c.code.Close()
	c.commitment.Close()
}

func (a *Aggregator) collate(step uint64, txFrom, txTo uint64, roTx kv.Tx) (AggCollation, error) {
	var ac AggCollation
	var err error
	closeColl := true
	defer func() {
		if closeColl {
			ac.Close()
		}
	}()
	if ac.accounts, err = a.accounts.collate(step, txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.storage, err = a.storage.collate(step, txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.code, err = a.code.collate(step, txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.commitment, err = a.commitment.collate(step, txFrom, txTo, roTx); err != nil {
		return AggCollation{}, err
	}
	if ac.logAddrs, err = a.logAddrs.collate(txFrom, txTo, roTx); err != nil {
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
	accounts   StaticFiles
	storage    StaticFiles
	code       StaticFiles
	commitment StaticFiles
	logAddrs   InvertedFiles
	logTopics  InvertedFiles
	tracesFrom InvertedFiles
	tracesTo   InvertedFiles
}

func (sf AggStaticFiles) Close() {
	sf.accounts.Close()
	sf.storage.Close()
	sf.code.Close()
	sf.commitment.Close()
	sf.logAddrs.Close()
	sf.logTopics.Close()
	sf.tracesFrom.Close()
	sf.tracesTo.Close()
}

func (a *Aggregator) buildFiles(step uint64, collation AggCollation) (AggStaticFiles, error) {
	var sf AggStaticFiles
	closeFiles := true
	defer func() {
		if closeFiles {
			sf.Close()
		}
	}()
	var wg sync.WaitGroup
	wg.Add(8)
	errCh := make(chan error, 8)
	go func() {
		defer wg.Done()
		var err error
		if sf.accounts, err = a.accounts.buildFiles(step, collation.accounts); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if sf.storage, err = a.storage.buildFiles(step, collation.storage); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if sf.code, err = a.code.buildFiles(step, collation.code); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if sf.commitment, err = a.commitment.buildFiles(step, collation.commitment); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if sf.logAddrs, err = a.logAddrs.buildFiles(step, collation.logAddrs); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if sf.logTopics, err = a.logTopics.buildFiles(step, collation.logTopics); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if sf.tracesFrom, err = a.tracesFrom.buildFiles(step, collation.tracesFrom); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if sf.tracesTo, err = a.tracesTo.buildFiles(step, collation.tracesTo); err != nil {
			errCh <- err
		}
	}()
	go func() {
		wg.Wait()
		close(errCh)
	}()
	var lastError error
	for err := range errCh {
		lastError = err
	}
	if lastError == nil {
		closeFiles = false
	}
	return sf, lastError
}

func (a *Aggregator) integrateFiles(sf AggStaticFiles, txNumFrom, txNumTo uint64) {
	a.accounts.integrateFiles(sf.accounts, txNumFrom, txNumTo)
	a.storage.integrateFiles(sf.storage, txNumFrom, txNumTo)
	a.code.integrateFiles(sf.code, txNumFrom, txNumTo)
	a.commitment.integrateFiles(sf.commitment, txNumFrom, txNumTo)
	a.logAddrs.integrateFiles(sf.logAddrs, txNumFrom, txNumTo)
	a.logTopics.integrateFiles(sf.logTopics, txNumFrom, txNumTo)
	a.tracesFrom.integrateFiles(sf.tracesFrom, txNumFrom, txNumTo)
	a.tracesTo.integrateFiles(sf.tracesTo, txNumFrom, txNumTo)
}

func (a *Aggregator) prune(step uint64, txFrom, txTo, limit uint64) error {
	if err := a.accounts.prune(step, txFrom, txTo, limit); err != nil {
		return err
	}
	if err := a.storage.prune(step, txFrom, txTo, limit); err != nil {
		return err
	}
	if err := a.code.prune(step, txFrom, txTo, limit); err != nil {
		return err
	}
	if err := a.commitment.prune(step, txFrom, txTo, limit); err != nil {
		return err
	}
	if err := a.logAddrs.prune(txFrom, txTo, limit); err != nil {
		return err
	}
	if err := a.logTopics.prune(txFrom, txTo, limit); err != nil {
		return err
	}
	if err := a.tracesFrom.prune(txFrom, txTo, limit); err != nil {
		return err
	}
	if err := a.tracesTo.prune(txFrom, txTo, limit); err != nil {
		return err
	}
	return nil
}

func (a *Aggregator) EndTxNumMinimax() uint64 {
	min := a.accounts.endTxNumMinimax()
	if txNum := a.storage.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.code.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.commitment.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.logAddrs.endTxNumMinimax(); txNum < min {
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

// TODO make it a part of EndTxNumMinimax()
func (a *Aggregator) SeekCommitment() (uint64, uint64, error) {
	return a.commitment.SeekCommitment(a.aggregationStep)
}

type Ranges struct {
	accounts                                 DomainRanges
	storage                                  DomainRanges
	code                                     DomainRanges
	commitment                               DomainRanges
	logAddrsStartTxNum, logAddrsEndTxNum     uint64
	logAddrs                                 bool
	logTopicsStartTxNum, logTopicsEndTxNum   uint64
	logTopics                                bool
	tracesFromStartTxNum, tracesFromEndTxNum uint64
	tracesFrom                               bool
	tracesToStartTxNum, tracesToEndTxNum     uint64
	tracesTo                                 bool
}

func (r Ranges) any() bool {
	return r.accounts.any() || r.storage.any() || r.code.any() || r.commitment.any() || r.logAddrs || r.logTopics || r.tracesFrom || r.tracesTo
}

func (a *Aggregator) findMergeRange(maxEndTxNum, maxSpan uint64) Ranges {
	var r Ranges
	r.accounts = a.accounts.findMergeRange(maxEndTxNum, maxSpan)
	r.storage = a.storage.findMergeRange(maxEndTxNum, maxSpan)
	r.code = a.code.findMergeRange(maxEndTxNum, maxSpan)
	r.commitment = a.commitment.findMergeRange(maxEndTxNum, maxSpan)
	r.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum = a.logAddrs.findMergeRange(maxEndTxNum, maxSpan)
	r.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum = a.logTopics.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum = a.tracesFrom.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum = a.tracesTo.findMergeRange(maxEndTxNum, maxSpan)
	log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%+v\n", maxEndTxNum, maxSpan, r))
	return r
}

type SelectedStaticFiles struct {
	accounts                      []*filesItem
	accountsIdx, accountsHist     []*filesItem
	accountsI                     int
	storage                       []*filesItem
	storageIdx, storageHist       []*filesItem
	storageI                      int
	code                          []*filesItem
	codeIdx, codeHist             []*filesItem
	codeI                         int
	commitment                    []*filesItem
	commitmentIdx, commitmentHist []*filesItem
	commitmentI                   int
	logAddrs                      []*filesItem
	logAddrsI                     int
	logTopics                     []*filesItem
	logTopicsI                    int
	tracesFrom                    []*filesItem
	tracesFromI                   int
	tracesTo                      []*filesItem
	tracesToI                     int
}

func (sf SelectedStaticFiles) Close() {
	for _, group := range [][]*filesItem{
		sf.accounts, sf.accountsIdx, sf.accountsHist,
		sf.storage, sf.storageIdx, sf.storageHist,
		sf.code, sf.codeIdx, sf.codeHist,
		sf.commitment, sf.commitmentIdx, sf.commitmentHist,
		sf.logAddrs, sf.logTopics, sf.tracesFrom, sf.tracesTo,
	} {
		for _, item := range group {
			if item != nil {
				if item.decompressor != nil {
					item.decompressor.Close()
				}
				if item.index != nil {
					item.index.Close()
				}
			}
		}
	}
}

func (a *Aggregator) staticFilesInRange(r Ranges) SelectedStaticFiles {
	var sf SelectedStaticFiles
	if r.accounts.any() {
		sf.accounts, sf.accountsIdx, sf.accountsHist, sf.accountsI = a.accounts.staticFilesInRange(r.accounts)
	}
	if r.storage.any() {
		sf.storage, sf.storageIdx, sf.storageHist, sf.storageI = a.storage.staticFilesInRange(r.storage)
	}
	if r.code.any() {
		sf.code, sf.codeIdx, sf.codeHist, sf.codeI = a.code.staticFilesInRange(r.code)
	}
	if r.commitment.any() {
		sf.commitment, sf.commitmentIdx, sf.commitmentHist, sf.commitmentI = a.commitment.staticFilesInRange(r.commitment)
	}
	if r.logAddrs {
		sf.logAddrs, sf.logAddrsI = a.logAddrs.staticFilesInRange(r.logAddrsStartTxNum, r.logAddrsEndTxNum)
	}
	if r.logTopics {
		sf.logTopics, sf.logTopicsI = a.logTopics.staticFilesInRange(r.logTopicsStartTxNum, r.logTopicsEndTxNum)
	}
	if r.tracesFrom {
		sf.tracesFrom, sf.tracesFromI = a.tracesFrom.staticFilesInRange(r.tracesFromStartTxNum, r.tracesFromEndTxNum)
	}
	if r.tracesTo {
		sf.tracesTo, sf.tracesToI = a.tracesTo.staticFilesInRange(r.tracesToStartTxNum, r.tracesToEndTxNum)
	}
	return sf
}

type MergedFiles struct {
	accounts                      *filesItem
	accountsIdx, accountsHist     *filesItem
	storage                       *filesItem
	storageIdx, storageHist       *filesItem
	code                          *filesItem
	codeIdx, codeHist             *filesItem
	commitment                    *filesItem
	commitmentIdx, commitmentHist *filesItem
	logAddrs                      *filesItem
	logTopics                     *filesItem
	tracesFrom                    *filesItem
	tracesTo                      *filesItem
}

func (mf MergedFiles) Close() {
	for _, item := range []*filesItem{
		mf.accounts, mf.accountsIdx, mf.accountsHist,
		mf.storage, mf.storageIdx, mf.storageHist,
		mf.code, mf.codeIdx, mf.codeHist,
		mf.commitment, mf.commitmentIdx, mf.commitmentHist,
		mf.logAddrs, mf.logTopics, mf.tracesFrom, mf.tracesTo,
	} {
		if item != nil {
			if item.decompressor != nil {
				item.decompressor.Close()
			}
			if item.decompressor != nil {
				item.index.Close()
			}
		}
	}
}

func (a *Aggregator) mergeFiles(files SelectedStaticFiles, r Ranges, maxSpan uint64) (MergedFiles, error) {
	var mf MergedFiles
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}
	}()
	var wg sync.WaitGroup
	wg.Add(7)
	errCh := make(chan error, 8)
	go func() {
		defer wg.Done()
		var err error
		if r.accounts.any() {
			if mf.accounts, mf.accountsIdx, mf.accountsHist, err = a.accounts.mergeFiles(files.accounts, files.accountsIdx, files.accountsHist, r.accounts, maxSpan); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.storage.any() {
			if mf.storage, mf.storageIdx, mf.storageHist, err = a.storage.mergeFiles(files.storage, files.storageIdx, files.storageHist, r.storage, maxSpan); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.code.any() {
			if mf.code, mf.codeIdx, mf.codeHist, err = a.code.mergeFiles(files.code, files.codeIdx, files.codeHist, r.code, maxSpan); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.logAddrs {
			if mf.logAddrs, err = a.logAddrs.mergeFiles(files.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum, maxSpan); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.logTopics {
			if mf.logTopics, err = a.logTopics.mergeFiles(files.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum, maxSpan); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.tracesFrom {
			if mf.tracesFrom, err = a.tracesFrom.mergeFiles(files.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum, maxSpan); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.tracesTo {
			if mf.tracesTo, err = a.tracesTo.mergeFiles(files.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum, maxSpan); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		wg.Wait()

		var err error
		if r.commitment.any() {
			//if mf.commitment, mf.commitmentIdx, mf.commitmentHist, err = a.commitment.mergeFiles(files.commitment, files.commitmentIdx, files.commitmentHist, r.commitment, maxSpan); err != nil {
			if mf.commitment, mf.commitmentIdx, mf.commitmentHist, err = a.commitment.mergeFiles(files, mf, r.commitment, maxSpan); err != nil {
				errCh <- err
			}
		}
		close(errCh)
	}()
	var lastError error
	for err := range errCh {
		lastError = err
	}
	if lastError == nil {
		closeFiles = false
	}
	return mf, lastError
}

func (a *Aggregator) integrateMergedFiles(outs SelectedStaticFiles, in MergedFiles) {
	a.accounts.integrateMergedFiles(outs.accounts, outs.accountsIdx, outs.accountsHist, in.accounts, in.accountsIdx, in.accountsHist)
	a.storage.integrateMergedFiles(outs.storage, outs.storageIdx, outs.storageHist, in.storage, in.storageIdx, in.storageHist)
	a.code.integrateMergedFiles(outs.code, outs.codeIdx, outs.codeHist, in.code, in.codeIdx, in.codeHist)
	a.commitment.integrateMergedFiles(outs.commitment, outs.commitmentIdx, outs.commitmentHist, in.commitment, in.commitmentIdx, in.commitmentHist)
	a.logAddrs.integrateMergedFiles(outs.logAddrs, in.logAddrs)
	a.logTopics.integrateMergedFiles(outs.logTopics, in.logTopics)
	a.tracesFrom.integrateMergedFiles(outs.tracesFrom, in.tracesFrom)
	a.tracesTo.integrateMergedFiles(outs.tracesTo, in.tracesTo)
}

func (a *Aggregator) deleteFiles(outs SelectedStaticFiles) error {
	if err := a.accounts.deleteFiles(outs.accounts, outs.accountsIdx, outs.accountsHist); err != nil {
		return err
	}
	if err := a.storage.deleteFiles(outs.storage, outs.storageIdx, outs.storageHist); err != nil {
		return err
	}
	if err := a.code.deleteFiles(outs.code, outs.codeIdx, outs.codeHist); err != nil {
		return err
	}
	if err := a.commitment.deleteFiles(outs.commitment, outs.commitmentIdx, outs.commitmentHist); err != nil {
		return err
	}
	if err := a.logAddrs.deleteFiles(outs.logAddrs); err != nil {
		return err
	}
	if err := a.logTopics.deleteFiles(outs.logTopics); err != nil {
		return err
	}
	if err := a.tracesFrom.deleteFiles(outs.tracesFrom); err != nil {
		return err
	}
	if err := a.tracesTo.deleteFiles(outs.tracesTo); err != nil {
		return err
	}
	return nil
}

func (ac *AggregatorContext) ReadAccountData(addr []byte, roTx kv.Tx) ([]byte, error) {
	return ac.accounts.Get(addr, nil, roTx)
}

func (ac *AggregatorContext) ReadAccountDataBeforeTxNum(addr []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	return ac.accounts.GetBeforeTxNum(addr, txNum, roTx)
}

func (ac *AggregatorContext) ReadAccountStorage(addr []byte, loc []byte, roTx kv.Tx) ([]byte, error) {
	return ac.storage.Get(addr, loc, roTx)
}

func (ac *AggregatorContext) ReadAccountStorageBeforeTxNum(addr []byte, loc []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	if cap(ac.keyBuf) < len(addr)+len(loc) {
		ac.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ac.keyBuf) != len(addr)+len(loc) {
		ac.keyBuf = ac.keyBuf[:len(addr)+len(loc)]
	}
	copy(ac.keyBuf, addr)
	copy(ac.keyBuf[len(addr):], loc)
	return ac.storage.GetBeforeTxNum(ac.keyBuf, txNum, roTx)
}

func (ac *AggregatorContext) ReadAccountCode(addr []byte, roTx kv.Tx) ([]byte, error) {
	return ac.code.Get(addr, nil, roTx)
}

func (ac *AggregatorContext) ReadCommitment(addr []byte, roTx kv.Tx) ([]byte, error) {
	return ac.commitment.Get(addr, nil, roTx)
}

func (ac *AggregatorContext) ReadCommitmentBeforeTxNum(addr []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	return ac.commitment.GetBeforeTxNum(addr, txNum, roTx)
}

func (ac *AggregatorContext) ReadAccountCodeBeforeTxNum(addr []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	return ac.code.GetBeforeTxNum(addr, txNum, roTx)
}

func (ac *AggregatorContext) ReadAccountCodeSize(addr []byte, roTx kv.Tx) (int, error) {
	code, err := ac.code.Get(addr, nil, roTx)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (ac *AggregatorContext) ReadAccountCodeSizeBeforeTxNum(addr []byte, txNum uint64, roTx kv.Tx) (int, error) {
	code, err := ac.code.GetBeforeTxNum(addr, txNum, roTx)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

func (a *AggregatorContext) branchFn(prefix []byte) ([]byte, error) {
	// Look in the summary table first
	stateValue, err := a.ReadCommitment(prefix, a.a.rwTx)
	if err != nil {
		return nil, fmt.Errorf("failed read branch %x: %w", commitment.CompactedKeyToHex(prefix), err)
	}
	if stateValue == nil {
		return nil, nil
	}
	// fmt.Printf("Returning branch data prefix [%x], mergeVal=[%x]\n", commitment.CompactedKeyToHex(prefix), stateValue)
	return stateValue[2:], nil // Skip touchMap but keep afterMap
}

func (a *AggregatorContext) accountFn(plainKey []byte, cell *commitment.Cell) error {
	encAccount, err := a.ReadAccountData(plainKey, a.a.rwTx)
	if err != nil {
		return err
	}
	cell.Nonce = 0
	cell.Balance.Clear()
	copy(cell.CodeHash[:], commitment.EmptyCodeHash)
	if len(encAccount) > 0 {
		nonce, balance, chash := DecodeAccountBytes(encAccount)
		cell.Nonce = nonce
		cell.Balance.Set(balance)
		if chash != nil {
			copy(cell.CodeHash[:], chash)
		}
	}

	code, err := a.ReadAccountCode(plainKey, a.a.rwTx)
	if err != nil {
		return err
	}
	if code != nil {
		a.a.commitment.keccak.Reset()
		a.a.commitment.keccak.Write(code)
		copy(cell.CodeHash[:], a.a.commitment.keccak.Sum(nil))
	}
	cell.Delete = len(encAccount) == 0 && len(code) == 0
	return nil
}

func (a *AggregatorContext) storageFn(plainKey []byte, cell *commitment.Cell) error {
	// Look in the summary table first
	enc, err := a.ReadAccountStorage(plainKey[:length.Addr], plainKey[length.Addr:], a.a.rwTx)
	if err != nil {
		return err
	}
	cell.StorageLen = len(enc)
	copy(cell.Storage[:], enc)
	cell.Delete = cell.StorageLen == 0
	return nil
}

// Evaluates commitment for processed state. Commit=true - store trie state after evaluation
func (a *Aggregator) ComputeCommitment(saveStateAfter, trace bool) (rootHash []byte, err error) {
	ctx := a.MakeContext()
	rootHash, branchNodeUpdates, err := a.commitment.ComputeCommitment(ctx, trace)
	if err != nil {
		return nil, err
	}

	for pref, update := range branchNodeUpdates {
		prefix := []byte(pref)

		stateValue, err := ctx.ReadCommitment(prefix, a.rwTx)
		if err != nil {
			return nil, err
		}

		stated := commitment.BranchData(stateValue)
		merged, err := stated.MergeHexBranches(update, nil)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(stated, merged) {
			continue
		}
		if trace {
			fmt.Printf("computeCommitment merge [%x] [%x]+[%x]=>[%x]\n", prefix, stated, update, merged)
		}
		if err = a.UpdateCommitmentData(prefix, merged); err != nil {
			return nil, err
		}
	}

	if saveStateAfter {
		if err := a.commitment.storeCommitmentState(a.blockNum, a.txNum); err != nil {
			return nil, err
		}
	}

	return rootHash, nil
}

func (a *Aggregator) ReadyToFinishTx() bool {
	return (a.txNum+1)%a.aggregationStep == 0
}

func (a *Aggregator) SetCommitFn(fn func(txNum uint64) error) {
	a.commitFn = fn
}

func (a *Aggregator) FinishTx() error {
	atomic.AddUint64(&a.stats.TxCount, 1)

	if !a.ReadyToFinishTx() {
		return nil
	}
	closeAll := true
	step := a.txNum / a.aggregationStep
	if step == 0 {
		if a.commitFn != nil {
			_, err := a.ComputeCommitment(true, false)
			if err != nil {
				return err
			}
			if err := a.commitFn(a.txNum); err != nil {
				return fmt.Errorf("aggregator: db commit on finishTx failed, txNum=%d err=%w", a.txNum, err)
			}
		}
		return nil
	}
	step-- // Leave one step worth in the DB
	collation, err := a.collate(step, step*a.aggregationStep, (step+1)*a.aggregationStep, a.rwTx)
	if err != nil {
		return err
	}
	defer func() {
		if closeAll {
			collation.Close()
		}
	}()

	sf, err := a.buildFiles(step, collation)
	if err != nil {
		return err
	}
	defer func() {
		if closeAll {
			sf.Close()
		}
	}()
	a.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)
	if err = a.prune(step, step*a.aggregationStep, (step+1)*a.aggregationStep, math.MaxUint64); err != nil {
		return err
	}
	maxEndTxNum := a.EndTxNumMinimax()
	maxSpan := uint64(32) * a.aggregationStep
	for r := a.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = a.findMergeRange(maxEndTxNum, maxSpan) {
		outs := a.staticFilesInRange(r)
		defer func() {
			if closeAll {
				outs.Close()
			}
		}()
		in, err := a.mergeFiles(outs, r, maxSpan)
		if err != nil {
			return err
		}
		defer func() {
			if closeAll {
				in.Close()
			}
		}()
		a.integrateMergedFiles(outs, in)
		if err = a.deleteFiles(outs); err != nil {
			return err
		}
	}
	closeAll = false

	if _, err = a.ComputeCommitment(true, false); err != nil {
		return err
	}
	if a.commitFn != nil {
		if err := a.commitFn(a.txNum); err != nil {
			return err
		}
	}

	return nil
}

func (a *Aggregator) UpdateAccountData(addr []byte, account []byte) error {
	a.commitment.TouchPlainKey(addr, account, a.commitment.TouchPlainKeyAccount)
	return a.accounts.Put(addr, nil, account)
}

func (a *Aggregator) UpdateAccountCode(addr []byte, code []byte) error {
	a.commitment.TouchPlainKey(addr, code, a.commitment.TouchPlainKeyCode)
	if len(code) == 0 {
		return a.code.Delete(addr, nil)
	}
	return a.code.Put(addr, nil, code)
}

func (a *Aggregator) UpdateCommitmentData(prefix []byte, code []byte) error {
	return a.commitment.Put(prefix, nil, code)
}

func (a *Aggregator) DeleteAccount(addr []byte) error {
	a.commitment.TouchPlainKey(addr, nil, a.commitment.TouchPlainKeyAccount)

	if err := a.accounts.Delete(addr, nil); err != nil {
		return err
	}
	if err := a.code.Delete(addr, nil); err != nil {
		return err
	}
	var e error
	if err := a.storage.defaultDc.IteratePrefix(addr, func(k, _ []byte) {
		a.commitment.TouchPlainKey(k, nil, a.commitment.TouchPlainKeyStorage)
		if e == nil {
			e = a.storage.Delete(k, nil)
		}
	}); err != nil {
		return err
	}
	return e
}

func (a *Aggregator) WriteAccountStorage(addr, loc []byte, value []byte) error {
	composite := make([]byte, len(addr)+len(loc))
	copy(composite, addr)
	copy(composite[length.Addr:], loc)

	a.commitment.TouchPlainKey(composite, value, a.commitment.TouchPlainKeyStorage)
	if len(value) == 0 {
		return a.storage.Delete(addr, loc)
	}
	return a.storage.Put(addr, loc, value)
}

func (a *Aggregator) AddTraceFrom(addr []byte) error {
	return a.tracesFrom.Add(addr)
}

func (a *Aggregator) AddTraceTo(addr []byte) error {
	return a.tracesTo.Add(addr)
}

func (a *Aggregator) AddLogAddr(addr []byte) error {
	return a.logAddrs.Add(addr)
}

func (a *Aggregator) AddLogTopic(topic []byte) error {
	return a.logTopics.Add(topic)
}

func (ac *AggregatorContext) LogAddrIterator(addr []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.logAddrs.IterateRange(addr, startTxNum, endTxNum, roTx)
}

func (ac *AggregatorContext) LogTopicIterator(topic []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.logTopics.IterateRange(topic, startTxNum, endTxNum, roTx)
}

func (ac *AggregatorContext) TraceFromIterator(addr []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.tracesFrom.IterateRange(addr, startTxNum, endTxNum, roTx)
}

func (ac *AggregatorContext) TraceToIterator(addr []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.tracesTo.IterateRange(addr, startTxNum, endTxNum, roTx)
}

type FilesStats struct {
	TxCount    uint64
	FilesCount uint64
	IdxSize    uint64
	DataSize   uint64
}

func (a *Aggregator) Stats() FilesStats {
	res := a.stats
	stat := a.GetAndResetStats()
	res.IdxSize = stat.IndexSize
	res.DataSize = stat.DataSize
	res.FilesCount = stat.FilesCount
	return res
}

type AggregatorContext struct {
	a          *Aggregator
	accounts   *DomainContext
	storage    *DomainContext
	code       *DomainContext
	commitment *DomainContext
	logAddrs   *InvertedIndexContext
	logTopics  *InvertedIndexContext
	tracesFrom *InvertedIndexContext
	tracesTo   *InvertedIndexContext
	keyBuf     []byte
}

func (a *Aggregator) MakeContext() *AggregatorContext {
	return &AggregatorContext{
		a:          a,
		accounts:   a.accounts.MakeContext(),
		storage:    a.storage.MakeContext(),
		code:       a.code.MakeContext(),
		commitment: a.commitment.MakeContext(),
		logAddrs:   a.logAddrs.MakeContext(),
		logTopics:  a.logTopics.MakeContext(),
		tracesFrom: a.tracesFrom.MakeContext(),
		tracesTo:   a.tracesTo.MakeContext(),
	}
}

func DecodeAccountBytes(enc []byte) (nonce uint64, balance *uint256.Int, hash []byte) {
	balance = new(uint256.Int)

	if len(enc) > 0 {
		pos := 0
		nonceBytes := int(enc[pos])
		pos++
		if nonceBytes > 0 {
			nonce = bytesToUint64(enc[pos : pos+nonceBytes])
			pos += nonceBytes
		}
		balanceBytes := int(enc[pos])
		pos++
		if balanceBytes > 0 {
			balance.SetBytes(enc[pos : pos+balanceBytes])
			pos += balanceBytes
		}
		codeHashBytes := int(enc[pos])
		pos++
		if codeHashBytes > 0 {
			codeHash := make([]byte, length.Hash)
			copy(codeHash[:], enc[pos:pos+codeHashBytes])
		}
	}
	return
}

func EncodeAccountBytes(nonce uint64, balance *uint256.Int, hash []byte, incarnation uint64) []byte {
	l := int(1)
	if nonce > 0 {
		l += (bits.Len64(nonce) + 7) / 8
	}
	l++
	if !balance.IsZero() {
		l += balance.ByteLen()
	}
	l++
	if len(hash) == length.Hash {
		l += 32
	}
	l++
	if incarnation > 0 {
		l += (bits.Len64(incarnation) + 7) / 8
	}
	value := make([]byte, l)
	pos := 0

	if nonce == 0 {
		value[pos] = 0
		pos++
	} else {
		nonceBytes := (bits.Len64(nonce) + 7) / 8
		value[pos] = byte(nonceBytes)
		var nonce = nonce
		for i := nonceBytes; i > 0; i-- {
			value[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}
	if balance.IsZero() {
		value[pos] = 0
		pos++
	} else {
		balanceBytes := balance.ByteLen()
		value[pos] = byte(balanceBytes)
		pos++
		balance.WriteToSlice(value[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	if len(hash) == 0 {
		value[pos] = 0
		pos++
	} else {
		value[pos] = 32
		pos++
		copy(value[pos:pos+32], hash[:])
		pos += 32
	}
	if incarnation == 0 {
		value[pos] = 0
	} else {
		incBytes := (bits.Len64(incarnation) + 7) / 8
		value[pos] = byte(incBytes)
		var inc = incarnation
		for i := incBytes; i > 0; i-- {
			value[pos+i] = byte(inc)
			inc >>= 8
		}
	}
	return value
}
