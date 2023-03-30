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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	math2 "math"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/log/v3"

	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/commitment"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
)

type AggregatorV3 struct {
	rwTx             kv.RwTx
	db               kv.RoDB
	shared           *SharedDomains
	accounts         *Domain
	storage          *Domain
	code             *Domain
	commitment       *DomainCommitted
	tracesTo         *InvertedIndex
	backgroundResult *BackgroundResult
	logAddrs         *InvertedIndex
	logTopics        *InvertedIndex
	tracesFrom       *InvertedIndex
	logPrefix        string
	dir              string
	tmpdir           string
	txNum            atomic.Uint64
	blockNum         atomic.Uint64
	aggregationStep  uint64
	keepInDB         uint64
	maxTxNum         atomic.Uint64

	filesMutationLock sync.Mutex

	working                atomic.Bool
	workingMerge           atomic.Bool
	workingOptionalIndices atomic.Bool
	//warmupWorking          atomic.Bool
	ctx       context.Context
	ctxCancel context.CancelFunc

	needSaveFilesListInDB atomic.Bool
	wg                    sync.WaitGroup

	onFreeze OnFreezeFunc
	walLock  sync.RWMutex
}

type OnFreezeFunc func(frozenFileNames []string)

func NewAggregatorV3(ctx context.Context, dir, tmpdir string, aggregationStep uint64, db kv.RoDB) (*AggregatorV3, error) {
	ctx, ctxCancel := context.WithCancel(ctx)
	a := &AggregatorV3{ctx: ctx, ctxCancel: ctxCancel, onFreeze: func(frozenFileNames []string) {}, dir: dir, tmpdir: tmpdir, aggregationStep: aggregationStep, backgroundResult: &BackgroundResult{}, db: db, keepInDB: 2 * aggregationStep}
	var err error
	if a.accounts, err = NewDomain(dir, a.tmpdir, aggregationStep, "accounts", kv.AccountKeys, kv.AccountVals, kv.AccountHistoryKeys, kv.AccountHistoryVals, kv.AccountIdx, false, false); err != nil {
		return nil, err
	}
	if a.storage, err = NewDomain(dir, a.tmpdir, aggregationStep, "storage", kv.StorageKeys, kv.StorageVals, kv.StorageHistoryKeys, kv.StorageHistoryVals, kv.StorageIdx, false, false); err != nil {
		return nil, err
	}
	if a.code, err = NewDomain(dir, a.tmpdir, aggregationStep, "code", kv.CodeKeys, kv.CodeVals, kv.CodeHistoryKeys, kv.CodeHistoryVals, kv.CodeIdx, true, true); err != nil {
		return nil, err
	}
	commitd, err := NewDomain(dir, tmpdir, aggregationStep, "commitment", kv.CommitmentKeys, kv.CommitmentVals, kv.CommitmentHistoryKeys, kv.CommitmentHistoryVals, kv.CommitmentIdx, false, true)
	if err != nil {
		return nil, err
	}
	a.commitment = NewCommittedDomain(commitd, CommitmentModeDirect, commitment.VariantHexPatriciaTrie)
	if a.logAddrs, err = NewInvertedIndex(dir, a.tmpdir, aggregationStep, "logaddrs", kv.LogAddressKeys, kv.LogAddressIdx, false, nil); err != nil {
		return nil, err
	}
	if a.logTopics, err = NewInvertedIndex(dir, a.tmpdir, aggregationStep, "logtopics", kv.LogTopicsKeys, kv.LogTopicsIdx, false, nil); err != nil {
		return nil, err
	}
	if a.tracesFrom, err = NewInvertedIndex(dir, a.tmpdir, aggregationStep, "tracesfrom", kv.TracesFromKeys, kv.TracesFromIdx, false, nil); err != nil {
		return nil, err
	}
	if a.tracesTo, err = NewInvertedIndex(dir, a.tmpdir, aggregationStep, "tracesto", kv.TracesToKeys, kv.TracesToIdx, false, nil); err != nil {
		return nil, err
	}

	a.shared = NewSharedDomains(path.Join(tmpdir, "domains"), a.accounts, a.storage, a.code, a.commitment)
	a.recalcMaxTxNum()
	return a, nil
}
func (a *AggregatorV3) OnFreeze(f OnFreezeFunc) { a.onFreeze = f }

func (a *AggregatorV3) OpenFolder() error {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()
	var err error
	if err = a.accounts.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.storage.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.code.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.logAddrs.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.logTopics.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.tracesFrom.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.tracesTo.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	a.recalcMaxTxNum()
	return nil
}
func (a *AggregatorV3) OpenList(fNames []string) error {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()

	var err error
	if err = a.accounts.OpenList(fNames); err != nil {
		return err
	}
	if err = a.storage.OpenList(fNames); err != nil {
		return err
	}
	if err = a.code.OpenList(fNames); err != nil {
		return err
	}
	if err = a.commitment.OpenList(fNames); err != nil {
		return err
	}
	if err = a.logAddrs.OpenList(fNames); err != nil {
		return err
	}
	if err = a.logTopics.OpenList(fNames); err != nil {
		return err
	}
	if err = a.tracesFrom.OpenList(fNames); err != nil {
		return err
	}
	if err = a.tracesTo.OpenList(fNames); err != nil {
		return err
	}
	a.recalcMaxTxNum()
	return nil
}

func (a *AggregatorV3) Close() {
	a.ctxCancel()
	a.wg.Wait()

	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()

	a.accounts.Close()
	a.storage.Close()
	a.code.Close()
	a.commitment.Close()
	a.logAddrs.Close()
	a.logTopics.Close()
	a.tracesFrom.Close()
	a.tracesTo.Close()
	a.shared.Close()
}

/*
// CleanDir - remove all useless files. call it manually on startup of Main application (don't call it from utilities)
func (a *AggregatorV3) CleanDir() {
	a.accounts.CleanupDir()
	a.storage.CleanupDir()
	a.code.CleanupDir()
	a.logAddrs.CleanupDir()
	a.logTopics.CleanupDir()
	a.tracesFrom.CleanupDir()
	a.tracesTo.CleanupDir()
}
*/

func (a *AggregatorV3) SetWorkers(i int) {
	a.accounts.compressWorkers = i
	a.storage.compressWorkers = i
	a.code.compressWorkers = i
	a.commitment.compressWorkers = i
	a.logAddrs.compressWorkers = i
	a.logTopics.compressWorkers = i
	a.tracesFrom.compressWorkers = i
	a.tracesTo.compressWorkers = i
}

func (a *AggregatorV3) Files() (res []string) {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()

	res = append(res, a.accounts.Files()...)
	res = append(res, a.storage.Files()...)
	res = append(res, a.code.Files()...)
	res = append(res, a.commitment.Files()...)
	res = append(res, a.logAddrs.Files()...)
	res = append(res, a.logTopics.Files()...)
	res = append(res, a.tracesFrom.Files()...)
	res = append(res, a.tracesTo.Files()...)
	return res
}
func (a *AggregatorV3) BuildOptionalMissedIndicesInBackground(ctx context.Context, workers int) {
	if ok := a.workingOptionalIndices.CompareAndSwap(false, true); !ok {
		return
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer a.workingOptionalIndices.Store(false)
		if err := a.BuildOptionalMissedIndices(ctx, workers); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn("merge", "err", err)
		}
	}()
}

func (a *AggregatorV3) BuildOptionalMissedIndices(ctx context.Context, workers int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	if a.accounts != nil {
		g.Go(func() error { return a.accounts.BuildOptionalMissedIndices(ctx) })
	}
	if a.storage != nil {
		g.Go(func() error { return a.storage.BuildOptionalMissedIndices(ctx) })
	}
	if a.code != nil {
		g.Go(func() error { return a.code.BuildOptionalMissedIndices(ctx) })
	}
	if a.commitment != nil {
		g.Go(func() error { return a.commitment.BuildOptionalMissedIndices(ctx) })
	}
	return g.Wait()
}

func (a *AggregatorV3) BuildMissedIndices(ctx context.Context, workers int) error {
	{
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(workers)
		if err := a.accounts.BuildMissedIndices(ctx, g); err != nil {
			return err
		}
		if err := a.storage.BuildMissedIndices(ctx, g); err != nil {
			return err
		}
		if err := a.code.BuildMissedIndices(ctx, g); err != nil {
			return err
		}
		if err := a.commitment.BuildMissedIndices(ctx, g); err != nil {
			return err
		}
		a.logAddrs.BuildMissedIndices(ctx, g)
		a.logTopics.BuildMissedIndices(ctx, g)
		a.tracesFrom.BuildMissedIndices(ctx, g)
		a.tracesTo.BuildMissedIndices(ctx, g)

		if err := g.Wait(); err != nil {
			return err
		}
		if err := a.OpenFolder(); err != nil {
			return err
		}
	}

	return a.BuildOptionalMissedIndices(ctx, workers)
}

func (a *AggregatorV3) SetLogPrefix(v string) { a.logPrefix = v }

func (a *AggregatorV3) SetTx(tx kv.RwTx) {
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

func (a *AggregatorV3) SetTxNum(txNum uint64) {
	a.txNum.Store(txNum)
	a.accounts.SetTxNum(txNum)
	a.storage.SetTxNum(txNum)
	a.code.SetTxNum(txNum)
	a.commitment.SetTxNum(txNum)
	a.logAddrs.SetTxNum(txNum)
	a.logTopics.SetTxNum(txNum)
	a.tracesFrom.SetTxNum(txNum)
	a.tracesTo.SetTxNum(txNum)
}

type AggV3Collation struct {
	logAddrs   map[string]*roaring64.Bitmap
	logTopics  map[string]*roaring64.Bitmap
	tracesFrom map[string]*roaring64.Bitmap
	tracesTo   map[string]*roaring64.Bitmap
	accounts   Collation
	storage    Collation
	code       Collation
	commitment Collation
}

func (c AggV3Collation) Close() {
	c.accounts.Close()
	c.storage.Close()
	c.code.Close()
	c.commitment.Close()

	for _, b := range c.logAddrs {
		bitmapdb.ReturnToPool64(b)
	}
	for _, b := range c.logTopics {
		bitmapdb.ReturnToPool64(b)
	}
	for _, b := range c.tracesFrom {
		bitmapdb.ReturnToPool64(b)
	}
	for _, b := range c.tracesTo {
		bitmapdb.ReturnToPool64(b)
	}
}

func (a *AggregatorV3) buildFiles(ctx context.Context, step, txFrom, txTo uint64) (AggV3StaticFiles, error) {
	logEvery := time.NewTicker(60 * time.Second)
	defer logEvery.Stop()
	defer func(t time.Time) {
		log.Info(fmt.Sprintf("[snapshot] build %d-%d", step, step+1), "took", time.Since(t))
	}(time.Now())
	var sf AggV3StaticFiles
	var ac AggV3Collation
	closeColl := true
	defer func() {
		if closeColl {
			ac.Close()
		}
	}()
	//var wg sync.WaitGroup
	//wg.Add(8)
	//errCh := make(chan error, 8)
	//go func() {
	//	defer wg.Done()
	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.accounts, err = a.accounts.collateStream(ctx, step, txFrom, txTo, tx, logEvery)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.accounts, err = a.accounts.buildFiles(ctx, step, ac.accounts); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.storage, err = a.storage.collateStream(ctx, step, txFrom, txTo, tx, logEvery)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.storage, err = a.storage.buildFiles(ctx, step, ac.storage); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.code, err = a.code.collateStream(ctx, step, txFrom, txTo, tx, logEvery)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.code, err = a.code.buildFiles(ctx, step, ac.code); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()

	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.commitment, err = a.commitment.collateStream(ctx, step, txFrom, txTo, tx, logEvery)
		return err
	}); err != nil {
		return sf, err
	}

	if sf.commitment, err = a.commitment.buildFiles(ctx, step, ac.commitment); err != nil {
		return sf, err
	}

	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.logAddrs, err = a.logAddrs.collate(ctx, txFrom, txTo, tx, logEvery)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.logAddrs, err = a.logAddrs.buildFiles(ctx, step, ac.logAddrs); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.logTopics, err = a.logTopics.collate(ctx, txFrom, txTo, tx, logEvery)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.logTopics, err = a.logTopics.buildFiles(ctx, step, ac.logTopics); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.tracesFrom, err = a.tracesFrom.collate(ctx, txFrom, txTo, tx, logEvery)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.tracesFrom, err = a.tracesFrom.buildFiles(ctx, step, ac.tracesFrom); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.tracesTo, err = a.tracesTo.collate(ctx, txFrom, txTo, tx, logEvery)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.tracesTo, err = a.tracesTo.buildFiles(ctx, step, ac.tracesTo); err != nil {
		return sf, err
		//		errCh <- err
	}
	//}()
	//go func() {
	//	wg.Wait()
	//close(errCh)
	//}()
	//var lastError error
	//for err := range errCh {
	//	if err != nil {
	//		lastError = err
	//	}
	//}
	//if lastError == nil {
	closeColl = false
	//}
	return sf, nil
}

type AggV3StaticFiles struct {
	accounts   StaticFiles
	storage    StaticFiles
	code       StaticFiles
	commitment StaticFiles
	logAddrs   InvertedFiles
	logTopics  InvertedFiles
	tracesFrom InvertedFiles
	tracesTo   InvertedFiles
}

func (sf AggV3StaticFiles) Close() {
	sf.accounts.Close()
	sf.storage.Close()
	sf.code.Close()
	sf.logAddrs.Close()
	sf.logTopics.Close()
	sf.tracesFrom.Close()
	sf.tracesTo.Close()
}

func (a *AggregatorV3) aggregate(ctx context.Context, step uint64) error {
	var (
		logEvery = time.NewTicker(time.Second * 30)
		wg       sync.WaitGroup
		errCh    = make(chan error, 8)
		//maxSpan  = StepsInBiggestFile * a.aggregationStep
		txFrom = step * a.aggregationStep
		txTo   = (step + 1) * a.aggregationStep
		//workers  = 1

		stepStartedAt = time.Now()
	)

	defer logEvery.Stop()

	for _, d := range []*Domain{a.accounts, a.storage, a.code, a.commitment.Domain} {
		wg.Add(1)

		mxRunningCollations.Inc()
		start := time.Now()
		collation, err := d.collateStream(ctx, step, txFrom, txTo, d.tx, logEvery)
		mxRunningCollations.Dec()
		mxCollateTook.UpdateDuration(start)

		//mxCollationSize.Set(uint64(collation.valuesComp.Count()))
		//mxCollationSizeHist.Set(uint64(collation.historyComp.Count()))

		if err != nil {
			collation.Close()
			return fmt.Errorf("domain collation %q has failed: %w", d.filenameBase, err)
		}

		go func(wg *sync.WaitGroup, d *Domain, collation Collation) {
			defer wg.Done()
			mxRunningMerges.Inc()

			start := time.Now()
			sf, err := d.buildFiles(ctx, step, collation)
			collation.Close()

			if err != nil {
				errCh <- err

				sf.Close()
				//mxRunningMerges.Dec()
				return
			}

			//mxRunningMerges.Dec()

			d.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)
			d.stats.LastFileBuildingTook = time.Since(start)
		}(&wg, d, collation)

		//mxPruningProgress.Add(2) // domain and history
		if err := d.prune(ctx, step, txFrom, txTo, (1<<64)-1, logEvery); err != nil {
			return err
		}
		//mxPruningProgress.Dec()
		//mxPruningProgress.Dec()

		mxPruneTook.Update(d.stats.LastPruneTook.Seconds())
		mxPruneHistTook.Update(d.stats.LastPruneHistTook.Seconds())
	}

	// indices are built concurrently
	for _, d := range []*InvertedIndex{a.logTopics, a.logAddrs, a.tracesFrom, a.tracesTo} {
		wg.Add(1)

		//mxRunningCollations.Inc()
		start := time.Now()
		collation, err := d.collate(ctx, step*a.aggregationStep, (step+1)*a.aggregationStep, d.tx, logEvery)
		//mxRunningCollations.Dec()
		mxCollateTook.UpdateDuration(start)

		if err != nil {
			return fmt.Errorf("index collation %q has failed: %w", d.filenameBase, err)
		}

		go func(wg *sync.WaitGroup, d *InvertedIndex, tx kv.Tx) {
			defer wg.Done()

			//mxRunningMerges.Inc()
			//start := time.Now()

			sf, err := d.buildFiles(ctx, step, collation)
			if err != nil {
				errCh <- err
				sf.Close()
				return
			}

			//mxRunningMerges.Dec()
			//mxBuildTook.UpdateDuration(start)

			d.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)

			icx := d.MakeContext()
			//mxRunningMerges.Inc()

			//if err := d.mergeRangesUpTo(ctx, d.endTxNumMinimax(), maxSpan, workers, icx); err != nil {
			//	errCh <- err
			//
			//	mxRunningMerges.Dec()
			//	icx.Close()
			//	return
			//}

			//mxRunningMerges.Dec()
			icx.Close()
		}(&wg, d, d.tx)

		//mxPruningProgress.Inc()
		//startPrune := time.Now()
		if err := d.prune(ctx, txFrom, txTo, 1<<64-1, logEvery); err != nil {
			return err
		}
		//mxPruneTook.UpdateDuration(startPrune)
		//mxPruningProgress.Dec()
	}

	// when domain files are build and db is pruned, we can merge them
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		if err := a.mergeDomainSteps(ctx); err != nil {
			errCh <- err
		}
	}(&wg)

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		log.Warn("domain collate-buildFiles failed", "err", err)
		return fmt.Errorf("domain collate-build failed: %w", err)
	}

	log.Info("[stat] aggregation is finished",
		"range", fmt.Sprintf("%.2fM-%.2fM", float64(txFrom)/10e5, float64(txTo)/10e5),
		"took", time.Since(stepStartedAt))

	//mxStepTook.UpdateDuration(stepStartedAt)

	return nil
}

func (a *AggregatorV3) mergeDomainSteps(ctx context.Context) error {
	mergeStartedAt := time.Now()
	var upmerges int
	for {
		somethingMerged, err := a.mergeLoopStep(ctx, 1)
		if err != nil {
			return err
		}

		if !somethingMerged {
			break
		}
		upmerges++
	}

	if upmerges > 1 {
		log.Info("[stat] aggregation merged", "merge_took", time.Since(mergeStartedAt), "merges_count", upmerges)
	}

	return nil
}

func (a *AggregatorV3) BuildFiles(ctx context.Context, db kv.RoDB) (err error) {
	txn := a.txNum.Load() + 1
	if txn <= a.maxTxNum.Load()+a.aggregationStep+a.keepInDB { // Leave one step worth in the DB
		return nil
	}

	_, err = a.shared.Commit(txn, true, false)
	if err != nil {
		return err
	}
	if err := a.shared.Flush(); err != nil {
		return err
	}

	// trying to create as much small-step-files as possible:
	// - to reduce amount of small merges
	// - to remove old data from db as early as possible
	// - during files build, may happen commit of new data. on each loop step getting latest id in db
	step := a.EndTxNumMinimax() / a.aggregationStep
	for ; step < lastIdInDB(db, a.accounts.indexKeysTable)/a.aggregationStep; step++ {
		if err := a.buildFilesInBackground(ctx, step); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Warn("buildFilesInBackground", "err", err)
			}
			break
		}
	}
	return nil
}

func (a *AggregatorV3) buildFilesInBackground(ctx context.Context, step uint64) (err error) {
	closeAll := true
	log.Info("[snapshots] history build", "step", fmt.Sprintf("%d-%d", step, step+1))
	sf, err := a.buildFiles(ctx, step, step*a.aggregationStep, (step+1)*a.aggregationStep)
	if err != nil {
		return err
	}
	defer func() {
		if closeAll {
			sf.Close()
		}
	}()
	a.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)
	//a.notifyAboutNewSnapshots()

	closeAll = false
	return nil
}

func (a *AggregatorV3) mergeLoopStep(ctx context.Context, workers int) (somethingDone bool, err error) {
	closeAll := true
	maxSpan := a.aggregationStep * StepsInBiggestFile
	r := a.findMergeRange(a.maxTxNum.Load(), maxSpan)
	if !r.any() {
		return false, nil
	}

	ac := a.MakeContext() // this need, to ensure we do all operations on files in "transaction-style", maybe we will ensure it on type-level in future
	defer ac.Close()

	outs, err := a.staticFilesInRange(r, ac)
	defer func() {
		if closeAll {
			outs.Close()
		}
	}()
	if err != nil {
		return false, err
	}

	in, err := a.mergeFiles(ctx, outs, r, maxSpan, workers)
	if err != nil {
		return true, err
	}
	defer func() {
		if closeAll {
			in.Close()
		}
	}()
	a.integrateMergedFiles(outs, in)
	a.onFreeze(in.FrozenList())
	closeAll = false
	return true, nil
}

func (a *AggregatorV3) MergeLoop(ctx context.Context, workers int) error {
	for {
		somethingMerged, err := a.mergeLoopStep(ctx, workers)
		if err != nil {
			return err
		}
		if !somethingMerged {
			return nil
		}
	}
}

func (a *AggregatorV3) integrateFiles(sf AggV3StaticFiles, txNumFrom, txNumTo uint64) {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()
	defer a.needSaveFilesListInDB.Store(true)
	defer a.recalcMaxTxNum()
	a.accounts.integrateFiles(sf.accounts, txNumFrom, txNumTo)
	a.storage.integrateFiles(sf.storage, txNumFrom, txNumTo)
	a.code.integrateFiles(sf.code, txNumFrom, txNumTo)
	a.commitment.integrateFiles(sf.commitment, txNumFrom, txNumTo)
	a.logAddrs.integrateFiles(sf.logAddrs, txNumFrom, txNumTo)
	a.logTopics.integrateFiles(sf.logTopics, txNumFrom, txNumTo)
	a.tracesFrom.integrateFiles(sf.tracesFrom, txNumFrom, txNumTo)
	a.tracesTo.integrateFiles(sf.tracesTo, txNumFrom, txNumTo)
}

func (a *AggregatorV3) NeedSaveFilesListInDB() bool {
	return a.needSaveFilesListInDB.CompareAndSwap(true, false)
}

func (a *AggregatorV3) Unwind(ctx context.Context, txUnwindTo uint64, stateLoad etl.LoadFunc) error {
	stateChanges := etl.NewCollector(a.logPrefix, a.tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer stateChanges.Close()
	if err := a.accounts.pruneF(txUnwindTo, math2.MaxUint64, func(_ uint64, k, v []byte) error {
		return stateChanges.Collect(k, v)
	}); err != nil {
		return err
	}
	if err := a.storage.pruneF(txUnwindTo, math2.MaxUint64, func(_ uint64, k, v []byte) error {
		return stateChanges.Collect(k, v)
	}); err != nil {
		return err
	}
	// TODO should code pruneF be here as well?
	if err := a.commitment.pruneF(txUnwindTo, math2.MaxUint64, func(_ uint64, k, v []byte) error {
		return stateChanges.Collect(k, v)
	}); err != nil {
		return err
	}

	if err := stateChanges.Load(a.rwTx, kv.PlainState, stateLoad, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	if err := a.logAddrs.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := a.logTopics.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := a.tracesFrom.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := a.tracesTo.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	return nil
}

func (a *AggregatorV3) Warmup(ctx context.Context, txFrom, limit uint64) error {
	if a.db == nil {
		return nil
	}
	e, ctx := errgroup.WithContext(ctx)
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.accounts.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.storage.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.code.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.commitment.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.logAddrs.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.logTopics.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.tracesFrom.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.tracesTo.warmup(ctx, txFrom, limit, tx) })
	})
	return e.Wait()
}

// StartWrites - pattern: `defer agg.StartWrites().FinishWrites()`
func (a *AggregatorV3) DiscardHistory() *AggregatorV3 {
	a.accounts.DiscardHistory()
	a.storage.DiscardHistory()
	a.code.DiscardHistory()
	a.commitment.DiscardHistory()
	a.logAddrs.DiscardHistory(a.tmpdir)
	a.logTopics.DiscardHistory(a.tmpdir)
	a.tracesFrom.DiscardHistory(a.tmpdir)
	a.tracesTo.DiscardHistory(a.tmpdir)
	return a
}

// StartWrites - pattern: `defer agg.StartWrites().FinishWrites()`
func (a *AggregatorV3) StartWrites() *AggregatorV3 {
	a.walLock.Lock()
	defer a.walLock.Unlock()
	a.accounts.StartWrites()
	a.storage.StartWrites()
	a.code.StartWrites()
	a.commitment.StartWrites()
	a.logAddrs.StartWrites()
	a.logTopics.StartWrites()
	a.tracesFrom.StartWrites()
	a.tracesTo.StartWrites()
	return a
}
func (a *AggregatorV3) StartUnbufferedWrites() *AggregatorV3 {
	a.walLock.Lock()
	defer a.walLock.Unlock()
	a.accounts.StartWrites()
	a.storage.StartWrites()
	a.code.StartWrites()
	a.commitment.StartWrites()
	a.logAddrs.StartWrites()
	a.logTopics.StartWrites()
	a.tracesFrom.StartWrites()
	a.tracesTo.StartWrites()
	return a
}
func (a *AggregatorV3) FinishWrites() {
	a.walLock.Lock()
	defer a.walLock.Unlock()
	a.accounts.FinishWrites()
	a.storage.FinishWrites()
	a.code.FinishWrites()
	a.commitment.FinishWrites()
	a.logAddrs.FinishWrites()
	a.logTopics.FinishWrites()
	a.tracesFrom.FinishWrites()
	a.tracesTo.FinishWrites()
}

type flusher interface {
	Flush(ctx context.Context, tx kv.RwTx) error
}

func (a *AggregatorV3) Flush(ctx context.Context, tx kv.RwTx) error {
	a.walLock.Lock()
	flushers := []flusher{
		a.accounts.Rotate(),
		a.storage.Rotate(),
		a.code.Rotate(),
		a.commitment.Domain.Rotate(),
		a.logAddrs.Rotate(),
		a.logTopics.Rotate(),
		a.tracesFrom.Rotate(),
		a.tracesTo.Rotate(),
	}
	a.walLock.Unlock()
	defer func(t time.Time) { log.Debug("[snapshots] history flush", "took", time.Since(t)) }(time.Now())
	for _, f := range flushers {
		if err := f.Flush(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func (a *AggregatorV3) BufferedDomains() *SharedDomains {
	return NewSharedDomains(path.Join(a.tmpdir, "shared"), a.accounts, a.code, a.storage, a.commitment)
}

func (a *AggregatorV3) CanPrune(tx kv.Tx) bool { return a.CanPruneFrom(tx) < a.maxTxNum.Load() }
func (a *AggregatorV3) CanPruneFrom(tx kv.Tx) uint64 {
	fst, _ := kv.FirstKey(tx, kv.TracesToKeys)
	fst2, _ := kv.FirstKey(tx, kv.StorageHistoryKeys)
	if len(fst) > 0 && len(fst2) > 0 {
		fstInDb := binary.BigEndian.Uint64(fst)
		fstInDb2 := binary.BigEndian.Uint64(fst2)
		return cmp.Min(fstInDb, fstInDb2)
	}
	return math2.MaxUint64
}

func (a *AggregatorV3) PruneWithTiemout(ctx context.Context, timeout time.Duration) error {
	t := time.Now()
	for a.CanPrune(a.rwTx) && time.Since(t) < timeout {
		if err := a.Prune(ctx, 1_000); err != nil { // prune part of retired data, before commit
			return err
		}
	}
	return nil
}

func (a *AggregatorV3) Prune(ctx context.Context, limit uint64) error {
	//if limit/a.aggregationStep > StepsInBiggestFile {
	//	ctx, cancel := context.WithCancel(ctx)
	//	defer cancel()
	//
	//	a.wg.Add(1)
	//	go func() {
	//		defer a.wg.Done()
	//		_ = a.Warmup(ctx, 0, cmp.Max(a.aggregationStep, limit)) // warmup is asyn and moving faster than data deletion
	//	}()
	//}
	return a.prune(ctx, 0, a.maxTxNum.Load(), limit)
}

func (a *AggregatorV3) prune(ctx context.Context, txFrom, txTo, limit uint64) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	step := txTo / a.aggregationStep
	if err := a.accounts.prune(ctx, step, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.storage.prune(ctx, step, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.code.prune(ctx, step, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.commitment.prune(ctx, step, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.logAddrs.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.logTopics.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.tracesFrom.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.tracesTo.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	return nil
}

func (a *AggregatorV3) LogStats(tx kv.Tx, tx2block func(endTxNumMinimax uint64) uint64) {
	if a.maxTxNum.Load() == 0 {
		return
	}
	histBlockNumProgress := tx2block(a.maxTxNum.Load())
	str := make([]string, 0, a.accounts.InvertedIndex.files.Len())
	a.accounts.InvertedIndex.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			bn := tx2block(item.endTxNum)
			str = append(str, fmt.Sprintf("%d=%dK", item.endTxNum/a.aggregationStep, bn/1_000))
		}
		return true
	})

	c, err := tx.CursorDupSort(a.accounts.InvertedIndex.indexTable)
	if err != nil {
		// TODO pass error properly around
		panic(err)
	}
	_, v, err := c.First()
	if err != nil {
		// TODO pass error properly around
		panic(err)
	}
	var firstHistoryIndexBlockInDB uint64
	if len(v) != 0 {
		firstHistoryIndexBlockInDB = tx2block(binary.BigEndian.Uint64(v))
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	log.Info("[snapshots] History Stat",
		"blocks", fmt.Sprintf("%dk", (histBlockNumProgress+1)/1000),
		"txs", fmt.Sprintf("%dm", a.maxTxNum.Load()/1_000_000),
		"txNum2blockNum", strings.Join(str, ","),
		"first_history_idx_in_db", firstHistoryIndexBlockInDB,
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
}

func (a *AggregatorV3) EndTxNumMinimax() uint64 { return a.maxTxNum.Load() }
func (a *AggregatorV3) EndTxNumFrozenAndIndexed() uint64 {
	return cmp.Min(
		cmp.Min(
			a.accounts.endIndexedTxNumMinimax(true),
			a.storage.endIndexedTxNumMinimax(true),
		),
		cmp.Min(
			a.code.endIndexedTxNumMinimax(true),
			a.commitment.endIndexedTxNumMinimax(true),
		),
	)
}
func (a *AggregatorV3) recalcMaxTxNum() {
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
	a.maxTxNum.Store(min)
}

type RangesV3 struct {
	accounts             DomainRanges
	storage              DomainRanges
	code                 DomainRanges
	commitment           DomainRanges
	logTopicsStartTxNum  uint64
	logAddrsEndTxNum     uint64
	logAddrsStartTxNum   uint64
	logTopicsEndTxNum    uint64
	tracesFromStartTxNum uint64
	tracesFromEndTxNum   uint64
	tracesToStartTxNum   uint64
	tracesToEndTxNum     uint64
	logAddrs             bool
	logTopics            bool
	tracesFrom           bool
	tracesTo             bool
}

func (r RangesV3) any() bool {
	return r.accounts.any() || r.storage.any() || r.code.any() || r.commitment.any() || r.logAddrs || r.logTopics || r.tracesFrom || r.tracesTo
}

func (a *AggregatorV3) findMergeRange(maxEndTxNum, maxSpan uint64) RangesV3 {
	var r RangesV3
	r.accounts = a.accounts.findMergeRange(maxEndTxNum, maxSpan)
	r.storage = a.storage.findMergeRange(maxEndTxNum, maxSpan)
	r.code = a.code.findMergeRange(maxEndTxNum, maxSpan)
	r.commitment = a.commitment.findMergeRange(maxEndTxNum, maxSpan)
	r.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum = a.logAddrs.findMergeRange(maxEndTxNum, maxSpan)
	r.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum = a.logTopics.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum = a.tracesFrom.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum = a.tracesTo.findMergeRange(maxEndTxNum, maxSpan)
	//log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%+v\n", maxEndTxNum, maxSpan, r))
	return r
}

type SelectedStaticFilesV3 struct {
	accounts       []*filesItem
	accountsIdx    []*filesItem
	accountsHist   []*filesItem
	storage        []*filesItem
	storageIdx     []*filesItem
	storageHist    []*filesItem
	code           []*filesItem
	codeIdx        []*filesItem
	codeHist       []*filesItem
	commitment     []*filesItem
	commitmentIdx  []*filesItem
	commitmentHist []*filesItem
	logTopics      []*filesItem
	tracesTo       []*filesItem
	tracesFrom     []*filesItem
	logAddrs       []*filesItem
	accountsI      int
	storageI       int
	codeI          int
	commitmentI    int
	logAddrsI      int
	logTopicsI     int
	tracesFromI    int
	tracesToI      int
}

func (sf SelectedStaticFilesV3) Close() {
	clist := [...][]*filesItem{
		sf.accounts, sf.accountsIdx, sf.accountsHist,
		sf.storage, sf.storageIdx, sf.accountsHist,
		sf.code, sf.codeIdx, sf.codeHist,
		sf.commitment, sf.commitmentIdx, sf.commitmentHist,
		sf.logAddrs, sf.logTopics, sf.tracesFrom, sf.tracesTo,
	}
	for _, group := range clist {
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

func (a *AggregatorV3) staticFilesInRange(r RangesV3, ac *AggregatorV3Context) (sf SelectedStaticFilesV3, err error) {
	_ = ac // maybe will move this method to `ac` object
	if r.accounts.any() {
		sf.accounts, sf.accountsIdx, sf.accountsHist, sf.accountsI = a.accounts.staticFilesInRange(r.accounts, ac.accounts)
	}
	if r.storage.any() {
		sf.storage, sf.storageIdx, sf.storageHist, sf.storageI = a.storage.staticFilesInRange(r.storage, ac.storage)
	}
	if r.code.any() {
		sf.code, sf.codeIdx, sf.codeHist, sf.codeI = a.code.staticFilesInRange(r.code, ac.code)
	}
	if r.commitment.any() {
		sf.commitment, sf.commitmentIdx, sf.commitmentHist, sf.commitmentI = a.commitment.staticFilesInRange(r.commitment, ac.commitment)
	}
	if r.logAddrs {
		sf.logAddrs, sf.logAddrsI = a.logAddrs.staticFilesInRange(r.logAddrsStartTxNum, r.logAddrsEndTxNum, ac.logAddrs)
	}
	if r.logTopics {
		sf.logTopics, sf.logTopicsI = a.logTopics.staticFilesInRange(r.logTopicsStartTxNum, r.logTopicsEndTxNum, ac.logTopics)
	}
	if r.tracesFrom {
		sf.tracesFrom, sf.tracesFromI = a.tracesFrom.staticFilesInRange(r.tracesFromStartTxNum, r.tracesFromEndTxNum, ac.tracesFrom)
	}
	if r.tracesTo {
		sf.tracesTo, sf.tracesToI = a.tracesTo.staticFilesInRange(r.tracesToStartTxNum, r.tracesToEndTxNum, ac.tracesTo)
	}
	return sf, err
}

type MergedFilesV3 struct {
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

func (mf MergedFilesV3) FrozenList() (frozen []string) {
	if mf.accountsHist != nil && mf.accountsHist.frozen {
		frozen = append(frozen, mf.accountsHist.decompressor.FileName())
	}
	if mf.accountsIdx != nil && mf.accountsIdx.frozen {
		frozen = append(frozen, mf.accountsIdx.decompressor.FileName())
	}

	if mf.storageHist != nil && mf.storageHist.frozen {
		frozen = append(frozen, mf.storageHist.decompressor.FileName())
	}
	if mf.storageIdx != nil && mf.storageIdx.frozen {
		frozen = append(frozen, mf.storageIdx.decompressor.FileName())
	}

	if mf.codeHist != nil && mf.codeHist.frozen {
		frozen = append(frozen, mf.codeHist.decompressor.FileName())
	}
	if mf.codeIdx != nil && mf.codeIdx.frozen {
		frozen = append(frozen, mf.codeIdx.decompressor.FileName())
	}

	if mf.logAddrs != nil && mf.logAddrs.frozen {
		frozen = append(frozen, mf.logAddrs.decompressor.FileName())
	}
	if mf.logTopics != nil && mf.logTopics.frozen {
		frozen = append(frozen, mf.logTopics.decompressor.FileName())
	}
	if mf.tracesFrom != nil && mf.tracesFrom.frozen {
		frozen = append(frozen, mf.tracesFrom.decompressor.FileName())
	}
	if mf.tracesTo != nil && mf.tracesTo.frozen {
		frozen = append(frozen, mf.tracesTo.decompressor.FileName())
	}
	return frozen
}
func (mf MergedFilesV3) Close() {
	clist := [...]*filesItem{
		mf.accounts, mf.accountsIdx, mf.accountsHist,
		mf.storage, mf.storageIdx, mf.storageHist,
		mf.code, mf.codeIdx, mf.codeHist,
		mf.commitment, mf.commitmentIdx, mf.commitmentHist,
		mf.logAddrs, mf.logTopics, mf.tracesFrom, mf.tracesTo,
	}

	for _, item := range clist {
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

func (a *AggregatorV3) mergeFiles(ctx context.Context, files SelectedStaticFilesV3, r RangesV3, maxSpan uint64, workers int) (MergedFilesV3, error) {
	var mf MergedFilesV3
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}
	}()

	var predicates *sync.WaitGroup
	if r.accounts.any() {
		predicates.Add(1)

		log.Info(fmt.Sprintf("[snapshots] merge: %d-%d", r.accounts.historyStartTxNum/a.aggregationStep, r.accounts.historyEndTxNum/a.aggregationStep))
		g.Go(func() (err error) {
			mf.accounts, mf.accountsIdx, mf.accountsHist, err = a.accounts.mergeFiles(ctx, files.accounts, files.accountsIdx, files.accountsHist, r.accounts, workers)
			predicates.Done()
			return err
		})
	}

	if r.storage.any() {
		predicates.Add(1)
		g.Go(func() (err error) {
			mf.storage, mf.storageIdx, mf.storageHist, err = a.storage.mergeFiles(ctx, files.storage, files.storageIdx, files.storageHist, r.storage, workers)
			predicates.Done()
			return err
		})
	}
	if r.code.any() {
		g.Go(func() (err error) {
			mf.code, mf.codeIdx, mf.codeHist, err = a.code.mergeFiles(ctx, files.code, files.codeIdx, files.codeHist, r.code, workers)
			return err
		})
	}
	if r.commitment.any() {
		predicates.Wait()
		g.Go(func() (err error) {
			var v4Files SelectedStaticFiles
			var v4MergedF MergedFiles

			mf.commitment, mf.commitmentIdx, mf.commitmentHist, err = a.commitment.mergeFiles(ctx, v4Files.FillV3(&files), v4MergedF.FillV3(&mf), r.commitment, workers)
			return err
		})
	}

	if r.logAddrs {
		g.Go(func() error {
			var err error
			mf.logAddrs, err = a.logAddrs.mergeFiles(ctx, files.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum, workers)
			return err
		})
	}
	if r.logTopics {
		g.Go(func() error {
			var err error
			mf.logTopics, err = a.logTopics.mergeFiles(ctx, files.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum, workers)
			return err
		})
	}
	if r.tracesFrom {
		g.Go(func() error {
			var err error
			mf.tracesFrom, err = a.tracesFrom.mergeFiles(ctx, files.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum, workers)
			return err
		})
	}
	if r.tracesTo {
		g.Go(func() error {
			var err error
			mf.tracesTo, err = a.tracesTo.mergeFiles(ctx, files.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum, workers)
			return err
		})
	}
	err := g.Wait()
	if err == nil {
		closeFiles = false
	}
	return mf, err
}

func (a *AggregatorV3) integrateMergedFiles(outs SelectedStaticFilesV3, in MergedFilesV3) (frozen []string) {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()
	defer a.needSaveFilesListInDB.Store(true)
	defer a.recalcMaxTxNum()
	a.accounts.integrateMergedFiles(outs.accounts, outs.accountsIdx, outs.accountsHist, in.accounts, in.accountsIdx, in.accountsHist)
	a.storage.integrateMergedFiles(outs.storage, outs.storageIdx, outs.storageHist, in.storage, in.storageIdx, in.storageHist)
	a.code.integrateMergedFiles(outs.code, outs.codeIdx, outs.codeHist, in.code, in.codeIdx, in.codeHist)
	a.commitment.integrateMergedFiles(outs.commitment, outs.commitmentIdx, outs.commitmentHist, in.commitment, in.commitmentIdx, in.commitmentHist)
	a.logAddrs.integrateMergedFiles(outs.logAddrs, in.logAddrs)
	a.logTopics.integrateMergedFiles(outs.logTopics, in.logTopics)
	a.tracesFrom.integrateMergedFiles(outs.tracesFrom, in.tracesFrom)
	a.tracesTo.integrateMergedFiles(outs.tracesTo, in.tracesTo)
	a.cleanFrozenParts(in)
	return frozen
}
func (a *AggregatorV3) cleanFrozenParts(in MergedFilesV3) {
	a.accounts.cleanFrozenParts(in.accountsHist)
	a.storage.cleanFrozenParts(in.storageHist)
	a.code.cleanFrozenParts(in.codeHist)
	a.commitment.cleanFrozenParts(in.commitmentHist)
	a.logAddrs.cleanFrozenParts(in.logAddrs)
	a.logTopics.cleanFrozenParts(in.logTopics)
	a.tracesFrom.cleanFrozenParts(in.tracesFrom)
	a.tracesTo.cleanFrozenParts(in.tracesTo)
}

// KeepInDB - usually equal to one a.aggregationStep, but when we exec blocks from snapshots
// we can set it to 0, because no re-org on this blocks are possible
func (a *AggregatorV3) KeepInDB(v uint64) { a.keepInDB = v }

func (a *AggregatorV3) BuildFilesInBackground() {
	if (a.txNum.Load() + 1) <= a.maxTxNum.Load()+a.aggregationStep+a.keepInDB { // Leave one step worth in the DB
		return
	}

	step := a.maxTxNum.Load() / a.aggregationStep
	if ok := a.working.CompareAndSwap(false, true); !ok {
		return
	}

	toTxNum := (step + 1) * a.aggregationStep
	hasData := false
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer a.working.Store(false)

		// check if db has enough data (maybe we didn't commit them yet)
		lastInDB := lastIdInDB(a.db, a.accounts.indexKeysTable)
		hasData = lastInDB >= toTxNum
		if !hasData {
			return
		}

		// trying to create as much small-step-files as possible:
		// - to reduce amount of small merges
		// - to remove old data from db as early as possible
		// - during files build, may happen commit of new data. on each loop step getting latest id in db
		for step < lastIdInDB(a.db, a.accounts.indexKeysTable)/a.aggregationStep {
			if err := a.buildFilesInBackground(a.ctx, step); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Warn("buildFilesInBackground", "err", err)
				break
			}
			step++
		}

		if ok := a.workingMerge.CompareAndSwap(false, true); !ok {
			return
		}
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			defer a.workingMerge.Store(false)
			if err := a.MergeLoop(a.ctx, 1); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Warn("merge", "err", err)
			}

			a.BuildOptionalMissedIndicesInBackground(a.ctx, 1)
		}()
	}()
}

func (a *AggregatorV3) BatchHistoryWriteStart() *AggregatorV3 {
	a.walLock.RLock()
	return a
}
func (a *AggregatorV3) BatchHistoryWriteEnd() {
	a.walLock.RUnlock()
}

func (a *AggregatorV3) AddAccountPrev(addr []byte, prev []byte) error {
	return a.accounts.AddPrevValue(addr, nil, prev)
}

func (a *AggregatorV3) AddStoragePrev(addr []byte, loc []byte, prev []byte) error {
	return a.storage.AddPrevValue(addr, loc, prev)
}

// AddCodePrev - addr+inc => code
func (a *AggregatorV3) AddCodePrev(addr []byte, prev []byte) error {
	return a.code.AddPrevValue(addr, nil, prev)
}

func (a *AggregatorV3) AddTraceFrom(addr []byte) error {
	return a.tracesFrom.Add(addr)
}

func (a *AggregatorV3) AddTraceTo(addr []byte) error {
	return a.tracesTo.Add(addr)
}

func (a *AggregatorV3) AddLogAddr(addr []byte) error {
	return a.logAddrs.Add(addr)
}

func (a *AggregatorV3) AddLogTopic(topic []byte) error {
	return a.logTopics.Add(topic)
}

func (a *AggregatorV3) UpdateAccount(addr []byte, data, prevData []byte) error {
	a.commitment.TouchPlainKey(addr, data, a.commitment.TouchPlainKeyAccount)
	return a.accounts.PutWitPrev(addr, nil, data, prevData)
}

func (a *AggregatorV3) UpdateCode(addr []byte, code, prevCode []byte) error {
	a.commitment.TouchPlainKey(addr, code, a.commitment.TouchPlainKeyCode)
	if len(code) == 0 {
		return a.code.DeleteWithPrev(addr, nil, prevCode)
	}
	return a.code.PutWitPrev(addr, nil, code, prevCode)
}

func (a *AggregatorV3) DeleteAccount(addr, prev []byte) error {
	a.commitment.TouchPlainKey(addr, nil, a.commitment.TouchPlainKeyAccount)

	if err := a.accounts.DeleteWithPrev(addr, nil, prev); err != nil {
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

func (a *AggregatorV3) UpdateStorage(addr, loc []byte, value, preVal []byte) error {
	a.commitment.TouchPlainKey(common2.Append(addr, loc), value, a.commitment.TouchPlainKeyStorage)
	if len(value) == 0 {
		return a.storage.Delete(addr, loc)
	}
	return a.storage.PutWitPrev(addr, loc, value, preVal)
}

// ComputeCommitment evaluates commitment for processed state.
// If `saveStateAfter`=true, then trie state will be saved to DB after commitment evaluation.
func (a *AggregatorV3) ComputeCommitment(saveStateAfter, trace bool) (rootHash []byte, err error) {
	// if commitment mode is Disabled, there will be nothing to compute on.
	mxCommitmentRunning.Inc()
	rootHash, branchNodeUpdates, err := a.commitment.ComputeCommitment(trace)
	mxCommitmentRunning.Dec()

	if err != nil {
		return nil, err
	}
	//if a.seekTxNum > a.txNum {
	//	saveStateAfter = false
	//}

	mxCommitmentKeys.Add(int(a.commitment.comKeys))
	mxCommitmentTook.Update(a.commitment.comTook.Seconds())

	defer func(t time.Time) { mxCommitmentWriteTook.UpdateDuration(t) }(time.Now())

	sortedPrefixes := make([]string, len(branchNodeUpdates))
	for pref := range branchNodeUpdates {
		sortedPrefixes = append(sortedPrefixes, pref)
	}
	sort.Strings(sortedPrefixes)

	cct := a.commitment.MakeContext()
	defer cct.Close()

	for _, pref := range sortedPrefixes {
		prefix := []byte(pref)
		update := branchNodeUpdates[pref]

		stateValue, err := cct.Get(prefix, nil, a.rwTx)
		if err != nil {
			return nil, err
		}
		mxCommitmentUpdates.Inc()
		stated := commitment.BranchData(stateValue)
		merged, err := a.commitment.branchMerger.Merge(stated, update)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(stated, merged) {
			continue
		}
		if trace {
			fmt.Printf("computeCommitment merge [%x] [%x]+[%x]=>[%x]\n", prefix, stated, update, merged)
		}
		if err = a.commitment.Put(prefix, nil, merged); err != nil {
			return nil, err
		}
		mxCommitmentUpdatesApplied.Inc()
	}

	if saveStateAfter {
		if err := a.commitment.storeCommitmentState(a.blockNum.Load(), a.txNum.Load()); err != nil {
			return nil, err
		}
	}

	return rootHash, nil
}

// DisableReadAhead - usage: `defer d.EnableReadAhead().DisableReadAhead()`. Please don't use this funcs without `defer` to avoid leak.
func (a *AggregatorV3) DisableReadAhead() {
	a.accounts.DisableReadAhead()
	a.storage.DisableReadAhead()
	a.code.DisableReadAhead()
	a.commitment.DisableReadAhead()
	a.logAddrs.DisableReadAhead()
	a.logTopics.DisableReadAhead()
	a.tracesFrom.DisableReadAhead()
	a.tracesTo.DisableReadAhead()
}
func (a *AggregatorV3) EnableReadAhead() *AggregatorV3 {
	a.accounts.EnableReadAhead()
	a.storage.EnableReadAhead()
	a.code.EnableReadAhead()
	a.commitment.EnableReadAhead()
	a.logAddrs.EnableReadAhead()
	a.logTopics.EnableReadAhead()
	a.tracesFrom.EnableReadAhead()
	a.tracesTo.EnableReadAhead()
	return a
}
func (a *AggregatorV3) EnableMadvWillNeed() *AggregatorV3 {
	a.accounts.EnableMadvWillNeed()
	a.storage.EnableMadvWillNeed()
	a.code.EnableMadvWillNeed()
	a.commitment.EnableMadvWillNeed()
	a.logAddrs.EnableMadvWillNeed()
	a.logTopics.EnableMadvWillNeed()
	a.tracesFrom.EnableMadvWillNeed()
	a.tracesTo.EnableMadvWillNeed()
	return a
}
func (a *AggregatorV3) EnableMadvNormal() *AggregatorV3 {
	a.accounts.EnableMadvNormalReadAhead()
	a.storage.EnableMadvNormalReadAhead()
	a.code.EnableMadvNormalReadAhead()
	a.commitment.EnableMadvNormalReadAhead()
	a.logAddrs.EnableMadvNormalReadAhead()
	a.logTopics.EnableMadvNormalReadAhead()
	a.tracesFrom.EnableMadvNormalReadAhead()
	a.tracesTo.EnableMadvNormalReadAhead()
	return a
}

// -- range
func (ac *AggregatorV3Context) LogAddrRange(addr []byte, startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.U64, error) {
	return ac.logAddrs.IdxRange(addr, startTxNum, endTxNum, asc, limit, tx)
}

func (ac *AggregatorV3Context) LogTopicRange(topic []byte, startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.U64, error) {
	return ac.logTopics.IdxRange(topic, startTxNum, endTxNum, asc, limit, tx)
}

func (ac *AggregatorV3Context) TraceFromRange(addr []byte, startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.U64, error) {
	return ac.tracesFrom.IdxRange(addr, startTxNum, endTxNum, asc, limit, tx)
}

func (ac *AggregatorV3Context) TraceToRange(addr []byte, startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.U64, error) {
	return ac.tracesTo.IdxRange(addr, startTxNum, endTxNum, asc, limit, tx)
}
func (ac *AggregatorV3Context) AccountHistoryIdxRange(addr []byte, startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.U64, error) {
	return ac.accounts.hc.IdxRange(addr, startTxNum, endTxNum, asc, limit, tx)
}
func (ac *AggregatorV3Context) StorageHistoryIdxRange(addr []byte, startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.U64, error) {
	return ac.storage.hc.IdxRange(addr, startTxNum, endTxNum, asc, limit, tx)
}
func (ac *AggregatorV3Context) CodeHistoryIdxRange(addr []byte, startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.U64, error) {
	return ac.code.hc.IdxRange(addr, startTxNum, endTxNum, asc, limit, tx)
}

// -- range end

func (ac *AggregatorV3Context) ReadAccountDataNoStateWithRecent(addr []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	return ac.accounts.hc.GetNoStateWithRecent(addr, txNum, tx)
}

func (ac *AggregatorV3Context) ReadAccountDataNoState(addr []byte, txNum uint64) ([]byte, bool, error) {
	return ac.accounts.hc.GetNoState(addr, txNum)
}

func (ac *AggregatorV3Context) ReadAccountStorageNoStateWithRecent(addr []byte, loc []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	if cap(ac.keyBuf) < len(addr)+len(loc) {
		ac.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ac.keyBuf) != len(addr)+len(loc) {
		ac.keyBuf = ac.keyBuf[:len(addr)+len(loc)]
	}
	copy(ac.keyBuf, addr)
	copy(ac.keyBuf[len(addr):], loc)
	return ac.storage.hc.GetNoStateWithRecent(ac.keyBuf, txNum, tx)
}
func (ac *AggregatorV3Context) ReadAccountStorageNoStateWithRecent2(key []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	return ac.storage.hc.GetNoStateWithRecent(key, txNum, tx)
}

func (ac *AggregatorV3Context) ReadAccountStorageNoState(addr []byte, loc []byte, txNum uint64) ([]byte, bool, error) {
	if cap(ac.keyBuf) < len(addr)+len(loc) {
		ac.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ac.keyBuf) != len(addr)+len(loc) {
		ac.keyBuf = ac.keyBuf[:len(addr)+len(loc)]
	}
	copy(ac.keyBuf, addr)
	copy(ac.keyBuf[len(addr):], loc)
	return ac.storage.hc.GetNoState(ac.keyBuf, txNum)
}

func (ac *AggregatorV3Context) ReadAccountCodeNoStateWithRecent(addr []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	return ac.code.hc.GetNoStateWithRecent(addr, txNum, tx)
}
func (ac *AggregatorV3Context) ReadAccountCodeNoState(addr []byte, txNum uint64) ([]byte, bool, error) {
	return ac.code.hc.GetNoState(addr, txNum)
}

func (ac *AggregatorV3Context) ReadAccountCodeSizeNoStateWithRecent(addr []byte, txNum uint64, tx kv.Tx) (int, bool, error) {
	code, noState, err := ac.code.hc.GetNoStateWithRecent(addr, txNum, tx)
	if err != nil {
		return 0, false, err
	}
	return len(code), noState, nil
}
func (ac *AggregatorV3Context) ReadAccountCodeSizeNoState(addr []byte, txNum uint64) (int, bool, error) {
	code, noState, err := ac.code.hc.GetNoState(addr, txNum)
	if err != nil {
		return 0, false, err
	}
	return len(code), noState, nil
}

func (ac *AggregatorV3Context) AccountHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	return ac.accounts.hc.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
}

func (ac *AggregatorV3Context) StorageHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	return ac.storage.hc.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
}

func (ac *AggregatorV3Context) CodeHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	return ac.code.hc.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
}

func (ac *AggregatorV3Context) AccountHistoricalStateRange(startTxNum uint64, from, to []byte, limit int, tx kv.Tx) iter.KV {
	return ac.accounts.hc.WalkAsOf(startTxNum, from, to, tx, limit)
}

func (ac *AggregatorV3Context) StorageHistoricalStateRange(startTxNum uint64, from, to []byte, limit int, tx kv.Tx) iter.KV {
	return ac.storage.hc.WalkAsOf(startTxNum, from, to, tx, limit)
}

func (ac *AggregatorV3Context) CodeHistoricalStateRange(startTxNum uint64, from, to []byte, limit int, tx kv.Tx) iter.KV {
	return ac.code.hc.WalkAsOf(startTxNum, from, to, tx, limit)
}

type FilesStats22 struct {
}

func (a *AggregatorV3) Stats() FilesStats22 {
	var fs FilesStats22
	return fs
}

func (a *AggregatorV3) Code() *History       { return a.code.History }
func (a *AggregatorV3) Accounts() *History   { return a.accounts.History }
func (a *AggregatorV3) Storage() *History    { return a.storage.History }
func (a *AggregatorV3) Commitment() *History { return a.commitment.History }

type AggregatorV3Context struct {
	a          *AggregatorV3
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

func (a *AggregatorV3) MakeContext() *AggregatorV3Context {
	return &AggregatorV3Context{
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
func (ac *AggregatorV3Context) Close() {
	ac.accounts.Close()
	ac.storage.Close()
	ac.code.Close()
	ac.commitment.Close()
	ac.logAddrs.Close()
	ac.logTopics.Close()
	ac.tracesFrom.Close()
	ac.tracesTo.Close()
}

// BackgroundResult - used only indicate that some work is done
// no much reason to pass exact results by this object, just get latest state when need
type BackgroundResult struct {
	err error
	has bool
}

func (br *BackgroundResult) Has() bool     { return br.has }
func (br *BackgroundResult) Set(err error) { br.has, br.err = true, err }
func (br *BackgroundResult) GetAndReset() (bool, error) {
	has, err := br.has, br.err
	br.has, br.err = false, nil
	return has, err
}

func lastIdInDB(db kv.RoDB, table string) (lstInDb uint64) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		lst, _ := kv.LastKey(tx, table)
		if len(lst) > 0 {
			lstInDb = binary.BigEndian.Uint64(lst)
		}
		return nil
	}); err != nil {
		log.Warn("lastIdInDB", "err", err)
	}
	return lstInDb
}

// AggregatorStep is used for incremental reconstitution, it allows
// accessing history in isolated way for each step
type AggregatorStep struct {
	a          *AggregatorV3
	accounts   *HistoryStep
	storage    *HistoryStep
	code       *HistoryStep
	commitment *HistoryStep
	keyBuf     []byte
}

func (a *AggregatorV3) MakeSteps() ([]*AggregatorStep, error) {
	frozenAndIndexed := a.EndTxNumFrozenAndIndexed()
	accountSteps := a.accounts.MakeSteps(frozenAndIndexed)
	codeSteps := a.code.MakeSteps(frozenAndIndexed)
	storageSteps := a.storage.MakeSteps(frozenAndIndexed)
	commitmentSteps := a.commitment.MakeSteps(frozenAndIndexed)
	if len(accountSteps) != len(storageSteps) || len(storageSteps) != len(codeSteps) {
		return nil, fmt.Errorf("different limit of steps (try merge snapshots): accountSteps=%d, storageSteps=%d, codeSteps=%d", len(accountSteps), len(storageSteps), len(codeSteps))
	}
	steps := make([]*AggregatorStep, len(accountSteps))
	for i, accountStep := range accountSteps {
		steps[i] = &AggregatorStep{
			a:          a,
			accounts:   accountStep,
			storage:    storageSteps[i],
			code:       codeSteps[i],
			commitment: commitmentSteps[i],
		}
	}
	return steps, nil
}

func (as *AggregatorStep) TxNumRange() (uint64, uint64) {
	return as.accounts.indexFile.startTxNum, as.accounts.indexFile.endTxNum
}

func (as *AggregatorStep) IterateAccountsTxs() *ScanIteratorInc {
	return as.accounts.iterateTxs()
}

func (as *AggregatorStep) IterateStorageTxs() *ScanIteratorInc {
	return as.storage.iterateTxs()
}

func (as *AggregatorStep) IterateCodeTxs() *ScanIteratorInc {
	return as.code.iterateTxs()
}

func (as *AggregatorStep) ReadAccountDataNoState(addr []byte, txNum uint64) ([]byte, bool, uint64) {
	return as.accounts.GetNoState(addr, txNum)
}

// --- Domain part START ---
func (ac *AggregatorV3Context) AccountLatest(addr []byte, roTx kv.Tx) ([]byte, bool, error) {
	return ac.accounts.GetLatest(addr, nil, roTx)
}
func (ac *AggregatorV3Context) StorageLatest(addr []byte, loc []byte, roTx kv.Tx) ([]byte, bool, error) {
	return ac.storage.GetLatest(addr, loc, roTx)
}
func (ac *AggregatorV3Context) CodeLatest(addr []byte, roTx kv.Tx) ([]byte, bool, error) {
	return ac.code.GetLatest(addr, nil, roTx)
}
func (ac *AggregatorV3Context) IterAcc(prefix []byte, it func(k, v []byte), tx kv.RwTx) error {
	ac.a.SetTx(tx)
	return ac.accounts.IteratePrefix(prefix, it)
}

// --- Domain part END ---

func (as *AggregatorStep) ReadAccountStorageNoState(addr []byte, loc []byte, txNum uint64) ([]byte, bool, uint64) {
	if cap(as.keyBuf) < len(addr)+len(loc) {
		as.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(as.keyBuf) != len(addr)+len(loc) {
		as.keyBuf = as.keyBuf[:len(addr)+len(loc)]
	}
	copy(as.keyBuf, addr)
	copy(as.keyBuf[len(addr):], loc)
	return as.storage.GetNoState(as.keyBuf, txNum)
}

func (as *AggregatorStep) ReadAccountCodeNoState(addr []byte, txNum uint64) ([]byte, bool, uint64) {
	return as.code.GetNoState(addr, txNum)
}

func (as *AggregatorStep) ReadAccountCodeSizeNoState(addr []byte, txNum uint64) (int, bool, uint64) {
	code, noState, stateTxNum := as.code.GetNoState(addr, txNum)
	return len(code), noState, stateTxNum
}

func (as *AggregatorStep) MaxTxNumAccounts(addr []byte) (bool, uint64) {
	return as.accounts.MaxTxNum(addr)
}

func (as *AggregatorStep) MaxTxNumStorage(addr []byte, loc []byte) (bool, uint64) {
	if cap(as.keyBuf) < len(addr)+len(loc) {
		as.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(as.keyBuf) != len(addr)+len(loc) {
		as.keyBuf = as.keyBuf[:len(addr)+len(loc)]
	}
	copy(as.keyBuf, addr)
	copy(as.keyBuf[len(addr):], loc)
	return as.storage.MaxTxNum(as.keyBuf)
}

func (as *AggregatorStep) MaxTxNumCode(addr []byte) (bool, uint64) {
	return as.code.MaxTxNum(addr)
}

func (as *AggregatorStep) IterateAccountsHistory(txNum uint64) *HistoryIteratorInc {
	return as.accounts.interateHistoryBeforeTxNum(txNum)
}

func (as *AggregatorStep) IterateStorageHistory(txNum uint64) *HistoryIteratorInc {
	return as.storage.interateHistoryBeforeTxNum(txNum)
}

func (as *AggregatorStep) IterateCodeHistory(txNum uint64) *HistoryIteratorInc {
	return as.code.interateHistoryBeforeTxNum(txNum)
}

func (as *AggregatorStep) Clone() *AggregatorStep {
	return &AggregatorStep{
		a:        as.a,
		accounts: as.accounts.Clone(),
		storage:  as.storage.Clone(),
		code:     as.code.Clone(),
	}
}
