package state

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
)

type stateAggregate interface {
	collate(ctx context.Context, step, txFrom, txTo uint64, roTx kv.Tx, logEvery *time.Ticker) (Collation, error)
	buildFiles(ctx context.Context, step uint64, collation Collation) (StaticFiles, error)
	integrateFiles(sf StaticFiles, txNumFrom, txNumTo uint64)
	prune(ctx context.Context, step uint64, txFrom, txTo, limit uint64, logEvery *time.Ticker) error

	findMergeRange(maxEndTxNum, maxSpan uint64) DomainRanges
	staticFilesInRange(r DomainRanges) (valuesFiles, indexFiles, historyFiles []*filesItem, startJ int)
	mergeFiles(ctx context.Context, valuesFiles, indexFiles, historyFiles []*filesItem, r DomainRanges, workers int) (valuesIn, indexIn, historyIn *filesItem, err error)
	integrateMergedFiles(valuesOuts, indexOuts, historyOuts []*filesItem, valuesIn, indexIn, historyIn *filesItem)
	deleteFiles(valuesOuts, indexOuts, historyOuts []*filesItem) error
}

type mergedDomainFiles struct {
	values  *filesItem
	index   *filesItem
	history *filesItem
}

func (m *mergedDomainFiles) Close() {
	for _, item := range []*filesItem{
		m.values, m.index, m.history,
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

type staticFilesInRange struct {
	valuesFiles  []*filesItem
	indexFiles   []*filesItem
	historyFiles []*filesItem
	startJ       int
}

func (s *staticFilesInRange) Close() {
	for _, group := range [][]*filesItem{
		s.valuesFiles, s.indexFiles, s.historyFiles,
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

func (a *Aggregator) collateSingle(ctx context.Context, d stateAggregate, step uint64, workers int, logEvery *time.Ticker) error {
	collation, err := d.collate(ctx, step, step*a.aggregationStep, (step+1)*a.aggregationStep, a.rwTx, logEvery)
	if err != nil {
		return err
	}

	var closeAll bool = true
	defer func() {
		if closeAll {
			collation.Close()
		}
	}()

	sf, err := d.buildFiles(ctx, step, collation)
	if err != nil {
		return err
	}

	defer func() {
		if closeAll {
			sf.Close()
		}
	}()
	d.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)

	if err := d.prune(ctx, step, step*a.aggregationStep, (step+1)*a.aggregationStep, math.MaxUint64, logEvery); err != nil {
		return err
	}

	maxEndTxNum := a.EndTxNumMinimax()
	maxSpan := uint64(32) * a.aggregationStep

	for rng := d.findMergeRange(maxEndTxNum, maxSpan); rng.any(); rng = d.findMergeRange(maxEndTxNum, maxSpan) {
		var sfr staticFilesInRange
		sfr.valuesFiles, sfr.indexFiles, sfr.historyFiles, sfr.startJ = d.staticFilesInRange(rng)
		defer func() {
			if closeAll {
				sfr.Close()
			}
		}()

		var mf mergedDomainFiles
		if mf.values, mf.index, mf.history, err = d.mergeFiles(ctx, sfr.valuesFiles, sfr.indexFiles, sfr.historyFiles, rng, workers); err != nil {
			return err
		}
		defer func() {
			if closeAll {
				mf.Close()
			}
		}()

		//defer func(t time.Time) { log.Info("[snapshots] merge", "took", time.Since(t)) }(time.Now())
		d.integrateMergedFiles(sfr.valuesFiles, sfr.indexFiles, sfr.historyFiles, mf.values, mf.index, mf.history)

		if err := d.deleteFiles(sfr.valuesFiles, sfr.indexFiles, sfr.historyFiles); err != nil {
			return err
		}

		log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%+v\n", maxEndTxNum, maxSpan, rng))
	}

	closeAll = false
	return nil
}

func (a *Aggregator) collator(ctx context.Context, step uint64) error {
	logEvery := time.NewTicker(time.Second * 30)

	errCh := make(chan error, 8)
	var wg sync.WaitGroup

	for _, d := range []*InvertedIndex{a.logTopics, a.logAddrs, a.tracesFrom, a.tracesTo} {
		collation, err := d.collate(ctx, step*a.aggregationStep, (step+1)*a.aggregationStep, a.rwTx, logEvery)
		if err != nil {
			return err
		}

		wg.Add(1)
		go func(wg *sync.WaitGroup, d *InvertedIndex, collation map[string]*roaring64.Bitmap) {
			defer wg.Done()

			sf, err := d.buildFiles(ctx, step, collation)
			if err != nil {
				errCh <- err
				sf.Close()
				return
			}
			d.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)
			sf.Close()
		}(&wg, d, collation)
	}

	for _, d := range []*Domain{a.accounts, a.storage, a.code, a.commitment.Domain} {
		collation, err := d.collate(ctx, step, step*a.aggregationStep, (step+1)*a.aggregationStep, a.rwTx, logEvery)
		if err != nil {
			return err
		}

		wg.Add(1)
		go func(wg *sync.WaitGroup, d *Domain, collation Collation) {
			defer wg.Done()

			sf, err := d.buildFiles(ctx, step, collation)
			collation.Close()
			if err != nil {
				errCh <- err
				sf.Close()
				return
			}

			d.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)
			sf.Close()
		}(&wg, d, collation)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		log.Warn("build domain files failed", "err", err)
	}

	for _, d := range []*InvertedIndex{a.logTopics, a.logAddrs, a.tracesFrom, a.tracesTo} {
		err := d.prune(ctx, step*a.aggregationStep, (step+1)*a.aggregationStep, math.MaxUint64, logEvery)
		if err != nil {
			return err
		}
	}

	for _, d := range []*Domain{a.accounts, a.storage, a.code, a.commitment.Domain} {
		err := d.prune(ctx, step, step*a.aggregationStep, (step+1)*a.aggregationStep, math.MaxUint64, logEvery)
		if err != nil {
			return err
		}
	}

	maxEndTxNum := a.EndTxNumMinimax()
	maxSpan := uint64(32) * a.aggregationStep
	for r := a.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = a.findMergeRange(maxEndTxNum, maxSpan) {
		outs := a.staticFilesInRange(r)
		in, err := a.mergeFiles(ctx, outs, r, 1)
		if err != nil {
			outs.Close()
			return err
		}
		a.integrateMergedFiles(outs, in)
		in.Close()

		if err = a.deleteFiles(outs); err != nil {
			outs.Close()
			return err
		}
		outs.Close()
	}
	return nil
}

type mergeArtifacts struct {
	sfr staticFilesInRange
	mf  mergedDomainFiles
}

func (d *Domain) mergeRangesUpTo(ctx context.Context, maxTxNum, maxSpan uint64, workers int, output chan mergedDomainFiles) (err error) {
	//closeAll := true
	for rng := d.findMergeRange(maxSpan, maxTxNum); rng.any(); rng = d.findMergeRange(maxTxNum, maxSpan) {
		var sfr staticFilesInRange
		sfr.valuesFiles, sfr.indexFiles, sfr.historyFiles, sfr.startJ = d.staticFilesInRange(rng)
		//defer func() {
		//	if closeAll {
		//		sfr.Close()
		//	}
		//}()

		var mf mergedDomainFiles
		if mf.values, mf.index, mf.history, err = d.mergeFiles(ctx, sfr.valuesFiles, sfr.indexFiles, sfr.historyFiles, rng, workers); err != nil {
			return err
		}
		//defer func() {
		//	if closeAll {
		//		mf.Close()
		//	}
		//}()

		//defer func(t time.Time) { log.Info("[snapshots] merge", "took", time.Since(t)) }(time.Now())
		d.integrateMergedFiles(sfr.valuesFiles, sfr.indexFiles, sfr.historyFiles, mf.values, mf.index, mf.history)

		if err := d.deleteFiles(sfr.valuesFiles, sfr.indexFiles, sfr.historyFiles); err != nil {
			return err
		}

		log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%+v\n", maxTxNum, maxSpan, rng))
	}
	//closeAll = false
	return nil
}
