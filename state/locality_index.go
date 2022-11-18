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
	"fmt"
	"path/filepath"
	"time"

	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
)

// LocalityIndex - has info in which .ef files exists given key
// Format: key -> bitmap(step_number_list)
// step_number_list is list of .ef files where exists given key
type LocalityIndex struct {
	//file         *filesItem
	//filenameBase string
	//dir          string // Directory where static files are created
	//tmpdir       string // Directory where static files are created

	files *filesItem
}

func (l *LocalityIndex) Build(ctx context.Context, toStep uint64, h *History) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	fromStep := uint64(0)

	count := 0
	it := h.MakeContext().iterateReconTxs(nil, nil, toStep*h.aggregationStep)
	for it.HasNext() {
		_, _, progress := it.Next()
		count++
		select {
		default:
		case <-logEvery.C:
			log.Debug("[LocalityIndex] build", "progres", progress)
		}
	}

	lFName := fmt.Sprintf("%s.%d-%d.l", h.filenameBase, fromStep, toStep)
	lFPath := filepath.Join(h.dir, lFName)
	comp, err := compress.NewCompressor(ctx, "", lFPath, h.tmpdir, compress.MinPatternScore, h.workers, log.LvlTrace)
	if err != nil {
		return fmt.Errorf("create %s compressor: %w", h.filenameBase, err)
	}
	defer comp.Close()

	fName := fmt.Sprintf("%s.%d-%d.li", h.filenameBase, fromStep, toStep)
	idxPath := filepath.Join(h.dir, fName)

	var rs *recsplit.RecSplit
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.tmpdir,
		IndexFile:  idxPath,
	}); err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	bitmap := make([]byte, 4)
	var i uint64
	for {
		it = h.MakeContext().iterateReconTxs(nil, nil, toStep*h.aggregationStep)
		var prevK []byte
		for it.HasNext() {
			k, txNum, progress := it.Next()
			inStep := int(txNum / h.aggregationStep)
			bitmap[inStep%4] &= byte(inStep / 4)
			isNew := !bytes.Equal(k, prevK)
			if isNew {
				if err = comp.AddUncompressedWord(bitmap); err != nil {
					return err
				}
				offfset := i * 4
				if err = rs.AddKey(prevK, offfset); err != nil {
					return err
				}
				bitmap[0], bitmap[1], bitmap[2], bitmap[3] = 0, 0, 0, 0
				i++
			}
			prevK = append(prevK[:0], k...)

			select {
			default:
			case <-logEvery.C:
				log.Debug("[LocalityIndex] build", "progres", progress)
			}
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	if err = comp.Compress(); err != nil {
		return err
	}
	comp.Close()

	idx, err := recsplit.OpenIndex(idxPath)
	if err != nil {
		return fmt.Errorf("open idx: %w", err)
	}
	dec, err := compress.NewDecompressor(lFPath)
	if err != nil {
		return fmt.Errorf("open idx: %w", err)
	}
	l.files = &filesItem{index: idx, decompressor: dec, startTxNum: fromStep * h.aggregationStep, endTxNum: toStep * h.aggregationStep}
	return nil
}
