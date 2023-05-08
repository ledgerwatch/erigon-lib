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

package dbg

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	stack2 "github.com/go-stack/stack"
	"github.com/ledgerwatch/log/v3"
)

// Stack returns stack-trace in logger-friendly compact formatting
func Stack() string {
	return stack2.Trace().TrimBelow(stack2.Caller(1)).String()
}

type LeackDetector struct {
	enabled       bool
	doTraceTxs    bool
	traceTxs      map[uint64]LeackDetectorItem
	autoIncrement atomic.Uint64
	lock          sync.Mutex
}

type LeackDetectorItem struct {
	stack   string
	started time.Time
}

func NewLeackDetector(name string, enabled bool) *LeackDetector {
	d := &LeackDetector{enabled: enabled}
	if enabled {
		go func() {
			logEvery := time.NewTicker(30 * time.Second)
			defer logEvery.Stop()

			for {
				select {
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[dbg.%s]", name), "slow", strings.Join(d.slowList(), ","))
				}
			}
		}()
	}
	return d
}

func (d *LeackDetector) slowList() (res []string) {
	if d == nil || !d.enabled {
		return res
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	for key, value := range d.traceTxs {
		if time.Since(value.started) > time.Minute {
			res = append(res, strconv.Itoa(int(key))+": "+value.stack)
		}
	}
	return res
}

func (d *LeackDetector) Del(id uint64) {
	if d == nil || !d.enabled {
		return
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.traceTxs, id)
}
func (d *LeackDetector) Add() uint64 {
	if d == nil || !d.enabled {
		return 0
	}
	ac := LeackDetectorItem{
		stack:   Stack(),
		started: time.Now(),
	}
	id := d.autoIncrement.Add(1)
	d.lock.Lock()
	defer d.lock.Unlock()
	d.traceTxs[id] = ac
	return 0
}
