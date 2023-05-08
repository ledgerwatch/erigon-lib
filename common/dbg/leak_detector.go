package dbg

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/log/v3"
)

// LeakDetector - use it to find which resource was created but not closed (leaked)
// periodically does print in logs resources which living longer than 1min with their creation stack trace
// For example db transactions can call Add/Del from Begin/Commit/Rollback methods
type LeakDetector struct {
	enabled       bool
	list          map[uint64]LeakDetectorItem
	autoIncrement atomic.Uint64
	lock          sync.Mutex
}

type LeakDetectorItem struct {
	stack   string
	started time.Time
}

func NewLeakDetector(name string, enabled bool) *LeakDetector {
	if !enabled {
		return nil
	}
	d := &LeakDetector{enabled: enabled, list: map[uint64]LeakDetectorItem{}}
	if enabled {
		log.Warn("m1")
		go func() {
			logEvery := time.NewTicker(60 * time.Second)
			defer logEvery.Stop()
			log.Warn("m2")

			for {
				select {
				case <-logEvery.C:
					log.Warn("m3")
					if list := d.slowList(); len(list) > 0 {
						log.Info(fmt.Sprintf("[dbg.%s]", name), "slow", strings.Join(d.slowList(), ", "))
					}
				}
			}
		}()
	}
	return d
}

func (d *LeakDetector) slowList() (res []string) {
	if d == nil || !d.enabled {
		return res
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	for key, value := range d.list {
		if time.Since(value.started) > time.Minute {
			res = append(res, strconv.Itoa(int(key))+": "+value.stack)
		}
	}
	return res
}

func (d *LeakDetector) Del(id uint64) {
	if d == nil || !d.enabled {
		return
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.list, id)
}
func (d *LeakDetector) Add() uint64 {
	if d == nil || !d.enabled {
		return 0
	}
	ac := LeakDetectorItem{
		stack:   StackSkip(1),
		started: time.Now(),
	}
	id := d.autoIncrement.Add(1)
	d.lock.Lock()
	defer d.lock.Unlock()
	d.list[id] = ac
	return 0
}
