package dbg

import (
	"fmt"
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
		go func() {
			logEvery := time.NewTicker(60 * time.Second)
			defer logEvery.Stop()

			for {
				select {
				case <-logEvery.C:
					if list := d.slowList(); len(list) > 0 {
						log.Info(fmt.Sprintf("[dbg.%s] long living resources", name), "list", strings.Join(d.slowList(), ", "))
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
	i := 0
	for key, value := range d.list {
		living := time.Since(value.started)
		if living > time.Minute {
			res = append(res, fmt.Sprintf("%d(%s): %s", key, living, value.stack))
		}
		i++
		if i > 10 { // protect logs from too many output
			break
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
		stack:   StackSkip(2),
		started: time.Now(),
	}
	id := d.autoIncrement.Add(1)
	d.lock.Lock()
	defer d.lock.Unlock()
	d.list[id] = ac
	return id
}
