package kvcache

import (
	"sync"

	_ "github.com/VictoriaMetrics/fastcache"
	"github.com/google/btree"
	_ "github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/kv"
	"golang.org/x/sync/singleflight"
	_ "golang.org/x/sync/singleflight"
)

// StateCache works on top of Database Transaction and pair Cache+ReadTransaction must
// provide "Serializable Isolation Level" semantic: all data form consistent db view at moment
// when read transaction started, read data are immutable until end of read transaction, reader can't see newer updates
//
// - StateDiff event does clone cache and set new head.
//
// - Readers do firstly check kv.Tx:
//      - get latest block number and hash, and by this key get cache instance
//      - if cache instance found - just use it
//      - if not found and blockNumber>cache.top.number - wait for StateDiff on conditional variable
//      - otherwise - ???
// - If found in cache - return value without copy (reader can rely on fact that data are immutable until end of db transaction)
// - Otherwise just read from db (no requests deduplication for now - preliminary optimization).

var roots map[string]CacheRoot
var rootsLock sync.RWMutex

type CacheRoot struct {
	cache *btree.BTree
	lock  sync.RWMutex
	cond  *sync.Cond
}

var g singleflight.Group

func A(key, kv kv.RoDB) {
	g.Do("", key)
	singleflight.Group{}
}
