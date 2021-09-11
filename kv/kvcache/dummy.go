package kvcache

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
)

// DummyCache - doesn't remember anything - can be used when service is not remote
type DummyCache struct{}
type DummyView struct{}

var _ Cache = (*DummyCache)(nil)    // compile-time interface check
var _ CacheView = (*DummyView)(nil) // compile-time interface check

var dummyView = &DummyView{}

func NewDummy() *DummyCache                                                 { return &DummyCache{} }
func (c *DummyCache) View(ctx context.Context, tx kv.Tx) (CacheView, error) { return dummyView, nil }
func (c *DummyCache) OnNewBlock(sc *remote.StateChange)                     {}
func (c *DummyCache) Evict()                                                {}
func (c *DummyView) Get(k []byte, tx kv.Tx) ([]byte, error)                 { return tx.GetOne(kv.PlainState, k) }
