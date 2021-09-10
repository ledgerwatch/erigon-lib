package kvcache

import (
	"github.com/ledgerwatch/erigon-lib/kv"
)

// DummyCache - doesn't remember anything - can be used when service is not remote
type DummyCache struct{}
type DummyView struct{}

var dummyView = &DummyView{}

func NewDummy() *DummyCache                                 { return &DummyCache{} }
func (c *DummyCache) View(tx kv.Tx) (*DummyView, error)     { return dummyView, nil }
func (c *DummyView) Get(k []byte, tx kv.Tx) ([]byte, error) { return tx.GetOne(kv.PlainState, k) }
