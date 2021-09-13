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
