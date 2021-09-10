package kvcache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"go.uber.org/atomic"
)

// StateCache works on top of Database Transaction and pair Coherent+ReadTransaction must
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
//
// Pair.Value == nil - is a marker of absense key in db

type Cache interface {
	// View - returns CacheView consistent with givent kv.Tx
	View(tx kv.Tx) (CacheView, error)
	Evict()
}
type CacheView interface {
	Get(k []byte, tx kv.Tx) ([]byte, error)
}

var _ Cache = (*Coherent)(nil)         // compile-time interface check
var _ CacheView = (*CoherentView)(nil) // compile-time interface check

type Coherent struct {
	latest    string //latest root
	roots     map[string]*CoherentView
	rootsLock sync.RWMutex
}
type CoherentView struct {
	cache           *btree.BTree
	lock            sync.RWMutex
	ready           chan struct{} // close when ready
	readyChanClosed atomic.Bool   // protecting `ready` field from double-close (on unwind). Consumers don't need check this field.
}

type Pair struct {
	K, V []byte
}

func (p *Pair) Less(than btree.Item) bool { return bytes.Compare(p.K, than.(*Pair).K) < 0 }

func New() *Coherent {
	return &Coherent{roots: map[string]*CoherentView{}}
}

// selectOrCreateRoot - used for usual getting root
func (c *Coherent) selectOrCreateRoot(root string) *CoherentView {
	c.rootsLock.RLock()
	r, ok := c.roots[root]
	c.rootsLock.RUnlock()
	if ok {
		return r
	}

	c.rootsLock.Lock()
	r = &CoherentView{ready: make(chan struct{})}
	latestRoot, ok := c.roots[c.latest]
	if ok {
		fmt.Printf("clone: %x\n", c.latest)
		r.cache = latestRoot.cache.Clone()
	} else {
		fmt.Printf("create empty root: %x\n", root)
		r.cache = btree.New(32)
	}
	c.roots[root] = r
	c.rootsLock.Unlock()
	return r
}

// advanceRoot - used for advancing root onNewBlock
func (c *Coherent) advanceRoot(root string, direction remote.Direction) (r *CoherentView, fastUnwind bool) {
	c.rootsLock.RLock()
	r, ok := c.roots[root]
	c.rootsLock.RUnlock()
	if !ok {
		r = &CoherentView{ready: make(chan struct{})}
	}
	if c.latest == "" {
		c.rootsLock.Lock()
		c.roots[root] = r
		r.cache = btree.New(32)
		c.latest = root
		c.rootsLock.Unlock()
		return r, false
	}

	//TODO: need check if c.latest hash is still canonical. If not - can't clone from it
	c.rootsLock.RLock()
	switch direction {
	case remote.Direction_FORWARD:
		fmt.Printf("advance: clone: %x\n", c.latest)
		r.cache = c.roots[c.latest].cache.Clone()
	case remote.Direction_UNWIND:
		fmt.Printf("unwind: %x\n", c.latest)
		oldRoot, ok := c.roots[root]
		if ok {
			r = oldRoot
			fastUnwind = true
		} else {
			r.cache = btree.New(32)
		}

	default:
		panic("not implemented yet")
	}
	c.rootsLock.RUnlock()

	c.rootsLock.Lock()
	c.roots[root] = r
	c.latest = root
	c.rootsLock.Unlock()
	return r, fastUnwind
}

func (c *Coherent) OnNewBlock(sc *remote.StateChange) {
	h := gointerfaces.ConvertH256ToHash(sc.BlockHash)
	root := make([]byte, 40)
	binary.BigEndian.PutUint64(root, sc.BlockHeight)
	copy(root[8:], h[:])
	r, _ := c.advanceRoot(string(root), sc.Direction)
	r.lock.Lock()
	for i := range sc.Changes {
		switch sc.Changes[i].Action {
		case remote.Action_UPSERT:
			addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
			v := sc.Changes[i].Data
			r.cache.ReplaceOrInsert(&Pair{K: addr[:], V: v})
		case remote.Action_DELETE:
			addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
			r.cache.ReplaceOrInsert(&Pair{K: addr[:], V: nil})
		case remote.Action_CODE, remote.Action_UPSERT_CODE:
			//skip
		case remote.Action_STORAGE:
			addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
			for _, change := range sc.Changes[i].StorageChanges {
				loc := gointerfaces.ConvertH256ToHash(change.Location)
				k := make([]byte, 20+8+32)
				copy(k, addr[:])
				binary.BigEndian.PutUint64(k[20:], sc.Changes[i].Incarnation)
				copy(k[20+8:], loc[:])
				r.cache.ReplaceOrInsert(&Pair{K: addr[:], V: change.Data})
			}
		default:
			panic("not implemented yet")
		}
	}
	r.lock.Unlock()
	switched := r.readyChanClosed.CAS(false, true)
	if switched {
		close(r.ready) //broadcast
	}
}

func (c *Coherent) View(tx kv.Tx) (CacheView, error) {
	//TODO: handle case when db has no records
	encBlockNum, err := tx.GetOne(kv.SyncStageProgress, []byte("Finish"))
	if err != nil {
		return nil, err
	}
	blockHash, err := tx.GetOne(kv.HeaderCanonical, encBlockNum)
	if err != nil {
		return nil, err
	}
	root := make([]byte, 8+32)
	copy(root, encBlockNum)
	copy(root[8:], blockHash)
	c.rootsLock.RLock()
	doBlock := c.latest != ""
	c.rootsLock.RUnlock()

	fmt.Printf("choose root: %x\n", root)
	r := c.selectOrCreateRoot(string(root))
	if doBlock {
		select {
		case <-r.ready:
		case <-time.After(100 * time.Millisecond):
		}
	}
	return r, nil
}

func (c *CoherentView) Get(k []byte, tx kv.Tx) ([]byte, error) {
	c.lock.RLock()
	it := c.cache.Get(&Pair{K: k})
	c.lock.RUnlock()

	if it != nil {
		fmt.Printf("from cache: %#x,%#v\n", k, it.(*Pair).V)
		return it.(*Pair).V, nil
	}

	v, err := tx.GetOne(kv.PlainState, k)
	if err != nil {
		return nil, err
	}
	fmt.Printf("from db: %#x,%#v\n", k, v)

	it = &Pair{K: k, V: v}
	c.lock.Lock()
	c.cache.ReplaceOrInsert(it)
	c.lock.Unlock()
	return it.(*Pair).V, nil
}

func AssertCheckValues(tx kv.Tx, cache *Coherent) error {
	c, err := cache.View(tx)
	if err != nil {
		return err
	}
	c.(*CoherentView).cache.Ascend(func(i btree.Item) bool {
		k, v := i.(*Pair).K, i.(*Pair).V
		var dbV []byte
		dbV, err = tx.GetOne(kv.PlainState, k)
		if err != nil {
			return false
		}
		if !bytes.Equal(dbV, v) {
			err = fmt.Errorf("key: %x, has different values: %x != %x", k, v, copyBytes(dbV))
			return false
		}
		return true
	})
	return err
}

func copyBytes(b []byte) (copiedBytes []byte) {
	if b == nil {
		return nil
	}
	copiedBytes = make([]byte, len(b))
	copy(copiedBytes, b)
	return
}

func (c *Coherent) Evict() {
	c.rootsLock.Lock()
	defer c.rootsLock.Unlock()
	if c.latest == "" {
		return
	}
	latestBlockNum := binary.BigEndian.Uint64([]byte(c.latest))
	var toDel []string
	for root := range c.roots {
		blockNum := binary.BigEndian.Uint64([]byte(root))
		if blockNum > latestBlockNum-100 {
			continue
		}
		toDel = append(toDel, root)
	}
	for _, root := range toDel {
		delete(c.roots, root)
	}
}
