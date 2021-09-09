package kvcache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	_ "github.com/VictoriaMetrics/fastcache"
	"github.com/google/btree"
	_ "github.com/google/btree"
	_ "github.com/iwanbk/bcache"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
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
//
// Pair.Value == nil - is a marker of absense key in db

type Cache struct {
	latest    string //latest root
	roots     map[string]*CacheRoot
	rootsLock sync.RWMutex
}
type CacheRoot struct {
	cache *btree.BTree
	lock  sync.RWMutex
	ready chan struct{} // close when ready
}

type Pair struct {
	K, V []byte
}

func (p *Pair) Less(than btree.Item) bool { return bytes.Compare(p.K, than.(*Pair).K) < 0 }

func New() *Cache {
	return &Cache{roots: map[string]*CacheRoot{}}
}

// selectOrCreateRoot - used for usual getting root
func (c *Cache) selectOrCreateRoot(root string) *CacheRoot {
	c.rootsLock.RLock()
	r, ok := c.roots[root]
	c.rootsLock.RUnlock()
	if ok {
		return r
	}

	c.rootsLock.Lock()
	r = &CacheRoot{ready: make(chan struct{})}
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
func (c *Cache) advanceRoot(root string, direction remote.Direction) (r *CacheRoot, fastUnwind bool) {
	c.rootsLock.Lock()
	defer c.rootsLock.Unlock()
	r, ok := c.roots[root]
	if !ok {
		r = &CacheRoot{ready: make(chan struct{})}
	}
	if c.latest == "" {
		fmt.Printf("advance: empty latest: %x\n", root)
		c.roots[root] = r
		r.cache = btree.New(32)
		c.latest = root
		return r, false
	}

	//TODO: need check if c.latest hash is still canonical
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
	c.roots[root] = r
	c.latest = root
	return r, fastUnwind
}

func (c *Cache) OnNewBlock(sc *remote.StateChange) {
	//TODO: clone right root
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
	close(r.ready) //broadcast
}

func (c *Cache) View(tx kv.Tx) (*CacheRoot, error) {
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
		<-r.ready
	}
	return r, nil
}

func (c *CacheRoot) Get(k []byte, tx kv.Tx) ([]byte, error) {
	c.lock.RLock()
	it := c.cache.Get(&Pair{K: k})
	c.lock.RUnlock()

	if it == nil {
		v, err := tx.GetOne(kv.PlainState, k)
		if err != nil {
			return nil, err
		}

		c.lock.RLock()
		it = &Pair{K: k, V: v}
		c.cache.ReplaceOrInsert(it)
		c.lock.RUnlock()
		fmt.Printf("from db: %#x,%#v\n", k, v)
	} else {
		fmt.Printf("from cache: %#x,%#v\n", k, it.(*Pair).V)
	}
	return it.(*Pair).V, nil
}

//var g singleflight.Group
//
//func A(key, kv kv.RoDB) {
//	g.Do("", key)
//	singleflight.Group{}
//}
