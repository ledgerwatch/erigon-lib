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

package txpool

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-stack/stack"
	"github.com/google/btree"
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_txpool "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
	"google.golang.org/grpc/status"
)

var (
	processBatchTxsTimer    = metrics.NewSummary(`pool_process_remote_txs`)
	addRemoteTxsTimer       = metrics.NewSummary(`pool_add_remote_txs`)
	newBlockTimer           = metrics.NewSummary(`pool_new_block`)
	writeToDbTimer          = metrics.NewSummary(`pool_write_to_db`)
	propagateToNewPeerTimer = metrics.NewSummary(`pool_propagate_to_new_peer`)
	propagateNewTxsTimer    = metrics.NewSummary(`pool_propagate_new_txs`)
	writeToDbBytesCounter   = metrics.GetOrCreateCounter(`pool_write_to_db_bytes`)
)

const ASSERT = false

type Config struct {
	DBDir                 string
	SyncToNewPeersEvery   time.Duration
	ProcessRemoteTxsEvery time.Duration
	CommitEvery           time.Duration
	LogEvery              time.Duration
	CacheEvictEvery       time.Duration

	PendingSubPoolLimit int
	BaseFeeSubPoolLimit int
	QueuedSubPoolLimit  int
}

var DefaultConfig = Config{
	SyncToNewPeersEvery:   2 * time.Minute,
	ProcessRemoteTxsEvery: 100 * time.Millisecond,
	CommitEvery:           15 * time.Second,
	LogEvery:              30 * time.Second,
	CacheEvictEvery:       1 * time.Minute,

	PendingSubPoolLimit: 50_000,
	BaseFeeSubPoolLimit: 200_000,
	QueuedSubPoolLimit:  90_000,
}

// Pool is interface for the transaction pool
// This interface exists for the convinience of testing, and not yet because
// there are multiple implementations
type Pool interface {
	// Handle 3 main events - new remote txs from p2p, new local txs from RPC, new blocks from execution layer
	AddRemoteTxs(ctx context.Context, newTxs TxSlots)
	AddLocalTxs(ctx context.Context, newTxs TxSlots) ([]DiscardReason, error)
	OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs TxSlots, tx kv.Tx) error

	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(tx kv.Tx, hash []byte) (bool, error)
	Started() bool
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)

	AddNewGoodPeer(peerID PeerID)
}

var _ Pool = (*TxPool)(nil) // compile-time interface check

// SubPoolMarker ordered bitset responsible to sort transactions by sub-pools. Bits meaning:
// 1. Minimum fee requirement. Set to 1 if feeCap of the transaction is no less than in-protocol parameter of minimal base fee. Set to 0 if feeCap is less than minimum base fee, which means this transaction will never be included into this particular chain.
// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for the sender is M, and there are transactions for all nonces between M and N from the same sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
// 3. Sufficient balance for gas. Set to 1 if the balance of sender's account in the state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the sum of feeCap x gasLimit + transferred_value of all transactions from this sender with nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is set if there is currently a guarantee that the transaction and all its required prior transactions will be able to pay for gas.
// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than baseFee of the currently pending block. Set to 0 otherwise.
// 5. Local transaction. Set to 1 if transaction is local.
type SubPoolMarker uint8

const (
	EnoughFeeCapProtocol = 0b10000
	NoNonceGaps          = 0b01000
	EnoughBalance        = 0b00100
	EnoughFeeCapBlock    = 0b00010
	IsLocal              = 0b00001
)

type DiscardReason uint8

const (
	//TODO: all below codes are not fixed yet. Need add them to discardLocked func. Need save discard reasons to LRU or DB.
	Success             DiscardReason = 1
	AlreadyKnown        DiscardReason = 2
	Mined               DiscardReason = 3
	ReplacedByHigherTip DiscardReason = 4
	UnderPriced         DiscardReason = 5
	FeeTooLow           DiscardReason = 6
	OversizedData       DiscardReason = 7
	InvalidSender       DiscardReason = 8
	NegativeValue       DiscardReason = 9
	PendingPoolOverflow DiscardReason = 10
	BaseFeePoolOverflow DiscardReason = 11
	QueuedPoolOverflow  DiscardReason = 12
)

// metaTx holds transaction and some metadata
type metaTx struct {
	Tx             *TxSlot
	subPool        SubPoolMarker
	effectiveTip   uint64 // max(minTip, minFeeCap - baseFee)
	bestIndex      int
	worstIndex     int
	currentSubPool SubPoolType
	timestamp      uint64 // when it was added to pool
}

func newMetaTx(slot *TxSlot, isLocal bool, timestmap uint64) *metaTx {
	mt := &metaTx{Tx: slot, worstIndex: -1, bestIndex: -1, timestamp: timestmap}
	if isLocal {
		mt.subPool = IsLocal
	}
	return mt
}

type SubPoolType uint8

const PendingSubPool SubPoolType = 1
const BaseFeeSubPool SubPoolType = 2
const QueuedSubPool SubPoolType = 3

// sender - immutable structure which stores only nonce and balance of account
type sender struct {
	balance uint256.Int
	nonce   uint64
}

func newSender(nonce uint64, balance uint256.Int) *sender {
	return &sender{nonce: nonce, balance: balance}
}

var emptySender = newSender(0, *uint256.NewInt(0))

type sortByNonce struct{ *metaTx }

func (i *sortByNonce) Less(than btree.Item) bool {
	if i.metaTx.Tx.senderID != than.(*sortByNonce).metaTx.Tx.senderID {
		return i.metaTx.Tx.senderID < than.(*sortByNonce).metaTx.Tx.senderID
	}
	return i.metaTx.Tx.nonce < than.(*sortByNonce).metaTx.Tx.nonce
}

func calcProtocolBaseFee(baseFee uint64) uint64 {
	return 7
}

// TxPool - holds all pool-related data structures and lock-based tiny methods
// most of logic implemented by pure tests-friendly functions
//
// txpool doesn't start any goroutines - "leave concurrency to user" design
// txpool has no DB or TX fields - "leave db transactions management to user" design
// txpool has _coreDB field - but it must maximize local state cache hit-rate - and perform minimum _coreDB transactions
//
// It preserve TxSlot objects immutable
type TxPool struct {
	lock *sync.RWMutex

	started         atomic.Bool
	lastSeenBlock   atomic.Uint64
	protocolBaseFee atomic.Uint64
	currentBaseFee  atomic.Uint64

	senderID          uint64
	byHash            map[string]*metaTx // tx_hash => tx
	discardReasonsLRU *simplelru.LRU     // tx_hash => discard_reason
	pending           *PendingPool
	baseFee, queued   *SubPool

	// track isLocal flag of already mined transactions. used at unwind.
	isLocalLRU *simplelru.LRU // tx_hash => is_local
	_coreDB    kv.RoDB

	// fields for transaction propagation
	recentlyConnectedPeers *recentlyConnectedPeers
	newTxs                 chan Hashes
	deletedTxs             []*metaTx
	senders                *sendersBatch
	_cache                 kvcache.Cache
	byNonce                *ByNonce // senderID => (sorted map of tx nonce => *metaTx)

	// batch processing of remote transactions
	// handling works fast without batching, but batching allow:
	//   - reduce amount of _coreDB transactions
	//   - batch notifications about new txs (reduce P2P spam to other nodes about txs propagation)
	//   - and as a result reducing pool.RWLock contention
	unprocessedRemoteTxs    *TxSlots
	unprocessedRemoteByHash map[string]int // to reject duplicates

	cfg     Config
	rules   chain.Rules
	chainID uint256.Int
}

func New(newTxs chan Hashes, coreDB kv.RoDB, cfg Config, cache kvcache.Cache, rules chain.Rules, chainID uint256.Int) (*TxPool, error) {
	localsHistory, err := simplelru.NewLRU(10_000, nil)
	if err != nil {
		return nil, err
	}
	discardHistory, err := simplelru.NewLRU(10_000, nil)
	if err != nil {
		return nil, err
	}
	return &TxPool{
		lock:                    &sync.RWMutex{},
		byHash:                  map[string]*metaTx{},
		isLocalLRU:              localsHistory,
		discardReasonsLRU:       discardHistory,
		byNonce:                 &ByNonce{btree.New(32)},
		recentlyConnectedPeers:  &recentlyConnectedPeers{},
		pending:                 NewPendingSubPool(PendingSubPool),
		baseFee:                 NewSubPool(BaseFeeSubPool),
		queued:                  NewSubPool(QueuedSubPool),
		newTxs:                  newTxs,
		_cache:                  cache,
		senders:                 newSendersCache(),
		_coreDB:                 coreDB,
		cfg:                     cfg,
		rules:                   rules,
		chainID:                 chainID,
		senderID:                1,
		unprocessedRemoteTxs:    &TxSlots{},
		unprocessedRemoteByHash: map[string]int{},
	}, nil
}

func (p *TxPool) OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs TxSlots, tx kv.Tx) error {
	defer newBlockTimer.UpdateDuration(time.Now())
	t := time.Now()

	cache := p.cache()
	cache.OnNewBlock(stateChanges)
	coreTx, err := p.coreDB().BeginRo(ctx)
	if err != nil {
		return err
	}
	defer coreTx.Rollback()

	if err := p.fromDB(ctx, tx, coreTx); err != nil {
		return err
	}

	viewID, err := cache.View(ctx, coreTx)
	if err != nil {
		return err
	}
	if ASSERT {
		if _, err := kvcache.AssertCheckValues(ctx, coreTx, cache); err != nil {
			log.Error("AssertCheckValues", "err", err, "stack", stack.Trace().String())
		}
	}

	if err := unwindTxs.Valid(); err != nil {
		return err
	}
	if err := minedTxs.Valid(); err != nil {
		return err
	}
	baseFee := stateChanges.ChangeBatch[len(stateChanges.ChangeBatch)-1].ProtocolBaseFee
	blockHeight := stateChanges.ChangeBatch[len(stateChanges.ChangeBatch)-1].BlockHeight

	p.lock.Lock()
	defer p.lock.Unlock()

	protocolBaseFee, baseFee := p.setBaseFee(baseFee)
	p.lastSeenBlock.Store(blockHeight)
	if err := p.senders.onNewBlock(stateChanges, unwindTxs, minedTxs); err != nil {
		return err
	}

	if ASSERT {
		for i := range unwindTxs.txs {
			if unwindTxs.txs[i].senderID == 0 {
				panic(fmt.Errorf("onNewBlock.unwindTxs: senderID can't be zero"))
			}
		}
		for i := range minedTxs.txs {
			if minedTxs.txs[i].senderID == 0 {
				panic(fmt.Errorf("onNewBlock.minedTxs: senderID can't be zero"))
			}
		}
	}

	if err := removeMined(p.byNonce, minedTxs.txs, p.pending, p.baseFee, p.queued, p.discardLocked); err != nil {
		return err
	}

	//log.Debug("[txpool] new block", "unwinded", len(unwindTxs.txs), "mined", len(minedTxs.txs), "baseFee", baseFee, "blockHeight", blockHeight)
	if err := addTxs(p.lastSeenBlock.Load(), cache, viewID, coreTx, p.cfg, p.senders, unwindTxs, protocolBaseFee, baseFee, p.pending, p.baseFee, p.queued, p.byNonce, p.byHash, p.addLocked, p.discardLocked); err != nil {
		return err
	}

	if p.started.CAS(false, true) {
		log.Info("[txpool] Started")
	}

	notifyNewTxs := make(Hashes, 0, 32*len(unwindTxs.txs))
	for i := range unwindTxs.txs {
		_, ok := p.byHash[string(unwindTxs.txs[i].idHash[:])]
		if !ok {
			continue
		}
		notifyNewTxs = append(notifyNewTxs, unwindTxs.txs[i].idHash[:]...)
	}
	if len(notifyNewTxs) > 0 {
		select {
		case p.newTxs <- notifyNewTxs:
		default:
		}
	}

	log.Info("[txpool] new block", "number", p.lastSeenBlock.Load(), "in", time.Since(t))
	return nil
}

func (p *TxPool) processRemoteTxs(ctx context.Context) error {
	if !p.started.Load() {
		return fmt.Errorf("txpool not started yet")
	}

	cache := p.cache()
	defer processBatchTxsTimer.UpdateDuration(time.Now())
	coreTx, err := p.coreDB().BeginRo(ctx)
	if err != nil {
		return err
	}
	defer coreTx.Rollback()
	viewID, err := cache.View(ctx, coreTx)
	if err != nil {
		return err
	}

	//t := time.Now()
	p.lock.Lock()
	defer p.lock.Unlock()

	l := len(p.unprocessedRemoteTxs.txs)
	if l == 0 {
		return nil
	}
	newTxs := *p.unprocessedRemoteTxs

	if err := newTxs.Valid(); err != nil {
		return err
	}
	err = p.senders.onNewTxs(newTxs)
	if err != nil {
		return err
	}

	if err := addTxs(p.lastSeenBlock.Load(), cache, viewID, coreTx, p.cfg, p.senders, newTxs, p.protocolBaseFee.Load(), p.currentBaseFee.Load(), p.pending, p.baseFee, p.queued, p.byNonce, p.byHash, p.addLocked, p.discardLocked); err != nil {
		return err
	}

	// notify about all non-dropped txs
	notifyNewTxs := make(Hashes, 0, 32*len(newTxs.txs))
	for i := range newTxs.txs {
		_, ok := p.byHash[string(newTxs.txs[i].idHash[:])]
		if !ok {
			continue
		}
		notifyNewTxs = append(notifyNewTxs, newTxs.txs[i].idHash[:]...)
	}
	if len(notifyNewTxs) > 0 {
		select {
		case <-ctx.Done():
			return nil
		case p.newTxs <- notifyNewTxs:
		default:
		}
	}

	p.unprocessedRemoteTxs.Resize(0)
	p.unprocessedRemoteByHash = map[string]int{}

	//log.Info("[txpool] on new txs", "amount", len(newTxs.txs), "in", time.Since(t))
	return nil
}
func (p *TxPool) getRlpLocked(tx kv.Tx, hash []byte) (rlpTxn []byte, sender []byte, isLocal bool, err error) {
	txn, ok := p.byHash[string(hash)]
	if ok && txn.Tx.rlp != nil {
		return txn.Tx.rlp, []byte(p.senders.senderID2Addr[txn.Tx.senderID]), txn.subPool&IsLocal > 0, nil
	}
	v, err := tx.GetOne(kv.PoolTransaction, hash)
	if err != nil {
		return nil, nil, false, err
	}
	if v == nil {
		return nil, nil, false, nil
	}
	return v[20:], v[:20], txn != nil && txn.subPool&IsLocal > 0, nil
}
func (p *TxPool) GetRlp(tx kv.Tx, hash []byte) ([]byte, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	rlpTx, _, _, err := p.getRlpLocked(tx, hash)
	return rlpTx, err
}
func (p *TxPool) AppendLocalHashes(buf []byte) []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	for hash, txn := range p.byHash {
		if txn.subPool&IsLocal == 0 {
			continue
		}
		buf = append(buf, hash...)
	}
	return buf
}
func (p *TxPool) AppendRemoteHashes(buf []byte) []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()

	for hash, txn := range p.byHash {
		if txn.subPool&IsLocal != 0 {
			continue
		}
		buf = append(buf, hash...)
	}
	for hash := range p.unprocessedRemoteByHash {
		buf = append(buf, hash...)
	}
	return buf
}
func (p *TxPool) AppendAllHashes(buf []byte) []byte {
	buf = p.AppendLocalHashes(buf)
	buf = p.AppendRemoteHashes(buf)
	return buf
}
func (p *TxPool) IdHashKnown(tx kv.Tx, hash []byte) (bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if _, ok := p.unprocessedRemoteByHash[string(hash)]; ok {
		return true, nil
	}
	if _, ok := p.byHash[string(hash)]; ok {
		return true, nil
	}
	return tx.Has(kv.PoolTransaction, hash)
}
func (p *TxPool) IsLocal(idHash []byte) bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.isLocalLRU.Contains(string(idHash))
}
func (p *TxPool) DiscardReason(idHash []byte) DiscardReason {
	p.lock.RLock()
	defer p.lock.RUnlock()
	reason, ok := p.discardReasonsLRU.Get(string(idHash))
	if ok {
		return reason.(DiscardReason)
	}
	return 0
}
func (p *TxPool) AddNewGoodPeer(peerID PeerID) { p.recentlyConnectedPeers.AddPeer(peerID) }
func (p *TxPool) Started() bool                { return p.started.Load() }

// Best - returns top `n` elements of pending queue
// id doesn't perform full copy of txs, hovewer underlying elements are immutable
func (p *TxPool) Best(n uint16, txs *TxsRlp, tx kv.Tx) error {
	p.lock.RLock()
	defer p.lock.RUnlock()

	txs.Resize(uint(min(uint64(n), uint64(len(p.pending.best)))))

	best := p.pending.best
	for i := 0; i < int(n) && i < len(best); i++ {
		rlpTx, sender, isLocal, err := p.getRlpLocked(tx, best[i].Tx.idHash[:])
		if err != nil {
			return err
		}
		if len(rlpTx) == 0 {
			continue
		}
		txs.Txs[i] = rlpTx
		copy(txs.Senders.At(i), sender)
		txs.IsLocal[i] = isLocal
	}
	return nil
}

func (p *TxPool) CountContent() (int, int, int) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.pending.Len(), p.baseFee.Len(), p.queued.Len()
}
func (p *TxPool) AddRemoteTxs(_ context.Context, newTxs TxSlots) {
	defer addRemoteTxsTimer.UpdateDuration(time.Now())
	p.lock.Lock()
	defer p.lock.Unlock()
	for i := range newTxs.txs {
		_, ok := p.unprocessedRemoteByHash[string(newTxs.txs[i].idHash[:])]
		if ok {
			continue
		}
		p.unprocessedRemoteTxs.Append(newTxs.txs[i], newTxs.senders.At(i), newTxs.isLocal[i])
	}
}
func (p *TxPool) AddLocalTxs(ctx context.Context, newTxs TxSlots) ([]DiscardReason, error) {
	if err := newTxs.Valid(); err != nil {
		return nil, err
	}

	coreTx, err := p.coreDB().BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer coreTx.Rollback()

	viewID, err := p.cache().View(ctx, coreTx)
	if err != nil {
		return nil, err
	}

	if !p.Started() {
		return nil, fmt.Errorf("pool not started yet")
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if err = p.senders.onNewTxs(newTxs); err != nil {
		return nil, err
	}
	if err := addTxs(p.lastSeenBlock.Load(), p._cache, viewID, coreTx, p.cfg, p.senders, newTxs, p.protocolBaseFee.Load(), p.currentBaseFee.Load(), p.pending, p.baseFee, p.queued, p.byNonce, p.byHash, p.addLocked, p.discardLocked); err != nil {
		return nil, err
	}

	// notify about all non-dropped txs
	notifyNewTxs := make(Hashes, 0, 32*len(newTxs.txs))
	for i := range newTxs.txs {
		_, ok := p.byHash[string(newTxs.txs[i].idHash[:])]
		if !ok {
			continue
		}
		notifyNewTxs = append(notifyNewTxs, newTxs.txs[i].idHash[:]...)
	}
	if len(notifyNewTxs) > 0 {
		select {
		case p.newTxs <- notifyNewTxs:
		default:
		}
	}

	reasons := make([]DiscardReason, len(newTxs.txs))
	for i := range newTxs.txs {
		reason, ok := p.discardReasonsLRU.Get(string(newTxs.txs[i].idHash[:]))
		if ok {
			reasons[i] = reason.(DiscardReason)
		}
	}
	return reasons, nil
}
func (p *TxPool) coreDB() kv.RoDB {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p._coreDB
}

func (p *TxPool) cache() kvcache.Cache {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p._cache
}
func addTxs(blockNum uint64, cache kvcache.Cache, viewID kvcache.ViewID, coreTx kv.Tx, cfg Config, senders *sendersBatch, newTxs TxSlots, protocolBaseFee, currentBaseFee uint64, pending *PendingPool, baseFee, queued *SubPool, byNonce *ByNonce, byHash map[string]*metaTx, add func(*metaTx) bool, discard func(*metaTx, DiscardReason)) error {
	if ASSERT {
		for i := range newTxs.txs {
			if newTxs.txs[i].senderID == 0 {
				panic(fmt.Errorf("senderID can't be zero"))
			}
		}
	}
	//defer func(t time.Time) { fmt.Printf("pool.go:611: %s\n", time.Since(t)) }(time.Now())
	// This can be thought of a reverse operation from the one described before.
	// When a block that was deemed "the best" of its height, is no longer deemed "the best", the
	// transactions contained in it, are now viable for inclusion in other blocks, and therefore should
	// be returned into the transaction pool.
	// An interesting note here is that if the block contained any transactions local to the node,
	// by being first removed from the pool (from the "local" part of it), and then re-injected,
	// they effective lose their priority over the "remote" transactions. In order to prevent that,
	// somehow the fact that certain transactions were local, needs to be remembered for some
	// time (up to some "immutability threshold").
	changedSenders := unsafeAddToPendingPool(blockNum, newTxs, byHash, add)
	for id := range changedSenders {
		nonce, balance, err := senders.info(cache, viewID, coreTx, id)
		if err != nil {
			return err
		}
		onSenderChange(id, nonce, balance, byNonce, protocolBaseFee, currentBaseFee, pending, baseFee, queued)
	}

	//defer func(t time.Time) { fmt.Printf("pool.go:630: %s\n", time.Since(t)) }(time.Now())
	//pending.EnforceWorstInvariants()
	//baseFee.EnforceInvariants()
	//queued.EnforceInvariants()
	promote(pending, baseFee, queued, cfg, discard)
	//pending.EnforceWorstInvariants()
	pending.EnforceBestInvariants()

	return nil
}

func (p *TxPool) setBaseFee(baseFee uint64) (uint64, uint64) {
	if baseFee > 0 {
		p.protocolBaseFee.Store(calcProtocolBaseFee(baseFee))
		p.currentBaseFee.Store(baseFee)
	}
	return p.protocolBaseFee.Load(), p.currentBaseFee.Load()
}

func (p *TxPool) addLocked(mt *metaTx) bool {
	// Insert to pending pool, if pool doesn't have txn with same Nonce and bigger Tip
	found := p.byNonce.get(mt.Tx.senderID, mt.Tx.nonce)
	if found != nil {
		if mt.Tx.tip <= found.Tx.tip {
			return false
		}

		switch found.currentSubPool {
		case PendingSubPool:
			p.pending.UnsafeRemove(found)
		case BaseFeeSubPool:
			p.baseFee.UnsafeRemove(found)
		case QueuedSubPool:
			p.queued.UnsafeRemove(found)
		default:
			//already removed
		}

		p.discardLocked(found, ReplacedByHigherTip)
	}

	p.byHash[string(mt.Tx.idHash[:])] = mt

	if replaced := p.byNonce.replaceOrInsert(mt); replaced != nil {
		if ASSERT {
			panic("must neve happen")
		}
	}

	if mt.subPool&IsLocal != 0 {
		p.isLocalLRU.Add(string(mt.Tx.idHash[:]), struct{}{})
	}
	p.pending.Add(mt)
	return true
}
func (p *TxPool) discardLocked(mt *metaTx, reason DiscardReason) {
	delete(p.byHash, string(mt.Tx.idHash[:]))
	p.deletedTxs = append(p.deletedTxs, mt)
	p.byNonce.delete(mt)
	p.discardReasonsLRU.Add(string(mt.Tx.idHash[:]), reason)
}

// removeMined - apply new highest block (or batch of blocks)
//
// 1. New best block arrives, which potentially changes the balance and the nonce of some senders.
// We use senderIds data structure to find relevant senderId values, and then use senders data structure to
// modify state_balance and state_nonce, potentially remove some elements (if transaction with some nonce is
// included into a block), and finally, walk over the transaction records and update SubPool fields depending on
// the actual presence of nonce gaps and what the balance is.
func removeMined(byNonce *ByNonce, minedTxs []*TxSlot, pending *PendingPool, baseFee, queued *SubPool, discard func(*metaTx, DiscardReason)) error {
	noncesToRemove := map[uint64]uint64{}
	for _, txn := range minedTxs {
		nonce, ok := noncesToRemove[txn.senderID]
		if !ok || txn.nonce > nonce {
			noncesToRemove[txn.senderID] = txn.nonce
		}
	}

	var toDel []*metaTx // can't delete items while iterate them
	for senderID, nonce := range noncesToRemove {
		//if sender.byNonce.Len() > 0 {
		//log.Debug("[txpool] removing mined", "senderID", tx.senderID, "sender.byNonce.len()", sender.byNonce.Len())
		//}
		// delete mined transactions from everywhere
		byNonce.ascend(senderID, func(mt *metaTx) bool {
			//log.Debug("[txpool] removing mined, cmp nonces", "tx.nonce", it.metaTx.Tx.nonce, "sender.nonce", sender.nonce)
			if mt.Tx.nonce > nonce {
				return false
			}
			toDel = append(toDel, mt)
			// del from sub-pool
			switch mt.currentSubPool {
			case PendingSubPool:
				pending.UnsafeRemove(mt)
			case BaseFeeSubPool:
				baseFee.UnsafeRemove(mt)
			case QueuedSubPool:
				queued.UnsafeRemove(mt)
			default:
				//already removed
			}
			return true
		})

		for i := range toDel {
			discard(toDel[i], Mined)
		}
		toDel = toDel[:0]
	}
	return nil
}

// unwind
func unsafeAddToPendingPool(blockNum uint64, newTxs TxSlots, byHash map[string]*metaTx, add func(*metaTx) bool) (changedSenders map[uint64]struct{}) {
	changedSenders = map[uint64]struct{}{}
	for i, txn := range newTxs.txs {
		if _, ok := byHash[string(txn.idHash[:])]; ok {
			continue
		}
		mt := newMetaTx(txn, newTxs.isLocal[i], blockNum)

		if add(mt) {
			changedSenders[txn.senderID] = struct{}{}
		}
	}
	return changedSenders
}

func onSenderChange(senderID uint64, senderNonce uint64, senderBalance uint256.Int, byNonce *ByNonce, protocolBaseFee, currentBaseFee uint64, pending *PendingPool, baseFee, queued *SubPool) {
	noGapsNonce := senderNonce
	cumulativeRequiredBalance := uint256.NewInt(0)
	minFeeCap := uint64(math.MaxUint64)
	minTip := uint64(math.MaxUint64)
	byNonce.ascend(senderID, func(mt *metaTx) bool {
		// Sender has enough balance for: gasLimit x feeCap + transferred_value
		needBalance := uint256.NewInt(mt.Tx.gas)
		needBalance.Mul(needBalance, uint256.NewInt(mt.Tx.feeCap))
		needBalance.Add(needBalance, &mt.Tx.value)
		minFeeCap = min(minFeeCap, mt.Tx.feeCap)
		minTip = min(minTip, mt.Tx.tip)
		if currentBaseFee >= minFeeCap {
			mt.effectiveTip = minTip
		} else {
			mt.effectiveTip = minFeeCap - currentBaseFee
		}
		// 1. Minimum fee requirement. Set to 1 if feeCap of the transaction is no less than in-protocol
		// parameter of minimal base fee. Set to 0 if feeCap is less than minimum base fee, which means
		// this transaction will never be included into this particular chain.
		mt.subPool &^= EnoughFeeCapProtocol
		if mt.Tx.feeCap >= protocolBaseFee {
			mt.subPool |= EnoughFeeCapProtocol
		} else {
			mt.subPool = 0 // TODO: we immediately drop all transactions if they have no first bit - then maybe we don't need this bit at all? And don't add such transactions to queue?
			return true
		}

		// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for
		// the sender is M, and there are transactions for all nonces between M and N from the same
		// sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
		mt.subPool &^= NoNonceGaps
		if noGapsNonce == mt.Tx.nonce {
			mt.subPool |= NoNonceGaps
			noGapsNonce++
		}

		// 3. Sufficient balance for gas. Set to 1 if the balance of sender's account in the
		// state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the
		// sum of feeCap x gasLimit + transferred_value of all transactions from this sender with
		// nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is
		// set if there is currently a guarantee that the transaction and all its required prior
		// transactions will be able to pay for gas.
		mt.subPool &^= EnoughBalance
		if mt.Tx.nonce >= senderNonce {
			cumulativeRequiredBalance = cumulativeRequiredBalance.Add(cumulativeRequiredBalance, needBalance) // already deleted all transactions with nonce <= sender.nonce
			if senderBalance.Gt(cumulativeRequiredBalance) || senderBalance.Eq(cumulativeRequiredBalance) {
				mt.subPool |= EnoughBalance
			}
		}

		// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than
		// baseFee of the currently pending block. Set to 0 otherwise.
		mt.subPool &^= EnoughFeeCapBlock
		if mt.Tx.feeCap >= currentBaseFee {
			mt.subPool |= EnoughFeeCapBlock
		}

		// 5. Local transaction. Set to 1 if transaction is local.
		// can't change

		switch mt.currentSubPool {
		case PendingSubPool:
			pending.Updated(mt)
		case BaseFeeSubPool:
			baseFee.Updated(mt)
		case QueuedSubPool:
			queued.Updated(mt)
		}
		return true
	})
}

func promote(pending *PendingPool, baseFee, queued *SubPool, cfg Config, discard func(*metaTx, DiscardReason)) {
	//1. If top element in the worst green queue has subPool != 0b1111 (binary), it needs to be removed from the green pool.
	//   If subPool < 0b1000 (not satisfying minimum fee), discard.
	//   If subPool == 0b1110, demote to the yellow pool, otherwise demote to the red pool.
	for worst := pending.Worst(); pending.Len() > 0; worst = pending.Worst() {
		if worst.subPool >= 0b11110 {
			break
		}
		if worst.subPool >= 0b11100 {
			baseFee.Add(pending.PopWorst())
			continue
		}
		if worst.subPool >= 0b10000 {
			queued.Add(pending.PopWorst())
			continue
		}
		discard(pending.PopWorst(), FeeTooLow)
	}

	//2. If top element in the worst green queue has subPool == 0b1111, but there is not enough room in the pool, discard.
	for worst := pending.Worst(); pending.Len() > cfg.PendingSubPoolLimit; worst = pending.Worst() {
		if worst.subPool >= 0b11111 { // TODO: here must 'subPool == 0b1111' or 'subPool <= 0b1111' ?
			break
		}
		discard(pending.PopWorst(), PendingPoolOverflow)
	}

	//3. If the top element in the best yellow queue has subPool == 0b1111, promote to the green pool.
	for best := baseFee.Best(); baseFee.Len() > 0; best = baseFee.Best() {
		if best.subPool < 0b11110 {
			break
		}
		pending.Add(baseFee.PopBest())
	}

	//4. If the top element in the worst yellow queue has subPool != 0x1110, it needs to be removed from the yellow pool.
	//   If subPool < 0b1000 (not satisfying minimum fee), discard. Otherwise, demote to the red pool.
	for worst := baseFee.Worst(); baseFee.Len() > 0; worst = baseFee.Worst() {
		if worst.subPool >= 0b11100 {
			break
		}
		if worst.subPool >= 0b10000 {
			queued.Add(baseFee.PopWorst())
			continue
		}
		discard(baseFee.PopWorst(), FeeTooLow)
	}

	//5. If the top element in the worst yellow queue has subPool == 0x1110, but there is not enough room in the pool, discard.
	for worst := baseFee.Worst(); baseFee.Len() > cfg.BaseFeeSubPoolLimit; worst = baseFee.Worst() {
		if worst.subPool >= 0b11110 {
			break
		}
		discard(baseFee.PopWorst(), BaseFeePoolOverflow)
	}

	//6. If the top element in the best red queue has subPool == 0x1110, promote to the yellow pool. If subPool == 0x1111, promote to the green pool.
	for best := queued.Best(); queued.Len() > 0; best = queued.Best() {
		if best.subPool < 0b11100 {
			break
		}
		if best.subPool < 0b11110 {
			baseFee.Add(queued.PopBest())
			continue
		}

		pending.Add(queued.PopBest())
	}

	//7. If the top element in the worst red queue has subPool < 0b1000 (not satisfying minimum fee), discard.
	for worst := queued.Worst(); queued.Len() > 0; worst = queued.Worst() {
		if worst.subPool >= 0b10000 {
			break
		}

		discard(queued.PopWorst(), FeeTooLow)
	}

	//8. If the top element in the worst red queue has subPool >= 0b100, but there is not enough room in the pool, discard.
	for _ = queued.Worst(); queued.Len() > cfg.QueuedSubPoolLimit; _ = queued.Worst() {
		discard(queued.PopWorst(), QueuedPoolOverflow)
	}
}

// MainLoop - does:
// send pending byHash to p2p:
//      - new byHash
//      - all pooled byHash to recently connected peers
//      - all local pooled byHash to random peers periodically
// promote/demote transactions
// reorgs
func MainLoop(ctx context.Context, db kv.RwDB, coreDB kv.RoDB, p *TxPool, newTxs chan Hashes, send *Send, newSlotsStreams *NewSlotsStreams, notifyMiningAboutNewSlots func()) {
	syncToNewPeersEvery := time.NewTicker(p.cfg.SyncToNewPeersEvery)
	defer syncToNewPeersEvery.Stop()
	processRemoteTxsEvery := time.NewTicker(p.cfg.ProcessRemoteTxsEvery)
	defer processRemoteTxsEvery.Stop()
	commitEvery := time.NewTicker(p.cfg.CommitEvery)
	defer commitEvery.Stop()
	logEvery := time.NewTicker(p.cfg.LogEvery)
	defer logEvery.Stop()

	localTxHashes := make([]byte, 0, 128)
	remoteTxHashes := make([]byte, 0, 128)

	for {
		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:
			p.logStats()
		case <-processRemoteTxsEvery.C:
			if !p.Started() {
				continue
			}
			if err := p.processRemoteTxs(ctx); err != nil {
				if s, ok := status.FromError(err); ok && retryLater(s.Code()) {
					continue
				}
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					continue
				}
				log.Error("[txpool] process batch remote txs", "err", err)
			}
		case <-commitEvery.C:
			if db != nil {
				t := time.Now()
				written, err := p.flush(db)
				if err != nil {
					log.Error("[txpool] flush is local history", "err", err)
					continue
				}
				writeToDbBytesCounter.Set(written)
				log.Debug("[txpool] Commit", "written_kb", written/1024, "in", time.Since(t))
			}
		case h := <-newTxs:
			t := time.Now()
			notifyMiningAboutNewSlots()
			if err := db.View(ctx, func(tx kv.Tx) error {
				slotsRlp := make([][]byte, 0, h.Len())
				for i := 0; i < h.Len(); i++ {
					slotRlp, err := p.GetRlp(tx, h.At(i))
					if err != nil {
						return err
					}
					slotsRlp = append(slotsRlp, slotRlp)
				}
				newSlotsStreams.Broadcast(&proto_txpool.OnAddReply{RplTxs: slotsRlp})
				return nil
			}); err != nil {
				log.Error("[txpool] send new slots by grpc", "err", err)
			}

			// first broadcast all local txs to all peers, then non-local to random sqrt(peersAmount) peers
			localTxHashes = localTxHashes[:0]
			remoteTxHashes = remoteTxHashes[:0]

			for i := 0; i < h.Len(); i++ {
				if p.IsLocal(h.At(i)) {
					localTxHashes = append(localTxHashes, h.At(i)...)
				} else {
					remoteTxHashes = append(localTxHashes, h.At(i)...)
				}
			}

			send.BroadcastLocalPooledTxs(localTxHashes)
			send.BroadcastRemotePooledTxs(remoteTxHashes)
			propagateNewTxsTimer.UpdateDuration(t)
		case <-syncToNewPeersEvery.C: // new peer
			newPeers := p.recentlyConnectedPeers.GetAndClean()
			if len(newPeers) == 0 {
				continue
			}
			t := time.Now()
			remoteTxHashes = p.AppendAllHashes(remoteTxHashes[:0])
			send.PropagatePooledTxsToPeersList(newPeers, remoteTxHashes)
			propagateToNewPeerTimer.UpdateDuration(t)
		}
	}
}

func (p *TxPool) flush(db kv.RwDB) (written uint64, err error) {
	defer writeToDbTimer.UpdateDuration(time.Now())
	p.lock.Lock()
	defer p.lock.Unlock()
	//it's important that write db tx is done inside lock, to make last writes visible for all read operations
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		err = p.flushLocked(tx)
		if err != nil {
			return err
		}
		written, _, err = tx.(*mdbx.MdbxTx).SpaceDirty()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return written, nil
}
func (p *TxPool) flushLocked(tx kv.RwTx) (err error) {
	for i := 0; i < len(p.deletedTxs); i++ {
		if !p.byNonce.hasTxs(p.deletedTxs[i].Tx.senderID) {
			addr, ok := p.senders.senderID2Addr[p.deletedTxs[i].Tx.senderID]
			if ok {
				delete(p.senders.senderID2Addr, p.deletedTxs[i].Tx.senderID)
				delete(p.senders.senderIDs, addr)
			}
		}
		if err := tx.Delete(kv.PoolTransaction, p.deletedTxs[i].Tx.idHash[:], nil); err != nil {
			return err
		}
		p.deletedTxs[i] = nil // for gc
	}

	txHashes := p.isLocalLRU.Keys()
	encID := make([]byte, 8)
	if err := tx.ClearBucket(kv.RecentLocalTransaction); err != nil {
		return err
	}
	for i := range txHashes {
		binary.BigEndian.PutUint64(encID, uint64(i))
		if err := tx.Append(kv.RecentLocalTransaction, encID, []byte(txHashes[i].(string))); err != nil {
			return err
		}
	}

	v := make([]byte, 0, 1024)
	for txHash, metaTx := range p.byHash {
		if metaTx.Tx.rlp == nil {
			continue
		}
		v = common.EnsureEnoughSize(v, 20+len(metaTx.Tx.rlp))
		for addr, id := range p.senders.senderIDs { // no inverted index - tradeoff flush speed for memory usage
			if id == metaTx.Tx.senderID {
				copy(v[:20], addr)
				break
			}
		}
		copy(v[20:], metaTx.Tx.rlp)

		has, _ := tx.Has(kv.PoolTransaction, []byte(txHash))
		if has {
			panic("must not happen")
		}
		if err := tx.Put(kv.PoolTransaction, []byte(txHash), v); err != nil {
			return err
		}
		metaTx.Tx.rlp = nil
	}

	binary.BigEndian.PutUint64(encID, p.protocolBaseFee.Load())
	if err := tx.Put(kv.PoolInfo, PoolProtocolBaseFeeKey, encID); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(encID, p.currentBaseFee.Load())
	if err := tx.Put(kv.PoolInfo, PoolPendingBaseFeeKey, encID); err != nil {
		return err
	}

	// clean - in-memory data structure as later as possible - because if during this Tx will happen error,
	// DB will stay consitant but some in-memory structures may be alread cleaned, and retry will not work
	// failed write transaction must not create side-effects
	p.deletedTxs = p.deletedTxs[:0]
	return nil
}

func (p *TxPool) fromDB(ctx context.Context, tx kv.Tx, coreTx kv.Tx) error {
	lastSeenBlock, err := LastSeenBlock(tx)
	if err != nil {
		return err
	}
	p.lastSeenBlock.Store(lastSeenBlock)

	viewID, err := p._cache.View(ctx, coreTx)
	if err != nil {
		return err
	}

	if err := tx.ForEach(kv.RecentLocalTransaction, nil, func(k, v []byte) error {
		p.isLocalLRU.Add(string(v), struct{}{})
		return nil
	}); err != nil {
		return err
	}

	txs := TxSlots{}
	parseCtx := NewTxParseContext(p.rules, p.chainID)
	parseCtx.WithSender(false)

	i := 0
	if err := tx.ForEach(kv.PoolTransaction, nil, func(k, v []byte) error {
		addr, txRlp := v[:20], v[20:]
		txs.Resize(uint(i + 1))
		txs.txs[i] = &TxSlot{}

		_, err := parseCtx.ParseTransaction(txRlp, 0, txs.txs[i], nil)
		if err != nil {
			return fmt.Errorf("err: %w, rlp: %x\n", err, txRlp)
		}
		txs.txs[i].rlp = nil // means that we don't need store it in db anymore
		copy(txs.senders.At(i), addr)

		id, ok := p.senders.senderIDs[string(addr)]
		if !ok {
			p.senders.senderID++
			id = p.senders.senderID
			p.senders.senderIDs[string(addr)] = id
			p.senders.senderID2Addr[id] = string(addr)
		}
		txs.txs[i].senderID = id
		binary.BigEndian.Uint64(v)

		isLocalTx := p.isLocalLRU.Contains(string(k))
		txs.isLocal[i] = isLocalTx
		i++
		return nil
	}); err != nil {
		return err
	}

	var protocolBaseFee, currentBaseFee uint64
	{
		v, err := tx.GetOne(kv.PoolInfo, PoolProtocolBaseFeeKey)
		if err != nil {
			return err
		}
		if len(v) > 0 {
			protocolBaseFee = binary.BigEndian.Uint64(v)
		}
	}
	{
		v, err := tx.GetOne(kv.PoolInfo, PoolPendingBaseFeeKey)
		if err != nil {
			return err
		}
		if len(v) > 0 {
			currentBaseFee = binary.BigEndian.Uint64(v)
		}
	}
	err = p.senders.onNewTxs(txs)
	if err != nil {
		return err
	}
	if err := addTxs(0, p._cache, viewID, coreTx, p.cfg, p.senders, txs, protocolBaseFee, currentBaseFee, p.pending, p.baseFee, p.queued, p.byNonce, p.byHash, p.addLocked, p.discardLocked); err != nil {
		return err
	}
	p.currentBaseFee.Store(currentBaseFee)
	p.protocolBaseFee.Store(protocolBaseFee)

	return nil
}
func LastSeenBlock(tx kv.Getter) (uint64, error) {
	v, err := tx.GetOne(kv.PoolInfo, PoolLastSeenBlockKey)
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(v), nil
}
func PutLastSeenBlock(tx kv.Putter, n uint64, buf []byte) error {
	buf = common.EnsureEnoughSize(buf, 8)
	binary.BigEndian.PutUint64(buf, n)
	err := tx.Put(kv.PoolInfo, PoolLastSeenBlockKey, buf)
	if err != nil {
		return err
	}
	return nil
}
func ChainConfig(tx kv.Getter) (*chain.Config, error) {
	v, err := tx.GetOne(kv.PoolInfo, PoolChainConfigKey)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	var config chain.Config
	if err := json.Unmarshal(v, &config); err != nil {
		return nil, fmt.Errorf("invalid chain config JSON in pool db: %w", err)
	}
	return &config, nil
}
func PutChainConfig(tx kv.Putter, cc *chain.Config, buf []byte) error {
	wr := bytes.NewBuffer(buf)
	if err := json.NewEncoder(wr).Encode(cc); err != nil {
		return fmt.Errorf("invalid chain config JSON in pool db: %w", err)
	}
	if err := tx.Put(kv.PoolInfo, PoolChainConfigKey, wr.Bytes()); err != nil {
		return err
	}
	return nil
}

//nolint
func (p *TxPool) printDebug(prefix string) {
	fmt.Printf("%s.pool.byHash\n", prefix)
	for _, j := range p.byHash {
		fmt.Printf("\tsenderID=%d, nonce=%d, tip=%d\n", j.Tx.senderID, j.Tx.nonce, j.Tx.tip)
	}
	fmt.Printf("%s.pool.queues.len: %d,%d,%d\n", prefix, p.pending.Len(), p.baseFee.Len(), p.queued.Len())
	for i := range p.pending.best {
		p.pending.best[i].Tx.printDebug(fmt.Sprintf("%s.pending: %b", prefix, p.pending.best[i].subPool))
	}
	for i := range *p.queued.best {
		(*p.queued.best)[i].Tx.printDebug(fmt.Sprintf("%s.queued : %b", prefix, (*p.queued.best)[i].subPool))
	}
}
func (p *TxPool) logStats() {
	//protocolBaseFee, currentBaseFee := p.protocolBaseFee.Load(), p.currentBaseFee.Load()

	p.lock.RLock()
	defer p.lock.RUnlock()

	//idsInMem := p.senders.idsCount()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ctx := []interface{}{
		//"baseFee", fmt.Sprintf("%d, %dm", protocolBaseFee, currentBaseFee/1_000_000),
		"block", p.lastSeenBlock.Load(),
		"pending", p.pending.Len(),
		"baseFee", p.baseFee.Len(),
		"queued", p.queued.Len(),
	}
	cacheKeys := p._cache.Len()
	if cacheKeys > 0 {
		ctx = append(ctx, "cache_keys", cacheKeys)
	}
	ctx = append(ctx, "alloc_mb", m.Alloc/1024/1024, "sys_mb", m.Sys/1024/1024)
	log.Info("[txpool] stat", ctx...)
	//if ASSERT {
	//stats := kvcache.DebugStats(p.senders.cache)
	//log.Info(fmt.Sprintf("[txpool] cache %T, roots amount %d", p.senders.cache, len(stats)))
	//for i := range stats {
	//	log.Info("[txpool] cache", "root", stats[i].BlockNum, "len", stats[i].Lenght)
	//}
	//stats := kvcache.DebugStats(p.senders.cache)
	//log.Info(fmt.Sprintf("[txpool] cache %T, roots amount %d", p.senders.cache, len(stats)))
	//for i := range stats {
	//	log.Info("[txpool] cache", "root", stats[i].BlockNum, "len", stats[i].Lenght)
	//}
	//ages := kvcache.DebugAges(p.senders.cache)
	//for i := range ages {
	//	log.Info("[txpool] age", "age", ages[i].BlockNum, "amount", ages[i].Lenght)
	//}
	//}
}

//Deprecated need switch to streaming-like
func (p *TxPool) deprecatedForEach(_ context.Context, f func(rlp, sender []byte, t SubPoolType), tx kv.Tx) error {
	p.lock.RLock()
	defer p.lock.RUnlock()

	p.byNonce.tree.Ascend(func(i btree.Item) bool {
		mt := i.(*sortByNonce).metaTx
		slot := mt.Tx
		slotRlp := slot.rlp
		if slot.rlp == nil {
			v, err := tx.GetOne(kv.PoolTransaction, slot.idHash[:])
			if err != nil {
				log.Error("[txpool] get tx from db", "err", err)
				return false
			}
			if v == nil {
				log.Error("[txpool] tx not found in db")
				return false
			}
			slotRlp = v[20:]
		}

		var sender []byte
		found := false
		for addr, senderID := range p.senders.senderIDs { // TODO: do we need inverted index here?
			if slot.senderID == senderID {
				sender = []byte(addr)
				found = true
				break
			}
		}
		if !found {
			return true
		}
		f(slotRlp, sender, mt.currentSubPool)
		return true
	})
	return nil
}

var PoolChainConfigKey = []byte("pending_chain_config")
var PoolLastSeenBlockKey = []byte("pending_last_seen_block")
var PoolPendingBaseFeeKey = []byte("pending_base_fee")
var PoolProtocolBaseFeeKey = []byte("protocol_base_fee")

// recentlyConnectedPeers does buffer IDs of recently connected good peers
// then sync of pooled Transaction can happen to all of then at once
// DoS protection and performance saving
// it doesn't track if peer disconnected, it's fine
type recentlyConnectedPeers struct {
	lock  sync.RWMutex
	peers []PeerID
}

func (l *recentlyConnectedPeers) AddPeer(p PeerID) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.peers = append(l.peers, p)
}

func (l *recentlyConnectedPeers) GetAndClean() []PeerID {
	l.lock.Lock()
	defer l.lock.Unlock()
	peers := l.peers
	l.peers = nil
	return peers
}

//nolint
func (sc *sendersBatch) printDebug(prefix string) {
	fmt.Printf("%s.sendersBatch.sender\n", prefix)
	//for i, j := range sc.senderInfo {
	//	fmt.Printf("\tid=%d,nonce=%d,balance=%d\n", i, j.nonce, j.balance.Uint64())
	//}
}

// sendersBatch stores in-memory senders-related objects - which are different from DB (updated/dirty)
// flushing to db periodicaly. it doesn't play as read-cache (because db is small and memory-mapped - doesn't need cache)
// non thread-safe
type sendersBatch struct {
	senderID      uint64
	senderIDs     map[string]uint64
	senderID2Addr map[uint64]string
}

func newSendersCache() *sendersBatch {
	return &sendersBatch{senderIDs: map[string]uint64{}, senderID2Addr: map[uint64]string{}}
}

func (sc *sendersBatch) id(addr string) (uint64, bool) {
	id, ok := sc.senderIDs[addr]
	return id, ok
}
func (sc *sendersBatch) info(cache kvcache.Cache, viewID kvcache.ViewID, coreTx kv.Tx, id uint64) (nonce uint64, balance uint256.Int, err error) {
	//cacheTotalCounter.Inc()
	addr, ok := sc.senderID2Addr[id]
	if !ok {
		panic("must not happen")
	}
	encoded, err := cache.Get([]byte(addr), coreTx, viewID)
	if err != nil {
		return 0, emptySender.balance, err
	}
	if len(encoded) == 0 {
		return emptySender.nonce, emptySender.balance, nil
	}
	nonce, balance, err = DecodeSender(encoded)
	if err != nil {
		return 0, emptySender.balance, err
	}
	return nonce, balance, nil
}

func (sc *sendersBatch) onNewTxs(newTxs TxSlots) (err error) {
	for i := 0; i < len(newTxs.txs); i++ {
		id, ok := sc.id(string(newTxs.senders.At(i)))
		if ok {
			newTxs.txs[i].senderID = id
			continue
		}
		sc.senderID++
		sc.senderIDs[string(newTxs.senders.At(i))] = sc.senderID
		sc.senderID2Addr[sc.senderID] = string(newTxs.senders.At(i))

		newTxs.txs[i].senderID = sc.senderID
	}
	return nil
}
func (sc *sendersBatch) onNewBlock(stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs TxSlots) error {
	for _, diff := range stateChanges.ChangeBatch {
		for _, change := range diff.Changes { // merge state changes
			addrB := gointerfaces.ConvertH160toAddress(change.Address)
			addr := string(addrB[:])
			_, ok := sc.id(addr)
			if !ok {
				sc.senderID++
				sc.senderIDs[addr] = sc.senderID
				sc.senderID2Addr[sc.senderID] = addr
			}
		}

		for i := 0; i < unwindTxs.senders.Len(); i++ {
			addr := string(unwindTxs.senders.At(i))
			id, ok := sc.id(addr)
			if !ok {
				sc.senderID++
				id = sc.senderID
				sc.senderIDs[addr] = sc.senderID
				sc.senderID2Addr[sc.senderID] = addr
			}
			unwindTxs.txs[i].senderID = id
		}

		for i := 0; i < len(minedTxs.txs); i++ {
			addr := string(minedTxs.senders.At(i))
			id, ok := sc.id(addr)
			if !ok {
				sc.senderID++
				id = sc.senderID
				sc.senderIDs[addr] = sc.senderID
				sc.senderID2Addr[sc.senderID] = addr
			}
			minedTxs.txs[i].senderID = id
		}
	}
	return nil
}

type ByNonce struct {
	tree *btree.BTree
}

func (b *ByNonce) ascend(senderID uint64, f func(*metaTx) bool) {
	b.tree.AscendGreaterOrEqual(&sortByNonce{&metaTx{Tx: &TxSlot{senderID: senderID}}}, func(i btree.Item) bool {
		mt := i.(*sortByNonce).metaTx
		if mt.Tx.senderID != senderID {
			return false
		}
		return f(mt)
	})
}
func (b *ByNonce) hasTxs(senderID uint64) bool {
	has := false
	b.ascend(senderID, func(*metaTx) bool {
		has = true
		return false
	})
	return has
}
func (b *ByNonce) get(senderID, txNonce uint64) *metaTx {
	if found := b.tree.Get(&sortByNonce{&metaTx{Tx: &TxSlot{senderID: senderID, nonce: txNonce}}}); found != nil {
		return found.(*sortByNonce).metaTx
	}
	return nil
}

//nolint
func (b *ByNonce) has(mt *metaTx) bool {
	found := b.tree.Get(&sortByNonce{mt})
	return found != nil
}
func (b *ByNonce) delete(mt *metaTx) { b.tree.Delete(&sortByNonce{mt}) }
func (b *ByNonce) replaceOrInsert(mt *metaTx) *metaTx {
	it := b.tree.ReplaceOrInsert(&sortByNonce{mt})
	if it != nil {
		return it.(*sortByNonce).metaTx
	}
	return nil
}

type PendingPool struct {
	t     SubPoolType
	best  bestSlice
	worst *WorstQueue
}

func NewPendingSubPool(t SubPoolType) *PendingPool {
	return &PendingPool{t: t, best: []*metaTx{}, worst: &WorstQueue{}}
}

// bestSlice - is similar to best queue, but with O(n log n) complexity and
// it maintains element.bestIndex field
type bestSlice []*metaTx

func (s bestSlice) Len() int { return len(s) }
func (s bestSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
	s[i].bestIndex, s[j].bestIndex = i, j
}
func (s bestSlice) Less(i, j int) bool { return !s[i].Less(s[j]) }
func (s bestSlice) UnsafeRemove(i *metaTx) bestSlice {
	s.Swap(i.bestIndex, len(s)-1)
	s[len(s)-1].bestIndex = -1
	s[len(s)-1] = nil
	return s[:len(s)-1]
}
func (s bestSlice) UnsafeAdd(i *metaTx) bestSlice {
	a := append(s, i)
	i.bestIndex = len(s)
	return a
}

func (p *PendingPool) EnforceWorstInvariants() {
	heap.Init(p.worst)
}
func (p *PendingPool) EnforceBestInvariants() {
	sort.Sort(p.best)
}

func (p *PendingPool) Best() *metaTx {
	if len(p.best) == 0 {
		return nil
	}
	return p.best[0]
}
func (p *PendingPool) Worst() *metaTx {
	if len(*p.worst) == 0 {
		return nil
	}
	return (*p.worst)[0]
}
func (p *PendingPool) PopWorst() *metaTx {
	i := heap.Pop(p.worst).(*metaTx)
	p.best = p.best.UnsafeRemove(i)
	return i
}
func (p *PendingPool) Updated(mt *metaTx) {
	heap.Fix(p.worst, mt.worstIndex)
}
func (p *PendingPool) Len() int { return len(p.best) }

// UnsafeRemove - does break Heap invariants, but it has O(1) instead of O(log(n)) complexity.
// Must manually call heap.Init after such changes.
// Make sense to batch unsafe changes
func (p *PendingPool) UnsafeRemove(i *metaTx) {
	if p.Len() == 0 {
		return
	}
	if p.Len() == 1 && i.bestIndex == 0 {
		p.worst.Pop()
		p.best = p.best.UnsafeRemove(i)
		return
	}
	// manually call funcs instead of heap.Pop
	p.worst.Swap(i.worstIndex, p.worst.Len()-1)
	p.worst.Pop()
	p.best.Swap(i.bestIndex, p.best.Len()-1)
	p.best = p.best.UnsafeRemove(i)
}
func (p *PendingPool) UnsafeAdd(i *metaTx) {
	i.currentSubPool = p.t
	p.worst.Push(i)
	p.best = p.best.UnsafeAdd(i)
}
func (p *PendingPool) Add(i *metaTx) {
	i.currentSubPool = p.t
	heap.Push(p.worst, i)
	p.best = p.best.UnsafeAdd(i)
}
func (p *PendingPool) DebugPrint(prefix string) {
	for i, it := range p.best {
		fmt.Printf("%s.best: %d, %d, %d,%d\n", prefix, i, it.subPool, it.bestIndex, it.Tx.nonce)
	}
	for i, it := range *p.worst {
		fmt.Printf("%s.worst: %d, %d, %d,%d\n", prefix, i, it.subPool, it.worstIndex, it.Tx.nonce)
	}
}

type SubPool struct {
	t     SubPoolType
	best  *BestQueue
	worst *WorstQueue
}

func NewSubPool(t SubPoolType) *SubPool {
	return &SubPool{t: t, best: &BestQueue{}, worst: &WorstQueue{}}
}

func (p *SubPool) EnforceInvariants() {
	heap.Init(p.worst)
	heap.Init(p.best)
}
func (p *SubPool) Best() *metaTx {
	if len(*p.best) == 0 {
		return nil
	}
	return (*p.best)[0]
}
func (p *SubPool) Worst() *metaTx {
	if len(*p.worst) == 0 {
		return nil
	}
	return (*p.worst)[0]
}
func (p *SubPool) PopBest() *metaTx {
	i := heap.Pop(p.best).(*metaTx)
	heap.Remove(p.worst, i.worstIndex)
	return i
}
func (p *SubPool) PopWorst() *metaTx {
	i := heap.Pop(p.worst).(*metaTx)
	heap.Remove(p.best, i.bestIndex)
	return i
}
func (p *SubPool) Len() int { return p.best.Len() }
func (p *SubPool) Add(i *metaTx) {
	i.currentSubPool = p.t
	heap.Push(p.best, i)
	heap.Push(p.worst, i)
}

func (p *SubPool) Remove(i *metaTx) {
	heap.Remove(p.best, i.bestIndex)
	heap.Remove(p.worst, i.worstIndex)
	i.currentSubPool = 0
}
func (p *SubPool) Updated(i *metaTx) {
	heap.Fix(p.best, i.bestIndex)
	heap.Fix(p.worst, i.worstIndex)
}

// UnsafeRemove - does break Heap invariants, but it has O(1) instead of O(log(n)) complexity.
// Must manually call heap.Init after such changes.
// Make sense to batch unsafe changes
func (p *SubPool) UnsafeRemove(i *metaTx) {
	if p.Len() == 0 {
		return
	}
	if p.Len() == 1 && i.bestIndex == 0 {
		p.worst.Pop()
		p.best.Pop()
		return
	}
	// manually call funcs instead of heap.Pop
	p.worst.Swap(i.worstIndex, p.worst.Len()-1)
	p.worst.Pop()
	p.best.Swap(i.bestIndex, p.best.Len()-1)
	p.best.Pop()
}
func (p *SubPool) UnsafeAdd(i *metaTx) {
	i.currentSubPool = p.t
	p.worst.Push(i)
	p.best.Push(i)
}
func (p *SubPool) DebugPrint(prefix string) {
	for i, it := range *p.best {
		fmt.Printf("%s.best: %d, %d, %d\n", prefix, i, it.subPool, it.bestIndex)
	}
	for i, it := range *p.worst {
		fmt.Printf("%s.worst: %d, %d, %d\n", prefix, i, it.subPool, it.worstIndex)
	}
}

type BestQueue []*metaTx

func (mt *metaTx) Less(than *metaTx) bool {
	if mt.subPool != than.subPool {
		return mt.subPool < than.subPool
	}

	if mt.effectiveTip != than.effectiveTip {
		return mt.effectiveTip < than.effectiveTip
	}

	if mt.Tx.nonce != than.Tx.nonce {
		return mt.Tx.nonce < than.Tx.nonce
	}
	if mt.timestamp != than.timestamp {
		return mt.timestamp < than.timestamp
	}
	return false
}

func (p BestQueue) Len() int           { return len(p) }
func (p BestQueue) Less(i, j int) bool { return !p[i].Less(p[j]) } // We want Pop to give us the highest, not lowest, priority so we use !less here.
func (p BestQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].bestIndex = i
	p[j].bestIndex = j
}
func (p *BestQueue) Push(x interface{}) {
	n := len(*p)
	item := x.(*metaTx)
	item.bestIndex = n
	*p = append(*p, item)
}

func (p *BestQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.bestIndex = -1     // for safety
	item.currentSubPool = 0 // for safety
	*p = old[0 : n-1]
	return item
}

type WorstQueue []*metaTx

func (p WorstQueue) Len() int           { return len(p) }
func (p WorstQueue) Less(i, j int) bool { return p[i].Less(p[j]) }
func (p WorstQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].worstIndex = i
	p[j].worstIndex = j
}
func (p *WorstQueue) Push(x interface{}) {
	n := len(*p)
	item := x.(*metaTx)
	item.worstIndex = n
	*p = append(*p, x.(*metaTx))
}
func (p *WorstQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.worstIndex = -1    // for safety
	item.currentSubPool = 0 // for safety
	*p = old[0 : n-1]
	return item
}

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}
