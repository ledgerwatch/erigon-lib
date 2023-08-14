package txpool

import (
	"context"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon-lib/types"
	"os"
	"pgregory.net/rapid"
	"testing"
)

// helper functions
func createDB(t *rapid.T) kv.RwDB {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "pool-stateful-*")

	if err != nil {
		t.Fatalf("Could not create tempdir for test database: %v", err)
	}

	return memdb.New(tempDir)
}

func createPoolDB(t *rapid.T) kv.RwDB {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "pool-stateful-*")

	if err != nil {
		t.Fatalf("Could not create tempdir for test database: %v", err)
	}

	return memdb.NewPoolDB(tempDir)
}

// Generators (aka Strategies)
var (
	addressGenerator = rapid.Custom(func(t *rapid.T) common.Address {
		bytes := rapid.SliceOfN(rapid.Byte(), 20, 20).Draw(t, "addressBytes")
		return common.BytesToAddress(bytes)
	})
)

// State machine initialization
type poolMachine struct {
	p           *TxPool
	newTxs      chan types.Announcements
	ctx         context.Context
	coreDB      kv.RwDB
	poolDB      kv.RwDB
	blockHeight uint64
}

func (m *poolMachine) Init(t *rapid.T) {
	newTxs := make(chan types.Announcements)
	testDb := createDB(t)
	cache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(newTxs, testDb, txpoolcfg.DefaultConfig, cache, uint256.Int{10000}, nil)

	if err != nil {
		t.Fatalf("Error initializing TxPool: %v", err)
	}

	ctx := context.Background()

	m.p = pool
	m.newTxs = newTxs
	m.ctx = ctx
	m.coreDB = testDb
	m.poolDB = createPoolDB(t)
	m.blockHeight = 0
}

// Invariants
func (m *poolMachine) Check(t *rapid.T) {
	// todo
}

// Test rules
func (m *poolMachine) AddLocalTxs(t *rapid.T) {
	tx, err := m.coreDB.BeginRo(m.ctx)
	if err != nil {
		t.Fatalf("Could not open coreDB transaction")
	}

	transactions := types.TxSlots{}
	m.p.AddLocalTxs(context.Background(), transactions, tx)
}

func (m *poolMachine) EmptyBlock(t *rapid.T) {
	tx, err := m.poolDB.BeginRo(m.ctx)
	if err != nil {
		t.Fatalf("Could not open coreDB transaction")
	}
	defer tx.Rollback()

	var (
		batch       remote.StateChangeBatch
		unwindTxs   types.TxSlots
		newTxs      types.TxSlots
		stateChange remote.StateChange
	)

	gasFee := m.drawGasFee(t)
	batch.PendingBlockBaseFee = gasFee

	stateChange.BlockHeight = m.blockHeight + 1
	batch.ChangeBatch = []*remote.StateChange{&stateChange}

	err = m.p.OnNewBlock(m.ctx, &batch, unwindTxs, newTxs, tx)
	if err != nil {
		t.Fatalf("Error calling OnNewBlock: %v", err)
	}

	m.blockHeight += 1

	actualGasFee := m.p.pendingBaseFee.Load()
	if actualGasFee != gasFee {
		t.Errorf("Pending base fee not set as expected: expected %d, got %d", gasFee, actualGasFee)
	}
}

func (m *poolMachine) drawGasFee(t *rapid.T) uint64 {
	return rapid.Uint64Range(9_000_000_000, 11_000_000_000).Draw(t, "GasFee")
}

// todo remaining rules

// Main test function
func TestTxPoolStateful(t *testing.T) {
	rapid.Check(t, rapid.Run[*poolMachine]())
}
