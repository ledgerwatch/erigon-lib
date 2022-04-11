// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package txpool

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/types"
)

// Ensure, that PoolMock does implement Pool.
// If this is not the case, regenerate this file with moq.
var _ Pool = &PoolMock{}

// PoolMock is a mock implementation of Pool.
//
// 	func TestSomethingThatUsesPool(t *testing.T) {
//
// 		// make and configure a mocked Pool
// 		mockedPool := &PoolMock{
// 			AddLocalTxsFunc: func(ctx context.Context, newTxs TxSlots) ([]DiscardReason, error) {
// 				panic("mock out the AddLocalTxs method")
// 			},
// 			AddNewGoodPeerFunc: func(peerID PeerID)  {
// 				panic("mock out the AddNewGoodPeer method")
// 			},
// 			AddRemoteTxsFunc: func(ctx context.Context, newTxs TxSlots)  {
// 				panic("mock out the AddRemoteTxs method")
// 			},
// 			GetRlpFunc: func(tx kv.Tx, hash []byte) ([]byte, error) {
// 				panic("mock out the GetRlp method")
// 			},
// 			IdHashKnownFunc: func(tx kv.Tx, hash []byte) (bool, error) {
// 				panic("mock out the IdHashKnown method")
// 			},
// 			OnNewBlockFunc: func(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs TxSlots, minedTxs TxSlots, tx kv.Tx) error {
// 				panic("mock out the OnNewBlock method")
// 			},
// 			StartedFunc: func() bool {
// 				panic("mock out the Started method")
// 			},
// 			ValidateSerializedTxnFunc: func(serializedTxn []byte) error {
// 				panic("mock out the ValidateSerializedTxn method")
// 			},
// 		}
//
// 		// use mockedPool in code that requires Pool
// 		// and then make assertions.
//
// 	}
type PoolMock struct {
	// AddLocalTxsFunc mocks the AddLocalTxs method.
	AddLocalTxsFunc func(ctx context.Context, newTxs types.TxSlots) ([]DiscardReason, error)

	// AddNewGoodPeerFunc mocks the AddNewGoodPeer method.
	AddNewGoodPeerFunc func(peerID types.PeerID)

	// AddRemoteTxsFunc mocks the AddRemoteTxs method.
	AddRemoteTxsFunc func(ctx context.Context, newTxs types.TxSlots)

	// GetRlpFunc mocks the GetRlp method.
	GetRlpFunc func(tx kv.Tx, hash []byte) ([]byte, error)

	// IdHashKnownFunc mocks the IdHashKnown method.
	IdHashKnownFunc func(tx kv.Tx, hash []byte) (bool, error)

	// OnNewBlockFunc mocks the OnNewBlock method.
	OnNewBlockFunc func(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs types.TxSlots, minedTxs types.TxSlots, tx kv.Tx) error

	// StartedFunc mocks the Started method.
	StartedFunc func() bool

	// ValidateSerializedTxnFunc mocks the ValidateSerializedTxn method.
	ValidateSerializedTxnFunc func(serializedTxn []byte) error

	// calls tracks calls to the methods.
	calls struct {
		// AddLocalTxs holds details about calls to the AddLocalTxs method.
		AddLocalTxs []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// NewTxs is the newTxs argument value.
			NewTxs types.TxSlots
		}
		// AddNewGoodPeer holds details about calls to the AddNewGoodPeer method.
		AddNewGoodPeer []struct {
			// PeerID is the peerID argument value.
			PeerID types.PeerID
		}
		// AddRemoteTxs holds details about calls to the AddRemoteTxs method.
		AddRemoteTxs []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// NewTxs is the newTxs argument value.
			NewTxs types.TxSlots
		}
		// GetRlp holds details about calls to the GetRlp method.
		GetRlp []struct {
			// Tx is the tx argument value.
			Tx kv.Tx
			// Hash is the hash argument value.
			Hash []byte
		}
		// IdHashKnown holds details about calls to the IdHashKnown method.
		IdHashKnown []struct {
			// Tx is the tx argument value.
			Tx kv.Tx
			// Hash is the hash argument value.
			Hash []byte
		}
		// OnNewBlock holds details about calls to the OnNewBlock method.
		OnNewBlock []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// StateChanges is the stateChanges argument value.
			StateChanges *remote.StateChangeBatch
			// UnwindTxs is the unwindTxs argument value.
			UnwindTxs types.TxSlots
			// MinedTxs is the minedTxs argument value.
			MinedTxs types.TxSlots
			// Tx is the tx argument value.
			Tx kv.Tx
		}
		// Started holds details about calls to the Started method.
		Started []struct {
		}
		// ValidateSerializedTxn holds details about calls to the ValidateSerializedTxn method.
		ValidateSerializedTxn []struct {
			// SerializedTxn is the serializedTxn argument value.
			SerializedTxn []byte
		}
	}
	lockAddLocalTxs           sync.RWMutex
	lockAddNewGoodPeer        sync.RWMutex
	lockAddRemoteTxs          sync.RWMutex
	lockGetRlp                sync.RWMutex
	lockIdHashKnown           sync.RWMutex
	lockOnNewBlock            sync.RWMutex
	lockStarted               sync.RWMutex
	lockValidateSerializedTxn sync.RWMutex
}

// AddLocalTxs calls AddLocalTxsFunc.
func (mock *PoolMock) AddLocalTxs(ctx context.Context, newTxs types.TxSlots) ([]DiscardReason, error) {
	callInfo := struct {
		Ctx    context.Context
		NewTxs types.TxSlots
	}{
		Ctx:    ctx,
		NewTxs: newTxs,
	}
	mock.lockAddLocalTxs.Lock()
	mock.calls.AddLocalTxs = append(mock.calls.AddLocalTxs, callInfo)
	mock.lockAddLocalTxs.Unlock()
	if mock.AddLocalTxsFunc == nil {
		var (
			discardReasonsOut []DiscardReason
			errOut            error
		)
		return discardReasonsOut, errOut
	}
	return mock.AddLocalTxsFunc(ctx, newTxs)
}

// AddLocalTxsCalls gets all the calls that were made to AddLocalTxs.
// Check the length with:
//     len(mockedPool.AddLocalTxsCalls())
func (mock *PoolMock) AddLocalTxsCalls() []struct {
	Ctx    context.Context
	NewTxs types.TxSlots
} {
	var calls []struct {
		Ctx    context.Context
		NewTxs types.TxSlots
	}
	mock.lockAddLocalTxs.RLock()
	calls = mock.calls.AddLocalTxs
	mock.lockAddLocalTxs.RUnlock()
	return calls
}

// AddNewGoodPeer calls AddNewGoodPeerFunc.
func (mock *PoolMock) AddNewGoodPeer(peerID types.PeerID) {
	callInfo := struct {
		PeerID types.PeerID
	}{
		PeerID: peerID,
	}
	mock.lockAddNewGoodPeer.Lock()
	mock.calls.AddNewGoodPeer = append(mock.calls.AddNewGoodPeer, callInfo)
	mock.lockAddNewGoodPeer.Unlock()
	if mock.AddNewGoodPeerFunc == nil {
		return
	}
	mock.AddNewGoodPeerFunc(peerID)
}

// AddNewGoodPeerCalls gets all the calls that were made to AddNewGoodPeer.
// Check the length with:
//     len(mockedPool.AddNewGoodPeerCalls())
func (mock *PoolMock) AddNewGoodPeerCalls() []struct {
	PeerID types.PeerID
} {
	var calls []struct {
		PeerID types.PeerID
	}
	mock.lockAddNewGoodPeer.RLock()
	calls = mock.calls.AddNewGoodPeer
	mock.lockAddNewGoodPeer.RUnlock()
	return calls
}

// AddRemoteTxs calls AddRemoteTxsFunc.
func (mock *PoolMock) AddRemoteTxs(ctx context.Context, newTxs types.TxSlots) {
	callInfo := struct {
		Ctx    context.Context
		NewTxs types.TxSlots
	}{
		Ctx:    ctx,
		NewTxs: newTxs,
	}
	mock.lockAddRemoteTxs.Lock()
	mock.calls.AddRemoteTxs = append(mock.calls.AddRemoteTxs, callInfo)
	mock.lockAddRemoteTxs.Unlock()
	if mock.AddRemoteTxsFunc == nil {
		return
	}
	mock.AddRemoteTxsFunc(ctx, newTxs)
}

// AddRemoteTxsCalls gets all the calls that were made to AddRemoteTxs.
// Check the length with:
//     len(mockedPool.AddRemoteTxsCalls())
func (mock *PoolMock) AddRemoteTxsCalls() []struct {
	Ctx    context.Context
	NewTxs types.TxSlots
} {
	var calls []struct {
		Ctx    context.Context
		NewTxs types.TxSlots
	}
	mock.lockAddRemoteTxs.RLock()
	calls = mock.calls.AddRemoteTxs
	mock.lockAddRemoteTxs.RUnlock()
	return calls
}

// GetRlp calls GetRlpFunc.
func (mock *PoolMock) GetRlp(tx kv.Tx, hash []byte) ([]byte, error) {
	callInfo := struct {
		Tx   kv.Tx
		Hash []byte
	}{
		Tx:   tx,
		Hash: hash,
	}
	mock.lockGetRlp.Lock()
	mock.calls.GetRlp = append(mock.calls.GetRlp, callInfo)
	mock.lockGetRlp.Unlock()
	if mock.GetRlpFunc == nil {
		var (
			bytesOut []byte
			errOut   error
		)
		return bytesOut, errOut
	}
	return mock.GetRlpFunc(tx, hash)
}

// GetRlpCalls gets all the calls that were made to GetRlp.
// Check the length with:
//     len(mockedPool.GetRlpCalls())
func (mock *PoolMock) GetRlpCalls() []struct {
	Tx   kv.Tx
	Hash []byte
} {
	var calls []struct {
		Tx   kv.Tx
		Hash []byte
	}
	mock.lockGetRlp.RLock()
	calls = mock.calls.GetRlp
	mock.lockGetRlp.RUnlock()
	return calls
}

// IdHashKnown calls IdHashKnownFunc.
func (mock *PoolMock) IdHashKnown(tx kv.Tx, hash []byte) (bool, error) {
	callInfo := struct {
		Tx   kv.Tx
		Hash []byte
	}{
		Tx:   tx,
		Hash: hash,
	}
	mock.lockIdHashKnown.Lock()
	mock.calls.IdHashKnown = append(mock.calls.IdHashKnown, callInfo)
	mock.lockIdHashKnown.Unlock()
	if mock.IdHashKnownFunc == nil {
		var (
			bOut   bool
			errOut error
		)
		return bOut, errOut
	}
	return mock.IdHashKnownFunc(tx, hash)
}

// IdHashKnownCalls gets all the calls that were made to IdHashKnown.
// Check the length with:
//     len(mockedPool.IdHashKnownCalls())
func (mock *PoolMock) IdHashKnownCalls() []struct {
	Tx   kv.Tx
	Hash []byte
} {
	var calls []struct {
		Tx   kv.Tx
		Hash []byte
	}
	mock.lockIdHashKnown.RLock()
	calls = mock.calls.IdHashKnown
	mock.lockIdHashKnown.RUnlock()
	return calls
}

// OnNewBlock calls OnNewBlockFunc.
func (mock *PoolMock) OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs types.TxSlots, minedTxs types.TxSlots, tx kv.Tx) error {
	callInfo := struct {
		Ctx          context.Context
		StateChanges *remote.StateChangeBatch
		UnwindTxs    types.TxSlots
		MinedTxs     types.TxSlots
		Tx           kv.Tx
	}{
		Ctx:          ctx,
		StateChanges: stateChanges,
		UnwindTxs:    unwindTxs,
		MinedTxs:     minedTxs,
		Tx:           tx,
	}
	mock.lockOnNewBlock.Lock()
	mock.calls.OnNewBlock = append(mock.calls.OnNewBlock, callInfo)
	mock.lockOnNewBlock.Unlock()
	if mock.OnNewBlockFunc == nil {
		var (
			errOut error
		)
		return errOut
	}
	return mock.OnNewBlockFunc(ctx, stateChanges, unwindTxs, minedTxs, tx)
}

// OnNewBlockCalls gets all the calls that were made to OnNewBlock.
// Check the length with:
//     len(mockedPool.OnNewBlockCalls())
func (mock *PoolMock) OnNewBlockCalls() []struct {
	Ctx          context.Context
	StateChanges *remote.StateChangeBatch
	UnwindTxs    types.TxSlots
	MinedTxs     types.TxSlots
	Tx           kv.Tx
} {
	var calls []struct {
		Ctx          context.Context
		StateChanges *remote.StateChangeBatch
		UnwindTxs    types.TxSlots
		MinedTxs     types.TxSlots
		Tx           kv.Tx
	}
	mock.lockOnNewBlock.RLock()
	calls = mock.calls.OnNewBlock
	mock.lockOnNewBlock.RUnlock()
	return calls
}

// Started calls StartedFunc.
func (mock *PoolMock) Started() bool {
	callInfo := struct {
	}{}
	mock.lockStarted.Lock()
	mock.calls.Started = append(mock.calls.Started, callInfo)
	mock.lockStarted.Unlock()
	if mock.StartedFunc == nil {
		var (
			bOut bool
		)
		return bOut
	}
	return mock.StartedFunc()
}

// StartedCalls gets all the calls that were made to Started.
// Check the length with:
//     len(mockedPool.StartedCalls())
func (mock *PoolMock) StartedCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockStarted.RLock()
	calls = mock.calls.Started
	mock.lockStarted.RUnlock()
	return calls
}

// ValidateSerializedTxn calls ValidateSerializedTxnFunc.
func (mock *PoolMock) ValidateSerializedTxn(serializedTxn []byte) error {
	callInfo := struct {
		SerializedTxn []byte
	}{
		SerializedTxn: serializedTxn,
	}
	mock.lockValidateSerializedTxn.Lock()
	mock.calls.ValidateSerializedTxn = append(mock.calls.ValidateSerializedTxn, callInfo)
	mock.lockValidateSerializedTxn.Unlock()
	if mock.ValidateSerializedTxnFunc == nil {
		var (
			errOut error
		)
		return errOut
	}
	return mock.ValidateSerializedTxnFunc(serializedTxn)
}

// ValidateSerializedTxnCalls gets all the calls that were made to ValidateSerializedTxn.
// Check the length with:
//     len(mockedPool.ValidateSerializedTxnCalls())
func (mock *PoolMock) ValidateSerializedTxnCalls() []struct {
	SerializedTxn []byte
} {
	var calls []struct {
		SerializedTxn []byte
	}
	mock.lockValidateSerializedTxn.RLock()
	calls = mock.calls.ValidateSerializedTxn
	mock.lockValidateSerializedTxn.RUnlock()
	return calls
}
