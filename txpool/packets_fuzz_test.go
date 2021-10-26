//go:build gofuzzbeta
// +build gofuzzbeta

package txpool

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/u256"
)

func FuzzPooledTransactions66(f *testing.F) {
	f.Add([]byte{})
	f.Add(decodeHex("f8d7820457f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"))
	f.Fuzz(func(t *testing.T, in []byte) {
		t.Parallel()
		ctx := NewTxParseContext(*u256.N1)
		slots := TxSlots{}
		reqId, _, err := ParsePooledTransactions66(in, 0, ctx, &slots)
		if err != nil {
			t.Skip()
		}

		rlpTxs := [][]byte{}
		for i := range slots.txs {
			rlpTxs = append(rlpTxs, slots.txs[i].rlp)
		}
		_ = EncodePooledTransactions66(rlpTxs, reqId, nil)
		if err != nil {
			t.Skip()
		}
	})
}
