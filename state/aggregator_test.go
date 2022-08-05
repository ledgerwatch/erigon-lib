package state

import (
	"hash"
	"testing"

	"github.com/google/btree"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/kv"
)

func TestAggregator_touchPlainKey(t *testing.T) {
	type fields struct {
		aggregationStep uint64
		accounts        *Domain
		storage         *Domain
		code            *Domain
		commitment      *Domain
		commTree        *btree.BTreeG[*CommitmentItem]
		keccak          hash.Hash
		patriciaTrie    *commitment.HexPatriciaHashed
		logAddrs        *InvertedIndex
		logTopics       *InvertedIndex
		tracesFrom      *InvertedIndex
		tracesTo        *InvertedIndex
		txNum           uint64
		rwTx            kv.RwTx
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Aggregator{
				aggregationStep: tt.fields.aggregationStep,
				accounts:        tt.fields.accounts,
				storage:         tt.fields.storage,
				code:            tt.fields.code,
				commitment:      tt.fields.commitment,
				commTree:        tt.fields.commTree,
				keccak:          tt.fields.keccak,
				patriciaTrie:    tt.fields.patriciaTrie,
				logAddrs:        tt.fields.logAddrs,
				logTopics:       tt.fields.logTopics,
				tracesFrom:      tt.fields.tracesFrom,
				tracesTo:        tt.fields.tracesTo,
				txNum:           tt.fields.txNum,
				rwTx:            tt.fields.rwTx,
			}
			a.touchPlainKey(tt.args.key)
		})
	}
}
