//go:build gofuzzbeta
// +build gofuzzbeta

package commitment

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// gotip test -trimpath -v -fuzz=FuzzProcessUpdate -fuzztime=10s ./commitment

func Fuzz_ProcessUpdate(f *testing.F) {
	ha, _ := hex.DecodeString("13ccfe8074645cab4cb42b423625e055f0293c87")
	hb, _ := hex.DecodeString("73f822e709a0016bfaed8b5e81b5f86de31d6895")

	f.Add(uint64(2), ha, uint64(1235105), hb)

	f.Fuzz(func(t *testing.T, balanceA uint64, accountA []byte, balanceB uint64, accountB []byte) {
		if len(accountA) == 0 || len(accountA) > 10 || len(accountB) == 0 || len(accountB) > 10 {
			t.Skip()
		}

		ms := NewMockState(t)
		hph := NewHexPatriciaHashed(1, ms.branchFn, ms.accountFn, ms.storageFn, ms.lockFn, ms.unlockFn)
		hph.SetTrace(false)

		builder := NewUpdateBuilder().
			Balance(hex.EncodeToString(accountA), balanceA).
			Balance(hex.EncodeToString(accountB), balanceB)

		plainKeys, hashedKeys, updates := builder.Build()
		if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
			t.Fatal(err)
		}

		branchNodeUpdates, err := hph.ProcessUpdates(plainKeys, hashedKeys, updates)
		if err != nil {
			t.Fatal(err)
		}

		ms.applyBranchNodeUpdates(branchNodeUpdates)
		rootHash, err := hph.RootHash()
		if err != nil {
			t.Fatalf("failed to evaluate root hash: %v", err)
		}
		if len(rootHash) != 32 {
			t.Fatalf("invalid root hash length: expected 32 bytes, got %v", len(rootHash))
		}

		hph.Reset()

		branchNodeUpdates, err = hph.ProcessUpdates(plainKeys, hashedKeys, updates)
		if err != nil {
			t.Fatal(err)
		}

		rootHash2, err := hph.RootHash()
		if err != nil {
			t.Fatalf("failed to evaluate root hash: %v", err)
		}
		if len(rootHash2) > 32 {
			t.Fatalf("invalid root hash length: expected 32 bytes, got %v", len(rootHash))
		}
		if !bytes.Equal(rootHash, rootHash2) {
			t.Fatalf("invalid second root hash with same updates: [%v] != [%v]", hex.EncodeToString(rootHash), hex.EncodeToString(rootHash2))
		}

	})

}
