package commitment

import (
	"crypto/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func Test_AccountEncodeDecode(t *testing.T) {
	balance := uint256.NewInt(1002020020)
	acc := &Account{
		Nonce:   19,
		Balance: *balance,
	}
	rand.Read(acc.CodeHash[:])

	aux := make([]byte, 128)
	n := acc.encode(aux)
	require.NotZero(t, n)

	bcc := new(Account)
	bcc.decode(aux[:n])

	require.EqualValues(t, acc.Nonce, bcc.Nonce)
	require.True(t, acc.Balance.Eq(&bcc.Balance))
	require.EqualValues(t, acc.CodeHash, bcc.CodeHash)
}

func Test_BinPatriciaTrie_UniqueRepresentation(t *testing.T) {
	trie := NewBinaryPatriciaTrie()
	trieBatch := NewBinaryPatriciaTrie()

	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("01", 12).
		Balance("f1", 120000).
		Nonce("aa", 152512).
		Balance("9a", 100000).
		Balance("e8", 200000).
		Balance("a2", 300000).
		Balance("f0", 400000).
		Balance("af", 500000).
		Balance("33", 600000).
		Nonce("aa", 184).
		Build()

	for i := 0; i < len(updates); i++ {
		_, err := trie.ProcessUpdates(plainKeys[i:i+1], hashedKeys[i:i+1], updates[i:i+1])
		require.NoError(t, err)
	}

	trieBatch.ProcessUpdates(plainKeys, hashedKeys, updates)

	hash, _ := trie.RootHash()
	require.Len(t, hash, 32)

	batchHash, _ := trieBatch.RootHash()
	require.EqualValues(t, hash, batchHash)

	for _, hkey := range hashedKeys {
		buf, ok := trie.Get(hkey)
		require.True(t, ok)
		buf2, ok := trieBatch.Get(hkey)
		require.True(t, ok)
		require.EqualValues(t, buf2, buf)
	}
}
