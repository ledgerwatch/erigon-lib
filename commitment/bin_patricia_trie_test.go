package commitment

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Update(t *testing.T) {

	tests := []struct {
		key, value []byte
	}{
		{key: []byte{12}, value: []byte("notorious")},
		{key: []byte{14}, value: []byte("2pac")},
		{key: []byte{15}, value: []byte("eminem")},
		{key: []byte{11}, value: []byte("big pun")},
		{key: []byte{20}, value: []byte("method-man")},
		{key: []byte{18}, value: []byte("fat-joe")},
		{key: []byte{30}, value: []byte("jay-z")},
		{key: []byte{5}, value: []byte("redman")},
	}

	bt := NewBinaryPatriciaTrie()
	for _, test := range tests {
		bt.Update(test.key, test.value)
	}

	require.NotNil(t, bt.root)

	stack := make([]*Node, 0)
	var stackPtr int

	stack = append(stack, bt.root)
	stackPtr++
	visited := make(map[*Node]struct{})

	antipaths := make(map[*Node]string)
	antipaths[bt.root] = bitstring(bt.root.Key).String()

	for len(stack) > 0 {
		next := stack[stackPtr-1]
		_, seen := visited[next]
		if seen {
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
		visited[next] = struct{}{}

		if next.Value == nil {
			//require.Truef(t, next.L != nil || next.R != nil, "if node is not a leaf, at least one child should present")
			if next.P != nil {
				require.True(t, next.R != nil && next.L != nil, "bot child should exist L: %p, R: %p", next.L, next.R)
			}
		}
		if next.L != nil || next.R != nil {
			require.Truef(t, next.Value == nil, "if node has childs, node value should be nil, got %v", next.Value)
		}
		if next.L != nil {
			stack = append(stack, next.L)
			stackPtr++

			curp := antipaths[next]
			antipaths[next.L] = curp + bitstring(next.LPrefix).String()

			require.Truef(t, bytes.HasPrefix(next.LPrefix, []byte{0}), "left prefix always begins with 0, got %v", next.LPrefix)
		}
		if next.R != nil {
			stack = append(stack, next.R)
			stackPtr++

			curp := antipaths[next]
			antipaths[next.R] = curp + bitstring(next.RPrefix).String()

			require.Truef(t, bytes.HasPrefix(next.RPrefix, []byte{1}), "right prefix always begins with 1, got %v", next.RPrefix)
		}

		if next.Value != nil {
			// leaf, go back
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
	}

	for node, path := range antipaths {
		if node.Value == nil {
			continue
		}

		if newBitstring(node.Key).String() != path {
			t.Fatalf("node key %v- %v, path %v", node.Key, newBitstring(node.Key).String(), path)
		}
	}

	t.Logf("tree total nodes: %d", len(visited))
}

func Test_Get(t *testing.T) {
	bt := NewBinaryPatriciaTrie()

	tests := []struct {
		key, value []byte
	}{
		{key: []byte{12}, value: []byte("notorious")},
		{key: []byte{14}, value: []byte("2pac")},
		{key: []byte{15}, value: []byte("eminem")},
		{key: []byte{11}, value: []byte("big pun")},
		{key: []byte{20}, value: []byte("method-man")},
		{key: []byte{18}, value: []byte("fat-joe")},
		{key: []byte{30}, value: []byte("jay-z")},
		{key: []byte{5}, value: []byte("redman")},
	}

	for _, test := range tests {
		bt.Update(test.key, test.value)
	}

	require.NotNil(t, bt.root)

	for _, test := range tests {
		buf, ok := bt.Get(test.key)
		require.Truef(t, ok, "key %v not found", test.key)
		require.EqualValues(t, test.value, buf)
	}

}

func Test_BinaryPatriciaTrie_ProcessUpdates(t *testing.T) {
	bt := NewBinaryPatriciaTrie()

	builder := NewUpdateBuilder().
		Balance("9a", 100000).
		Balance("e8", 200000).
		Balance("a2", 300000).
		Balance("f0", 400000).
		Balance("af", 500000).
		Balance("33", 600000).
		Nonce("aa", 184)

	plainKeys, hashedKeys, updates := builder.Build()
	bt.SetTrace(true)
	bt.ProcessUpdates(plainKeys, hashedKeys, updates)

	require.NotNil(t, bt.root)

	stack := make([]*Node, 0)
	var stackPtr int

	stack = append(stack, bt.root)
	stackPtr++
	visited := make(map[*Node]struct{})

	// validity check
	for len(stack) > 0 {
		next := stack[stackPtr-1]
		_, seen := visited[next]
		if seen {
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
		visited[next] = struct{}{}

		if next.Value == nil {
			require.Truef(t, next.L != nil || next.R != nil, "if node is not a leaf, at least one child should present")
			if next.P != nil {
				require.True(t, next.R != nil && next.L != nil, "bot child should exist L: %p, R: %p", next.L, next.R)
			}
		}
		if next.L != nil || next.R != nil {
			require.Truef(t, next.Value == nil, "if node has childs, node value should be nil, got %v", next.Value)
		}
		if next.L != nil {
			stack = append(stack, next.L)
			stackPtr++

			require.Truef(t, bytes.HasPrefix(next.LPrefix, []byte{0}), "left prefix always begins with 0, got %v", next.LPrefix)
		}
		if next.R != nil {
			stack = append(stack, next.R)
			stackPtr++

			require.Truef(t, bytes.HasPrefix(next.RPrefix, []byte{1}), "right prefix always begins with 1, got %v", next.RPrefix)
		}

		if next.Value != nil {
			// leaf, go back
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
	}
	rootHash, _ := bt.RootHash()
	require.Len(t, rootHash, 32)
	fmt.Printf("%+v\n", hex.EncodeToString(rootHash))
	t.Logf("tree total nodes: %d", len(visited))
}

func Test_BinaryPatriciaTrie_UniqueRepresentation(t *testing.T) {
	trieSequential := NewBinaryPatriciaTrie()

	builder := NewUpdateBuilder().
		Balance("9a", 100000).
		Balance("e8", 200000).
		Balance("a2", 300000).
		Balance("f0", 400000).
		Balance("af", 500000).
		Balance("33", 600000).
		Nonce("aa", 184)

	plainKeys, hashedKeys, updates := builder.Build()

	emptyHash, _ := trieSequential.RootHash()
	require.EqualValues(t, EmptyRootHash, emptyHash)

	for i := 0; i < len(plainKeys); i++ {
		trieSequential.ProcessUpdates(plainKeys[i:i+1], hashedKeys[i:i+1], updates[i:i+1])
		sequentialHash, _ := trieSequential.RootHash()
		require.Len(t, sequentialHash, 32)
	}

	trieBatch := NewBinaryPatriciaTrie()
	trieBatch.SetTrace(true)
	trieBatch.ProcessUpdates(plainKeys, hashedKeys, updates)

	sequentialHash, _ := trieSequential.RootHash()
	batchHash, _ := trieBatch.RootHash()

	expectedRoot, _ := hex.DecodeString("87809bbb5282c01ac13cac744db5fee083882e93f781d6af2ad028455d5bdaac")

	require.EqualValues(t, batchHash, sequentialHash)
	require.EqualValues(t, expectedRoot, batchHash)
}

func Test_BinaryPatriciaTrie_BranchEncoding(t *testing.T) {
	builder := NewUpdateBuilder().
		Balance("9a", 100000).
		Balance("e8", 200000).
		Balance("a2", 300000).
		Balance("f0", 400000).
		Balance("af", 500000).
		Balance("33", 600000).
		Nonce("aa", 184)

	plainKeys, hashedKeys, updates := builder.Build()

	trie := NewBinaryPatriciaTrie()

	emptyHash, _ := trie.RootHash()
	require.EqualValues(t, EmptyRootHash, emptyHash)

	trie.SetTrace(true)

	branchUpdates, err := trie.ProcessUpdates(plainKeys, hashedKeys, updates)
	require.NoError(t, err)
	require.NotEmpty(t, branchUpdates)

	sequentialHash, _ := trie.RootHash()
	expectedRoot, _ := hex.DecodeString("87809bbb5282c01ac13cac744db5fee083882e93f781d6af2ad028455d5bdaac")

	require.EqualValues(t, expectedRoot, sequentialHash)

	for pref, update := range branchUpdates {
		account, _, _ := ExtractBinPlainKeys(update)
		t.Logf("pref %v: accounts:", pref)
		for _, acc := range account {
			t.Logf("\t%s\n", hex.EncodeToString(acc))
		}
	}
}

func Test_ExtractBinPlainKeys(t *testing.T) {
	v := []byte{0, 0, 0, 3, 16, 234, 3, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 234, 3, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 1, 2, 20, 185, 16, 244, 69, 61, 95, 160, 98, 248, 40, 202, 243, 176, 226, 173, 255, 56, 36, 64, 124, 36, 230, 128, 128, 160, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 20, 210, 121, 132, 104, 179, 67, 196, 177, 4, 236, 194, 88, 94, 28, 27, 87, 183, 193, 128, 116, 36, 230, 128, 128, 160, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	ExtractBinPlainKeys(v)
}

func Test_ReplaceBinPlainKeys(t *testing.T) {
	v, err := hex.DecodeString("0000000310ea0300000000000001010000000000000001000100000000010001010000000000010101000000000101000100000000000101000000000000010001000000000100010000000000010000000000000000010000000000000100010000000000000100010000000001000000000000000100000100000000000000010000000000000101000000000100010000000000010001000000000001000000000000000101010000000000010101000000000000000101000000000000000000000000000000000000000001000000000000000001010100000000000100000000000000010001000000000000000000000000010101010000000001010100000000000000000100000000010000010000000001010101000000000100010000000000000101010000000001010000000000000000010100000000010100010000000000000100000000000000010000000000000000000000000001000101000000000101010100000000000001000000000001000001000000000101010000000000010001010000000001010101000000000101010000000000010001000000000000000001000000000001010000000000000001010000000001000100000000000100010000000000010101000000000000010100000000000101010100000000000000000000000001010000ea03010000000000010001000000000000010101000000000000000100000000010001010000000000010101000000000000000000000000010000010000000001010101000000000101010000000000010000000000000001000101000000000000010100000000000101010000000000010100000000000000010100000000010100000000000001010100000000000100010000000000010101010000000001010101000000000001000000000000010000000000000000000001000000000000000100000000000100000000000000000101000000000101000100000000000100000000000000010100000000000001000000000000000001000000000000010101000000000001000100000000000001010000000001010101000000000100000000000000010100010000000001000101000000000101010100000000010001000000000000010100000000000101010100000000000101000000000000000100000000000000000100000000010100010000000000010100000000000000010000000000000001000000000001010100000000000000000000000000010000010000000001010000000000000100000000000000010001010000000000010000000000000100000000000000000100000000000001000100000000000001010000000000010001010214b910f4453d5fa062f828caf3b0e2adff3824407c24e68080a000000000000000000000000000000000000000000000000000000000000000000214d2798468b343c4b104ecc2585e1c1b57b7c1807424e68080a00000000000000000000000000000000000000000000000000000000000000000")
	require.NoError(t, err)

	accountPlainKeys := make([][]byte, 0)
	accountPlainKeys = append(accountPlainKeys, []byte("1a"))
	accountPlainKeys = append(accountPlainKeys, []byte("b1"))

	storageKeys := make([][]byte, 0)
	buf := make([]byte, 0)
	fin, err := ReplaceBinPlainKeys(v, accountPlainKeys, storageKeys, buf)
	require.NoError(t, err)
	require.NotEmpty(t, fin)
}

func Test_ReplaceBinPlainKeys2(t *testing.T) {
	v, err := hex.DecodeString("0003000310020001f303010000000000000100")
	require.NoError(t, err)

	accountPlainKeys := make([][]byte, 0)
	accountPlainKeys = append(accountPlainKeys, []byte("1a"))
	accountPlainKeys = append(accountPlainKeys, []byte("b1"))

	storageKeys := make([][]byte, 0)
	buf := make([]byte, 0)
	//fin, err := ReplacePlainKeys(v, accountPlainKeys, storageKeys, buf)
	fin, err := ReplaceBinPlainKeys(v, accountPlainKeys, storageKeys, buf)
	require.NoError(t, err)
	require.NotEmpty(t, fin)
}

func Test_EncodeUpdate(t *testing.T) {
	latest := &Node{
		LPrefix: bitstring{0, 1},
		RPrefix: bitstring{1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0},
		Key:     nil,
		Value:   nil,
	}

	latest.L = &Node{Key: []byte("0a"), Value: []byte("aa"), P: latest}
	latest.R = &Node{Key: []byte("1a"), Value: []byte("bb"), P: latest}

	trie := NewBinaryPatriciaTrie()
	buf := trie.encodeUpdate(bitstring{1, 1}, 0, 3, latest)

	newAccountPlainKeys := make([][]byte, 0)
	newAccountPlainKeys = append(newAccountPlainKeys, []byte("00a"))
	newAccountPlainKeys = append(newAccountPlainKeys, []byte("11a"))

	storageKeys := make([][]byte, 0)
	fin := make([]byte, 0)

	v, err := hex.DecodeString("0003000310020001f303010000000000000100")
	require.NoError(t, err)

	accountPlainKeys, storagePlainKeys, err := ExtractBinPlainKeys(v)
	require.NoError(t, err)
	require.Empty(t, storagePlainKeys)
	require.Len(t, accountPlainKeys, 2)

	fin, err = ReplaceBinPlainKeys(buf, newAccountPlainKeys, storageKeys, fin)
	require.NoError(t, err)
	require.NotEmpty(t, fin)

}
