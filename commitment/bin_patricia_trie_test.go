package commitment

import (
	"bytes"
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
	trieBatch.ProcessUpdates(plainKeys, hashedKeys, updates)

	sequentialHash, _ := trieSequential.RootHash()
	batchHash, _ := trieBatch.RootHash()

	require.EqualValues(t, batchHash, sequentialHash)
}
