package bin_patricia_trie

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Insert(t *testing.T) {
	bt := NewBinPatriciaTrie()
	// right split
	bt.Insert([]byte{12}, []byte("biggie"))
	bt.Insert([]byte{14}, []byte("2pac"))
	bt.Insert([]byte{15}, []byte("eminem"))
	//bt.Insert([]byte{11}, []byte("biggie"))
	//bt.Insert([]byte{20}, []byte("2pac"))
	//bt.Insert([]byte{18}, []byte("eminem"))
	//bt.Insert([]byte{30}, []byte("eminem"))
	//bt.Insert([]byte{5}, []byte("eminem"))

	require.NotNil(t, bt.root)

	stack := make([]*Node, 0)
	var stackPtr int

	stack = append(stack, bt.root)
	stackPtr++
	visited := make(map[*Node]struct{})

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
	t.Logf("tree total nodes: %d", len(visited))

}

func Test_InsertLeftSplit(t *testing.T) {
	bt := NewBinPatriciaTrie()
	// right split
	bt.Insert([]byte{12}, []byte("biggie"))
	bt.Insert([]byte{14}, []byte("2pac"))
	bt.Insert([]byte{8}, []byte("eminem"))

	require.NotNil(t, bt.root)

	stack := make([]*Node, 0)
	var stackPtr int

	stack = append(stack, bt.root)
	stackPtr++
	visited := make(map[*Node]struct{})

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
		}
		if next.L != nil || next.R != nil {
			require.Truef(t, next.Value == nil, "if node has childs, node value should be nil")
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
	t.Logf("tree total nodes: %d", len(visited))

}
