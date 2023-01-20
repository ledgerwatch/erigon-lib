package stream

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

type ArrStream[V any] struct {
	arr []V
	i   int
}

func ReverseArray[V any](arr []V) *ArrStream[V] {
	for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
		arr[i], arr[j] = arr[j], arr[i]
	}
	return Array(arr)
}
func Array[V any](arr []V) *ArrStream[V] { return &ArrStream[V]{arr: arr} }
func (it *ArrStream[V]) HasNext() bool   { return it.i < len(it.arr) }
func (it *ArrStream[V]) Close()          {}
func (it *ArrStream[V]) Next() (V, error) {
	v := it.arr[it.i]
	it.i++
	return v, nil
}
func (it *ArrStream[V]) NextBatch() ([]V, error) {
	v := it.arr[it.i:]
	it.i = len(it.arr)
	return v, nil
}

func Equal[V comparable](s1, s2 kv.UnaryStream[V]) bool {
	for s1.HasNext() && s2.HasNext() {
		k1, e1 := s1.Next()
		k2, e2 := s2.Next()
		if k1 != k2 || e1 != e2 {
			return false
		}
	}

	return !s1.HasNext() && !s2.HasNext()
}

func ExpectEqual[V comparable](t testing.TB, s1, s2 kv.UnaryStream[V]) {
	//t.Helper()
	for s1.HasNext() && s2.HasNext() {
		k1, e1 := s1.Next()
		k2, e2 := s2.Next()
		require.Equal(t, k1, k2)
		require.Equal(t, e1, e2)
	}

	has1 := s1.HasNext()
	has2 := s2.HasNext()
	var label string
	if has1 {
		v1, _ := s1.Next()
		label = fmt.Sprintf("v1: %v", v1)
	}
	if has2 {
		v2, _ := s2.Next()
		label += fmt.Sprintf(" v2: %v", v2)
	}
	require.False(t, has1, label)
	require.False(t, has2, label)
}

// UnionPairsStream - merge 2 kv.Pairs streams to 1 in lexicographically order
// 1-st stream has higher priority - when 2 streams return same key
type UnionPairsStream struct {
	x, y               kv.Pairs
	xHasNext, yHasNext bool
	xNextK, xNextV     []byte
	yNextK, yNextV     []byte
	nextErr            error
}

func MergePairs(x, y kv.Pairs) *UnionPairsStream {
	m := &UnionPairsStream{x: x, y: y}
	m.advanceX()
	m.advanceY()
	return m
}
func (m *UnionPairsStream) HasNext() bool { return m.xHasNext || m.yHasNext }
func (m *UnionPairsStream) advanceX() {
	if m.nextErr != nil {
		m.xNextK, m.xNextV = nil, nil
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.xNextV, m.nextErr = m.x.Next()
	}
}
func (m *UnionPairsStream) advanceY() {
	if m.nextErr != nil {
		m.yNextK, m.yNextV = nil, nil
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.yNextV, m.nextErr = m.y.Next()
	}
}
func (m *UnionPairsStream) Next() ([]byte, []byte, error) {
	if m.nextErr != nil {
		return nil, nil, m.nextErr
	}
	if !m.xHasNext && !m.yHasNext {
		panic(1)
	}
	if m.xHasNext && m.yHasNext {
		cmp := bytes.Compare(m.xNextK, m.yNextK)
		if cmp < 0 {
			k, v, err := m.xNextK, m.xNextV, m.nextErr
			m.advanceX()
			return k, v, err
		} else if cmp == 0 {
			k, v, err := m.xNextK, m.xNextV, m.nextErr
			m.advanceX()
			m.advanceY()
			return k, v, err
		}
		k, v, err := m.yNextK, m.yNextV, m.nextErr
		m.advanceY()
		return k, v, err
	}
	if m.xHasNext {
		k, v, err := m.xNextK, m.xNextV, m.nextErr
		m.advanceX()
		return k, v, err
	}
	k, v, err := m.yNextK, m.yNextV, m.nextErr
	m.advanceY()
	return k, v, err
}
func (m *UnionPairsStream) ToArray() (keys, values [][]byte, err error) { return ToPairsArr(m) }

// UnionPairsStream - merge 2 kv.Pairs streams to 1 in lexicographically order
// 1-st stream has higher priority - when 2 streams return same key
type IntersectStream[T constraints.Ordered] struct {
	x, y               kv.UnaryStream[T]
	xHasNext, yHasNext bool
	xNextK, yNextK     T
	err                error
}

func Intersect[T constraints.Ordered](x, y kv.UnaryStream[T]) *IntersectStream[T] {
	m := &IntersectStream[T]{x: x, y: y}
	m.advance()
	return m
}
func (m *IntersectStream[T]) HasNext() bool { return m.xHasNext && m.yHasNext }
func (m *IntersectStream[T]) advance() {
	m.advanceX()
	m.advanceY()
	for m.xHasNext && m.yHasNext {
		if m.err != nil {
			break
		}
		if m.xNextK < m.yNextK {
			m.advanceX()
			continue
		} else if m.xNextK == m.yNextK {
			return
		} else {
			m.advanceY()
			continue
		}
	}
	m.xHasNext = false
}

func (m *IntersectStream[T]) advanceX() {
	if m.err != nil {
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.err = m.x.Next()
	}
}
func (m *IntersectStream[T]) advanceY() {
	if m.err != nil {
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.err = m.y.Next()
	}
}
func (m *IntersectStream[T]) Next() (T, error) {
	k, err := m.xNextK, m.err
	m.advance()
	return k, err
}

//func (m *IntersectStream) ToArray() (keys, values [][]byte, err error) { return Naive2Arr(m) }

func ToPairsArr(it kv.Pairs) (keys, values [][]byte, err error) {
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return keys, values, err
		}
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values, nil
}

func ToArr[T any](it kv.UnaryStream[T]) (res []T, err error) {
	for it.HasNext() {
		k, err := it.Next()
		if err != nil {
			return res, nil
		}
		res = append(res, k)
	}
	return res, err
}

// PairsWithErrorStream - return N, keys and then error
type PairsWithErrorStream struct {
	errorAt, i int
}

func PairsWithError(errorAt int) *PairsWithErrorStream {
	return &PairsWithErrorStream{errorAt: errorAt}
}
func (m *PairsWithErrorStream) HasNext() bool { return true }
func (m *PairsWithErrorStream) Next() ([]byte, []byte, error) {
	if m.i >= m.errorAt {
		return nil, nil, fmt.Errorf("expected error at iteration: %d", m.errorAt)
	}
	m.i++
	return []byte(fmt.Sprintf("%x", m.i)), []byte(fmt.Sprintf("%x", m.i)), nil
}

// TransformStream - analog `map` (in terms of map-filter-reduce pattern)
type TransformStream[K, V any] struct {
	it        kv.Stream[K, V]
	transform func(K, V) (K, V)
}

func TransformPairs(it kv.Pairs, transform func(k, v []byte) ([]byte, []byte)) *TransformStream[[]byte, []byte] {
	return &TransformStream[[]byte, []byte]{it: it, transform: transform}
}
func Transform[K, V any](it kv.Stream[K, V], transform func(K, V) (K, V)) *TransformStream[K, V] {
	return &TransformStream[K, V]{it: it, transform: transform}
}
func (m *TransformStream[K, V]) HasNext() bool { return m.it.HasNext() }
func (m *TransformStream[K, V]) Next() (K, V, error) {
	k, v, err := m.it.Next()
	if err != nil {
		return k, v, err
	}
	newK, newV := m.transform(k, v)
	return newK, newV, nil
}

// FilterStream - analog `map` (in terms of map-filter-reduce pattern)
type FilterStream[K, V any] struct {
	it     kv.Stream[K, V]
	filter func(K, V) bool
}

func FilterPairs(it kv.Pairs, filter func(k, v []byte) bool) *FilterStream[[]byte, []byte] {
	return &FilterStream[[]byte, []byte]{it: it, filter: filter}
}
func Filter[K, V any](it kv.Stream[K, V], filter func(K, V) bool) *FilterStream[K, V] {
	return &FilterStream[K, V]{it: it, filter: filter}
}
func (m *FilterStream[K, V]) HasNext() bool { return m.it.HasNext() }
func (m *FilterStream[K, V]) Next() (k K, v V, err error) {
	for m.it.HasNext() {
		k, v, err = m.it.Next()
		if err != nil {
			return k, v, err
		}
		if !m.filter(k, v) {
			continue
		}
		return k, v, nil
	}
	return k, v, nil
}
