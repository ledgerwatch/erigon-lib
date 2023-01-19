package stream_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/stream"
	"github.com/stretchr/testify/require"
)

type ArrStream2 struct {
	arr      []int
	from, to int
	i        int
	asc      bool
	limit    int
	err      error
	ctx      context.Context
}

func Array2(ctx context.Context, arr []int, from, to int, asc bool, limit int) (kv.UnaryStream[int], error) {
	s := &ArrStream2{arr: arr, from: from, to: to, asc: asc, limit: limit, ctx: ctx}
	return s.init()
}

func (s *ArrStream2) init() (*ArrStream2, error) {
	if s.from != -1 { // no initial position
		if s.asc {
			s.i = 0
		} else {
			s.i = len(s.arr) - 1
		}
		return s, nil
	}

	if s.asc {
		for _, v := range s.arr {
			if v >= s.from {
				break
			}
			s.i++
		}
	} else {
		// seek exactly to given key or previous one
		for _, v := range s.arr {
			if v >= s.from {
				break
			}
			s.i++
		}
	}
	return s, nil
}

func (s *ArrStream2) HasNext() bool {
	if s.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if s.limit == 0 { // limit reached
		return false
	}
	if (s.asc && s.i == len(s.arr)) || (!s.asc && s.i == 0) { // end of table
		return false
	}
	if s.to == -1 { // no early-end
		return true
	}

	//Asc:  [from, to) AND from > to
	//Desc: [from, to) AND from < to
	//cmp := bytes.Compare(s.nextK, s.toPrefix)
	//return (s.orderAscend && cmp < 0) || (!s.orderAscend && cmp > 0)
	return (s.asc && s.arr[s.i] < s.to) || (!s.asc && s.arr[s.i] > s.to)
}
func (s *ArrStream2) Close() {}
func (s *ArrStream2) Next() (int, error) {
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	default:
	}

	v := s.arr[s.i]
	if s.asc {
		s.i++
	} else {
		s.i--
	}
	s.limit--
	return v, s.err
}

func TestName(t *testing.T) {
	ctx := context.Background()
	l := []int{1, 2, 3, 4, 5, 6, 7}
	s1, _ := Array2(ctx, l, 2, 4, true, -1)
	for s1.HasNext() {
		v, _ := s1.Next()
		fmt.Printf("s1: %d\n", v)
	}
	s1, _ = Array2(ctx, l, 4, 2, false, -1)
	for s1.HasNext() {
		v, _ := s1.Next()
		fmt.Printf("s2: %d\n", v)
	}
}

type PaginateIdx struct {
	from, to int
	i        int
	asc      bool
	limit    int
	err      error
	ctx      context.Context
}

func NewPaginateIdx(ctx context.Context, from, to int, limit int, request func(from, to int, limit int) *kv.U64Stream) (*PaginateIdx, error) {
	s := &PaginateIdx{from: from, to: to, limit: limit, ctx: ctx}
	return s.init()
}

func (s *PaginateIdx) init() (*PaginateIdx, error) {
	if s.from != -1 { // no initial position
		if s.asc {
			s.i = 0
		} else {
			s.i = len(s.arr) - 1
		}
		return s, nil
	}

	if s.asc {
		for _, v := range s.arr {
			if v >= s.from {
				break
			}
			s.i++
		}
	} else {
		// seek exactly to given key or previous one
		for _, v := range s.arr {
			if v >= s.from {
				break
			}
			s.i++
		}
	}
	return s, nil
}

func (s *PaginateIdx) HasNext() bool {
	if s.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if s.limit == 0 { // limit reached
		return false
	}
	if (s.asc && s.i == len(s.arr)) || (!s.asc && s.i == 0) { // end of table
		return false
	}
	if s.to == -1 { // no early-end
		return true
	}

	//Asc:  [from, to) AND from > to
	//Desc: [from, to) AND from < to
	//cmp := bytes.Compare(s.nextK, s.toPrefix)
	//return (s.orderAscend && cmp < 0) || (!s.orderAscend && cmp > 0)
	return (s.asc && s.arr[s.i] < s.to) || (!s.asc && s.arr[s.i] > s.to)
}
func (s *PaginateIdx) Close() {}
func (s *PaginateIdx) Next() (int, error) {
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	default:
	}

	v := s.arr[s.i]
	if s.asc {
		s.i++
	} else {
		s.i--
	}
	s.limit--
	return v, s.err
}

//func TestName2(t *testing.T) {
//	ctx := context.Background()
//	l := []int{1, 2, 3, 4, 5, 6, 7}
//	s1, _ := Array2(ctx, l, 2, 4, true, -1)
//	for s1.HasNext() {
//		v, _ := s1.Next()
//		fmt.Printf("s1: %d\n", v)
//	}
//	s1, _ = Array2(ctx, l, 4, 2, false, -1)
//	for s1.HasNext() {
//		v, _ := s1.Next()
//		fmt.Printf("s2: %d\n", v)
//	}
//}

//type PaginatePairs struct {
//	from, to int
//	i        int
//	asc      bool
//	limit    int
//	err      error
//	ctx      context.Context
//}
//
//func NewPaginatePairs(ctx context.Context, from, to int, limit int) (*PaginatePairs, error) {
//	s := &PaginatePairs{from: from, to: to, limit: limit, ctx: ctx}
//	return s.init()
//}
//
//func (s *PaginatePairs) init() (*IntersectU64, error) {
//	if s.from != -1 { // no initial position
//		if s.asc {
//			s.i = 0
//		} else {
//			s.i = len(s.arr) - 1
//		}
//		return s, nil
//	}
//
//	if s.asc {
//		for _, v := range s.arr {
//			if v >= s.from {
//				break
//			}
//			s.i++
//		}
//	} else {
//		// seek exactly to given key or previous one
//		for _, v := range s.arr {
//			if v >= s.from {
//				break
//			}
//			s.i++
//		}
//	}
//	return s, nil
//}
//
//func (s *PaginatePairs) HasNext() bool {
//	if s.err != nil { // always true, then .Next() call will return this error
//		return true
//	}
//	if s.limit == 0 { // limit reached
//		return false
//	}
//	if (s.asc && s.i == len(s.arr)) || (!s.asc && s.i == 0) { // end of table
//		return false
//	}
//	if s.to == -1 { // no early-end
//		return true
//	}
//
//	//Asc:  [from, to) AND from > to
//	//Desc: [from, to) AND from < to
//	//cmp := bytes.Compare(s.nextK, s.toPrefix)
//	//return (s.orderAscend && cmp < 0) || (!s.orderAscend && cmp > 0)
//	return (s.asc && s.arr[s.i] < s.to) || (!s.asc && s.arr[s.i] > s.to)
//}
//func (s *PaginatePairs) Close() {}
//func (s *PaginatePairs) Next() (int, error) {
//	select {
//	case <-s.ctx.Done():
//		return 0, s.ctx.Err()
//	default:
//	}
//
//	v := s.arr[s.i]
//	if s.asc {
//		s.i++
//	} else {
//		s.i--
//	}
//	s.limit--
//	return v, s.err
//}

//func TestName2(t *testing.T) {
//	ctx := context.Background()
//	l := []int{1, 2, 3, 4, 5, 6, 7}
//	s1, _ := Array2(ctx, l, 2, 4, true, -1)
//	for s1.HasNext() {
//		v, _ := s1.Next()
//		fmt.Printf("s1: %d\n", v)
//	}
//	s1, _ = Array2(ctx, l, 4, 2, false, -1)
//	for s1.HasNext() {
//		v, _ := s1.Next()
//		fmt.Printf("s2: %d\n", v)
//	}
//}

// Contraversial use-cases:
// - peek merge unlimited iterators until result>=PageSize. It require "Stop" primitive and better to be less eager.
// - get precise range/limit, maybe big
// Then we have 2 API's: Stream (rename Stream to Stream in kv_interface.go) and Cursor.
// But we want have Cursor+requestBatch operation - to avoid too many network ping-pong.
// invIdx.Stream() and invIdx.Paginate() (c.NextPage())
//

// Cockroach:
// 1. Iterate performs a paginated scan and applying the function f to every page.
// The semantics of retrieval and ordering are the same as for Scan. Note that
// Txn auto-retries the transaction if necessary. Hence, the paginated data
// must not be used for side-effects before the txn has committed.
// 2. []Pair; where Pair{k,v []byte}

// Vitess:
// 1. Exec/Stream methods
// But has separated class for each RealtionalAlgebra operator: InMemSort.run()/Intersect.run()
// then operator implementation use hardcoded Exec/Stream method.
// 2. Pairs has struct:
//  // A length of -1 means that the field is NULL. While
//  // reading values, you have to accummulate the length
//  // to know the offset where the next value begins in values.
//  repeated sint64 lengths = 1;
//  // values contains a concatenation of all values in the row.
//  bytes values = 2;

// no Limit in request
// request (from, N) - merge on client
// stream (all) - push from server - merge on client

func TestMerge(t *testing.T) {
	db := memdb.NewTestDB(t)
	ctx := context.Background()
	t.Run("simple", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.AccountsHistory, []byte{1}, []byte{1})
		_ = tx.Put(kv.AccountsHistory, []byte{3}, []byte{1})
		_ = tx.Put(kv.AccountsHistory, []byte{4}, []byte{1})
		_ = tx.Put(kv.PlainState, []byte{2}, []byte{9})
		_ = tx.Put(kv.PlainState, []byte{3}, []byte{9})
		it, _ := tx.Stream(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Stream(kv.PlainState, nil, nil)
		keys, values, err := stream.MergePairs(it, it2).ToArray()
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}, {3}, {4}}, keys)
		require.Equal([][]byte{{1}, {9}, {1}, {1}}, values)
	})
	t.Run("empty 1st", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.PlainState, []byte{2}, []byte{9})
		_ = tx.Put(kv.PlainState, []byte{3}, []byte{9})
		it, _ := tx.Stream(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Stream(kv.PlainState, nil, nil)
		keys, _, err := stream.MergePairs(it, it2).ToArray()
		require.NoError(err)
		require.Equal([][]byte{{2}, {3}}, keys)
	})
	t.Run("empty 2nd", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.AccountsHistory, []byte{1}, []byte{1})
		_ = tx.Put(kv.AccountsHistory, []byte{3}, []byte{1})
		_ = tx.Put(kv.AccountsHistory, []byte{4}, []byte{1})
		it, _ := tx.Stream(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Stream(kv.PlainState, nil, nil)
		keys, _, err := stream.MergePairs(it, it2).ToArray()
		require.NoError(err)
		require.Equal([][]byte{{1}, {3}, {4}}, keys)
	})
	t.Run("empty both", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it, _ := tx.Stream(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Stream(kv.PlainState, nil, nil)
		m := stream.MergePairs(it, it2)
		require.False(m.HasNext())
	})
	t.Run("error handling", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it := stream.PairsWithError(10)
		it2 := stream.PairsWithError(12)
		keys, _, err := stream.MergePairs(it, it2).ToArray()
		require.Equal("expected error at iteration: 10", err.Error())
		require.Equal(10, len(keys))
	})
}
