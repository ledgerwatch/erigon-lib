package state

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/mmap"
)

func logBase(n, base uint64) uint64 {
	return uint64(math.Ceil(math.Log(float64(n)) / math.Log(float64(base))))
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

type markupCursor struct {
	l, p, di, si uint64
	//l - level
	//p - pos inside level
	//si - current, actual son index
	//di - data array index
}

type node struct {
	p, d, s, fc uint64
	key         []byte
	val         []byte
}

type key struct {
	bucket, fprint uint64
}

func bytesToKey(b []byte) key {
	if len(b) > 16 {
		panic(fmt.Errorf("invalid size of key bytes to convert (size %d)", len(b)))
	}
	return key{
		bucket: binary.BigEndian.Uint64(b),
		fprint: binary.BigEndian.Uint64(b[8:]),
	}
}

func (k key) compare(k2 key) int {
	if k.bucket < k2.bucket {
		return -1
	}
	if k.bucket > k2.bucket {
		return 1
	}
	if k.fprint < k2.fprint {
		return -1
	}
	if k.fprint > k2.fprint {
		return 1
	}
	return 0
}

func (k key) Bytes() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], k.bucket)
	binary.BigEndian.PutUint64(buf[8:], k.fprint)
	return buf
}

// deprecated
func binsearch(a []node, x uint64) uint64 {
	l, r := uint64(0), uint64(len(a))
	for l < r {
		mid := (l + r) / 2
		if a[mid].d < x {
			l = mid + 1
		} else {
			r = mid
		}
	}
	return l
}

type Cursor struct {
	ctx context.Context
	ix  *BtIndex

	key   []byte
	value []byte
	d     uint64
}

func newCursor(ctx context.Context, k, v []byte, d uint64) *Cursor {
	return &Cursor{
		ctx:   ctx,
		key:   k,
		value: v,
		d:     d,
	}
}

func (c *Cursor) Key() []byte {
	return c.key
}

func (c *Cursor) Value() []byte {
	return c.value
}

func (c *Cursor) Next() bool {
	if c.d+1 >= c.ix.KeyCount() {
		return false
	}
	k, v, err := c.ix.dataLookup(c.d + 1)
	if err != nil {
		return false
	}
	c.key = common.Copy(k)
	c.value = common.Copy(v)
	c.d++
	return true
}

type btAlloc struct {
	d       uint64 // depth
	M       uint64 // child limit of any node
	N       uint64
	K       uint64
	vx      []uint64   // vertex count on level
	sons    [][]uint64 // i - level; 0 <= i < d; j_k - amount, j_k+1 - child count
	cursors []markupCursor
	nodes   [][]node
	data    []uint64
	naccess uint64
	trace   bool

	dataLookup func(di uint64) ([]byte, []byte, error)
}

func newBtAlloc(k, M uint64) *btAlloc {
	d := logBase(k, M)
	m := max64(2, M>>1)

	fmt.Printf("k=%d d=%d, M=%d m=%d\n", k, d, M, m)
	a := &btAlloc{
		vx:      make([]uint64, d+1),
		sons:    make([][]uint64, d+1),
		cursors: make([]markupCursor, d),
		nodes:   make([][]node, d),
		data:    make([]uint64, k),
		M:       M,
		K:       k,
		d:       d,
		trace:   true,
	}
	a.vx[0] = 1
	a.vx[d] = k

	nnc := func(vx uint64) uint64 {
		return uint64(math.Ceil(float64(vx) / float64(M)))
	}

	for i := a.d - 1; i > 0; i-- {
		nnc := uint64(math.Ceil(float64(a.vx[i+1]) / float64(M)))
		//nvc := uint64(math.Floor(float64(a.vx[i+1]) / float64(m))-1)
		//nnc := a.vx[i+1] / M
		//nvc := a.vx[i+1] / m
		//bvc := a.vx[i+1] / (m + (m >> 1))
		//_, _ = nvc, nnc
		a.vx[i] = min64(uint64(math.Pow(float64(M), float64(i))), nnc)
	}

	ncount := uint64(0)
	pnv := uint64(0)
	for l := a.d - 1; l > 0; l-- {
		s := nnc(a.vx[l+1])
		//left := a.vx[l+1] % M
		//if left > 0 {
		//	if left < m {
		//		s--
		//		newPrev := M - (m - left)
		//		dp := M - newPrev
		//		a.sons[l] = append(a.sons[l], 1, newPrev, 1, left+dp)
		//	} else {
		//		a.sons[l] = append(a.sons[l], 1, left)
		//	}
		//}
		a.sons[l] = append(a.sons[l], s, M)
		for ik := 0; ik < len(a.sons[l]); ik += 2 {
			ncount += a.sons[l][ik] * a.sons[l][ik+1]
			if l == 1 {
				pnv += a.sons[l][ik]
			}
		}
	}
	a.sons[0] = []uint64{1, pnv}
	ncount += a.sons[0][0] * a.sons[0][1] // last one
	a.N = ncount
	fmt.Printf("ncount=%d ∂%.5f\n", ncount, float64(a.N-uint64(k))/float64(a.N))

	for i, v := range a.sons {
		fmt.Printf("L%d=%v\n", i, v)
	}

	return a
}

func (a *btAlloc) traverseTrick() {
	for l := 0; l < len(a.sons)-1; l++ {
		if len(a.sons[l]) < 2 {
			panic("invalid btree allocation markup")
		}
		a.cursors[l] = markupCursor{uint64(l), 1, 0, 0}
		a.nodes[l] = make([]node, 0)
	}

	lf := a.cursors[len(a.cursors)-1]
	c := a.cursors[(len(a.cursors) - 2)]

	var d uint64
	var fin bool

	lf.di = d
	lf.si++
	d++
	a.cursors[len(a.cursors)-1] = lf

	moved := true
	for int(c.p) <= len(a.sons[c.l]) {
		if fin || d > a.K {
			break
		}
		c, lf = a.cursors[c.l], a.cursors[lf.l]

		c.di = d
		c.si++

		sons := a.sons[lf.l][lf.p]
		for i := uint64(1); i < sons; i++ {
			lf.si++
			d++
		}
		lf.di = d
		d++

		a.nodes[lf.l] = append(a.nodes[lf.l], node{p: lf.p, s: lf.si, d: lf.di})
		a.nodes[c.l] = append(a.nodes[c.l], node{p: c.p, s: c.si, d: c.di})
		a.cursors[lf.l] = lf
		a.cursors[c.l] = c

		for l := lf.l; l >= 0; l-- {
			sc := a.cursors[l]
			sons, gsons := a.sons[sc.l][sc.p-1], a.sons[sc.l][sc.p]
			if l < c.l && moved {
				sc.di = d
				a.nodes[sc.l] = append(a.nodes[sc.l], node{d: sc.di})
				sc.si++
				d++
			}
			moved = (sc.si-1)/gsons != sc.si/gsons
			if sc.si/gsons >= sons {
				sz := uint64(len(a.sons[sc.l]) - 1)
				if sc.p+2 > sz {
					fin = l == lf.l
					break
				} else {
					sc.p += 2
					sc.si, sc.di = 0, 0
				}
				//moved = true
			}
			if l == lf.l {
				sc.si++
				sc.di = d
				d++
			}
			a.cursors[l] = sc
			if l == 0 {
				break
			}
		}
		moved = false
	}
}

func (a *btAlloc) traverseDfs() {
	for l := 0; l < len(a.sons)-1; l++ {
		if len(a.sons[l]) < 2 {
			panic("invalid btree allocation markup")
		}
		a.cursors[l] = markupCursor{uint64(l), 1, 0, 0}
		a.nodes[l] = make([]node, 0)
	}

	// TODO if keys less than half leaf size store last key to just support bsearch on these amount.
	c := a.cursors[len(a.cursors)-1]
	pc := a.cursors[(len(a.cursors) - 2)]
	root := new(node)
	trace := false

	var di uint64
	for stop := false; !stop; {
		// fill leaves, mark parent if needed (until all grandparents not marked up until root)
		// check if eldest parent has brothers
		//     -- has bros -> fill their leaves from the bottom
		//     -- no bros  -> shift cursor (tricky)
		if di > a.K {
			a.N = di - 1 // actually filled node count
			fmt.Printf("ncount=%d ∂%.5f\n", a.N, float64(a.N-a.K)/float64(a.N))
			break
		}

		bros, parents := a.sons[c.l][c.p], a.sons[c.l][c.p-1]
		for i := uint64(0); i < bros; i++ {
			c.di = di
			if trace {
				fmt.Printf("L%d |%d| d %2d s %2d\n", c.l, c.p, c.di, c.si)
			}
			c.si++
			di++

			if i == 0 {
				pc.di = di
				if trace {
					fmt.Printf("P%d |%d| d %2d s %2d\n", pc.l, pc.p, pc.di, pc.si)
				}
				pc.si++
				di++
			}
		}

		a.nodes[c.l] = append(a.nodes[c.l], node{p: c.p, d: c.di, s: c.si})
		a.nodes[pc.l] = append(a.nodes[pc.l], node{p: pc.p, d: pc.di, s: pc.si, fc: uint64(len(a.nodes[c.l]) - 1)})

		pid := c.si / bros
		if pid >= parents {
			if c.p+2 >= uint64(len(a.sons[c.l])) {
				stop = true // end of row
				if trace {
					fmt.Printf("F%d |%d| d %2d\n", c.l, c.p, c.di)
				}
			} else {
				c.p += 2
				c.si = 0
				c.di = 0
			}
		}
		a.cursors[c.l] = c
		a.cursors[pc.l] = pc

		for l := pc.l; l >= 0; l-- {
			pc := a.cursors[l]
			uncles := a.sons[pc.l][pc.p]
			grands := a.sons[pc.l][pc.p-1]

			pi1 := pc.si / uncles
			pc.si++
			pc.di = 0

			pi2 := pc.si / uncles
			moved := pi2-pi1 != 0

			switch {
			case pc.l > 0:
				gp := a.cursors[pc.l-1]
				if gp.di == 0 {
					gp.di = di
					di++
					if trace {
						fmt.Printf("P%d |%d| d %2d s %2d\n", gp.l, gp.p, gp.di, gp.si)
					}
					a.nodes[gp.l] = append(a.nodes[gp.l], node{p: gp.p, d: gp.di, s: gp.si, fc: uint64(len(a.nodes[l]) - 1)})
					a.cursors[gp.l] = gp
				}
			default:
				if root.d == 0 {
					root.d = di
					//di++
					if trace {
						fmt.Printf("ROOT | d %2d\n", root.d)
					}
				}
			}

			//fmt.Printf("P%d |%d| d %2d s %2d pid %d\n", pc.l, pc.p, pc.di, pc.si-1)
			if pi2 >= grands { // skip one step of si due to different parental filling order
				if pc.p+2 >= uint64(len(a.sons[pc.l])) {
					if trace {
						fmt.Printf("EoRow %d |%d|\n", pc.l, pc.p)
					}
					break // end of row
				}
				//fmt.Printf("N %d d%d s%d\n", pc.l, pc.di, pc.si)
				//fmt.Printf("P%d |%d| d %2d s %2d pid %d\n", pc.l, pc.p, pc.di, pc.si, pid)
				pc.p += 2
				pc.si = 0
				pc.di = 0
			}
			a.cursors[pc.l] = pc

			if !moved {
				break
			}
		}
	}
}

// deprecated
func (a *btAlloc) traverse() {
	var sum uint64
	for l := 0; l < len(a.sons)-1; l++ {
		if len(a.sons[l]) < 2 {
			panic("invalid btree allocation markup")
		}
		a.cursors[l] = markupCursor{uint64(l), 1, 0, 0}

		for i := 0; i < len(a.sons[l]); i += 2 {
			sum += a.sons[l][i] * a.sons[l][i+1]
		}
		a.nodes[l] = make([]node, 0)
	}
	fmt.Printf("nodes total %d\n", sum)

	c := a.cursors[len(a.cursors)-1]

	var di uint64
	for stop := false; !stop; {
		bros := a.sons[c.l][c.p]
		parents := a.sons[c.l][c.p-1]

		// fill leaves, mark parent if needed (until all grandparents not marked up until root)
		// check if eldest parent has brothers
		//     -- has bros -> fill their leaves from the bottom
		//     -- no bros  -> shift cursor (tricky)

		for i := uint64(0); i < bros; i++ {
			c.di = di
			fmt.Printf("L%d |%d| d %2d s %2d\n", c.l, c.p, c.di, c.si)
			c.si++
			di++
		}

		pid := c.si / bros
		if pid >= parents {
			if c.p+2 >= uint64(len(a.sons[c.l])) {
				stop = true // end of row
				fmt.Printf("F%d |%d| d %2d\n", c.l, c.p, c.di)
			} else {
				//fmt.Printf("N %d d%d s%d\n", c.l, c.di, c.si)
				//a.nodes[c.l] = append(a.nodes[c.l], node{p: c.p, d: c.di, s: c.si})
				c.p += 2
				c.si = 0
				c.di = 0
			}
		}
		a.cursors[c.l] = c

		for l := len(a.cursors) - 2; l >= 0; l-- {
			pc := a.cursors[l]
			uncles := a.sons[pc.l][pc.p]
			grands := a.sons[pc.l][pc.p-1]

			pi1 := pc.si / uncles
			pc.si++
			pi2 := pc.si / uncles
			moved := pi2-pi1 != 0
			pc.di = di
			fmt.Printf("P%d |%d| d %2d s %2d pid %d\n", pc.l, pc.p, pc.di, pc.si-1, pid)
			a.nodes[pc.l] = append(a.nodes[pc.l], node{p: pc.p, d: pc.di, s: pc.si})

			di++

			if pi2 >= grands { // skip one step of si due to different parental filling order
				if pc.p+2 >= uint64(len(a.sons[pc.l])) {
					// end of row
					fmt.Printf("E%d |%d| d %2d\n", pc.l, pc.p, pc.di)
					break
				}
				//fmt.Printf("N %d d%d s%d\n", pc.l, pc.di, pc.si)
				//fmt.Printf("P%d |%d| d %2d s %2d pid %d\n", pc.l, pc.p, pc.di, pc.si, pid)
				pc.p += 2
				pc.si = 0
				pc.di = 0
			}
			a.cursors[pc.l] = pc

			if l >= 1 && a.cursors[l-1].di == 0 {
				continue
			}
			if !moved {
				break
			}
		}
	}
}

// deprecated
func (a *btAlloc) fetchByDi(i uint64) (uint64, bool) {
	if int(i) >= len(a.data) {
		return 0, true
	}
	return a.data[i], false
}

func (a *btAlloc) bsKey(x []byte, l, r uint64) (*Cursor, error) {
	var exit bool
	for l <= r {
		di := (l + r) >> 1

		mk, value, err := a.dataLookup(di)
		a.naccess++

		cmp := bytes.Compare(mk, x)
		switch {
		case err != nil:
			break
		case cmp == 0:
			return newCursor(context.TODO(), mk, value, di), nil
		case cmp == -1:
			if exit {
				break
			}
			l = di + 1
		default:
			r = di
		}
		if l == r {
			break
		}
	}
	return nil, fmt.Errorf("not found")
}

// deprecated
func (a *btAlloc) bs(i, x, l, r uint64, direct bool) (uint64, uint64, bool) {
	var exit bool
	var di uint64
	for l <= r {
		m := (l + r) >> 1
		if l == r {
			m = l
			exit = true
		}

		switch direct {
		case true:
			if m >= uint64(len(a.data)) {
				di = a.data[a.K-1]
				exit = true
			} else {
				di = a.data[m]
			}
		case false:
			di = a.nodes[i][m].d
		}

		mkey, nf := a.fetchByDi(di)
		a.naccess++
		switch {
		case nf:
			break
		case mkey == x:
			return m, r, true
		case mkey < x:
			if exit {
				break
			}
			if m+1 == r {
				if m > 0 {
					m--
				}
				return m, r, false
			}
			l = m + 1
		default:
			if exit {
				break
			}
			if m-l == 1 && l > 0 {
				return l - 1, r, false
			}
			r = m
		}
		if exit {
			break
		}
	}
	return l, r, false
}

func (a *btAlloc) bsNode(i, l, r uint64, x []byte) (*node, int64, int64, []byte) {
	var exit bool
	var lm, rm int64
	lm = -1
	rm = -1
	var n *node

	for l <= r {
		m := (l + r) >> 1
		if l == r {
			m = l
			exit = true
		}

		n = &a.nodes[i][m]
		// di = n.d
		// _ = di

		a.naccess++

		// mk, value, err := a.dataLookup(di)
		cmp := bytes.Compare(n.key, x)
		switch {
		// case err != nil:
		// 	fmt.Printf("err at switch %v\n", err)
		// 	break
		case cmp == 0:
			return n, int64(m), int64(m), n.val
		case cmp < 0:
			// if m+1 == r {
			// 	return n, m, rm, nil
			// }
			l = m + 1
			lm = int64(m)
		default:
			// if m == l {
			// 	return n, m, rm, nil
			// }
			r = m
			rm = int64(r)
		}
		if exit {
			break
		}
	}
	return n, lm, rm, nil
}

func (a *btAlloc) seek(ik []byte) (*Cursor, error) {
	var L, minD, maxD uint64
	var lm, rm int64
	R := uint64(len(a.nodes[0]) - 1)
	maxD = a.K + 1

	if a.trace {
		fmt.Printf("seek key %x\n", ik)
	}

	ln := new(node)
	var val []byte
	for l, level := range a.nodes {
		ln, lm, rm, val = a.bsNode(uint64(l), L, R, ik)
		if ln == nil { // should return node which is nearest to key from the left so never nil
			L = 0
			if a.trace {
				fmt.Printf("found nil key %x di=%d lvl=%d naccess_ram=%d\n", level[lm].key, level[lm].d, l, a.naccess)
			}
			panic(fmt.Errorf("nil node at %d", l))
		}
		if lm >= 0 {
			minD = a.nodes[l][lm].d
			L = level[lm].fc
		}
		if rm >= 0 {
			maxD = a.nodes[l][rm].d
			R = level[rm].fc
		}

		switch bytes.Compare(ln.key, ik) {
		case 1: // key > ik
			maxD = ln.d
		case -1: // key < ik
			minD = ln.d
		case 0:
			if a.trace {
				fmt.Printf("found key %x v=%x naccess_ram=%d\n", ik, val /*level[m].d,*/, a.naccess)
			}
			return newCursor(context.TODO(), ln.key, val, ln.d), nil
		}
		if a.trace {
			fmt.Printf("range={%x d=%d p=%d} (%d, %d) L=%d naccess_ram=%d\n", ln.key, ln.d, ln.p, minD, maxD, l, a.naccess)
		}
	}

	switch bytes.Compare(ik, ln.key) {
	case -1:
		L = minD // =0
	case 0:
		if a.trace {
			fmt.Printf("last found key %x v=%x di=%d naccess_ram=%d\n", ln.key, ln.val, ln.d, a.naccess)
		}
		return newCursor(context.TODO(), ln.key, val, ln.d), nil
	case 1:
		L = ln.d + 1
	}

	a.naccess = 0 // reset count before actually go to storage
	cursor, err := a.bsKey(ik, L, maxD)
	if err != nil {
		if a.trace {
			fmt.Printf("key %x not found\n", ik)
		}
		return nil, err
	} else {
		if a.trace {
			fmt.Printf("finally found key %x v=%x naccess_disk=%d [err=%v]\n", cursor.key, cursor.value, a.naccess, err)
		}
		return cursor, nil
	}

	return nil, fmt.Errorf("key not found")
}

// deprecated
func (a *btAlloc) search(ik uint64) bool {
	l, r := uint64(0), uint64(len(a.nodes[0]))
	lr, hr := uint64(0), a.N
	var naccess int64
	var trace bool
	for i := 0; i < len(a.nodes); i++ {
		for l < r {
			m := (l + r) >> 1
			mkey, nf := a.fetchByDi(a.nodes[i][m].d)
			naccess++
			if nf {
				break
			}
			if mkey < ik {
				lr = mkey
				l = m + 1
			} else if mkey == ik {
				if trace {
					fmt.Printf("found key %d @%d naccess=%d\n", mkey, m, naccess)
				}
				return true //mkey
			} else {
				r = m
				hr = mkey
			}
		}
		if trace {
			fmt.Printf("range={%d,%d} L=%d naccess=%d\n", lr, hr, i, naccess)
		}
		if i == len(a.nodes) {
			if trace {
				fmt.Printf("%d|%d - %d|%d\n", l, a.nodes[i][l].d, r, a.nodes[i][r].d)
			}
			return true
		}
		if i+1 >= len(a.nodes) {
			break
		}
		l = binsearch(a.nodes[i+1], lr)
		r = binsearch(a.nodes[i+1], hr)
	}

	if trace {
		fmt.Printf("smallest range %d-%d (%d-%d)\n", lr, hr, l, r)
	}
	if l == r && l > 0 {
		l--
	}

	lr, hr = a.nodes[a.d-1][l].d, a.nodes[a.d-1][r].d
	// search in smallest found interval
	for lr < hr {
		m := (lr + hr) >> 1
		mkey, nf := a.fetchByDi(m)
		naccess++
		if nf {
			break
		}
		if mkey < ik {
			//lr = mkey
			lr = m + 1
		} else if mkey == ik {
			if trace {
				fmt.Printf("last found key %d @%d naccess=%d\n", mkey, m, naccess)
			}
			return true //mkey
		} else {
			hr = m
			//hr = mkey
		}
	}

	return false
}

func (a *btAlloc) printSearchMx() {
	for i, n := range a.nodes {
		fmt.Printf("D%d |%d| ", i, len(n))
		for j, s := range n {
			fmt.Printf("%d ", s.d)
			if s.d >= a.K {
				break
			}

			kb, v, err := a.dataLookup(s.d)
			if err != nil {
				fmt.Printf("d %d not found %v\n", s.d, err)
			}
			a.nodes[i][j].key = common.Copy(kb)
			a.nodes[i][j].val = common.Copy(v)
		}
		fmt.Printf("\n")
	}
}

// BtIndexReader encapsulates Hash128 to allow concurrent access to Index
type BtIndexReader struct {
	index *BtIndex
}

func NewBtIndexReader(index *BtIndex) *BtIndexReader {
	return &BtIndexReader{
		index: index,
	}
}

// Lookup wraps index Lookup
func (r *BtIndexReader) Lookup(key []byte) uint64 {
	if r.index != nil {
		return r.index.Lookup(key)
	}
	return 0
}

func (r *BtIndexReader) Lookup2(key1, key2 []byte) uint64 {
	fk := make([]byte, 52)
	copy(fk[:length.Addr], key1)
	copy(fk[length.Addr:], key2)

	if r.index != nil {
		return r.index.Lookup(fk)
	}
	return 0
}

func (r *BtIndexReader) Seek(x []byte) (*Cursor, error) {
	if r.index != nil {
		cursor, err := r.index.alloc.seek(x)
		if err != nil {
			return nil, fmt.Errorf("seek key %x: %w", x, err)
		}
		cursor.ix = r.index
		return cursor, nil
	}
	return nil, fmt.Errorf("seek has been failed")
}

func (r *BtIndexReader) Empty() bool {
	return r.index.Empty()
}

type BtIndexWriter struct {
	built           bool
	lvl             log.Lvl
	maxOffset       uint64
	prevOffset      uint64
	delta           uint64
	minDelta        uint64
	batchSizeLimit  uint64
	indexW          *bufio.Writer
	indexF          *os.File
	bucketCollector *etl.Collector // Collector that sorts by buckets
	indexFileName   string
	indexFile       string
	tmpDir          string
	numBuf          [8]byte
	keyCount        uint64
	etlBufLimit     datasize.ByteSize
	bytesPerRec     int
}

type BtIndexWriterArgs struct {
	IndexFile   string // File name where the index and the minimal perfect hash function will be written to
	TmpDir      string
	KeyCount    int
	EtlBufLimit datasize.ByteSize
}

const BtreeLogPrefix = "btree"

// NewBtIndexWriter creates a new BtIndexWriter instance with given number of keys
// Typical bucket size is 100 - 2048, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
// salt parameters is used to randomise the hash function construction, to ensure that different Erigon instances (nodes)
// are likely to use different hash function, to collision attacks are unlikely to slow down any meaningful number of nodes at the same time
func NewBtIndexWriter(args BtIndexWriterArgs) (*BtIndexWriter, error) {
	btw := &BtIndexWriter{}
	btw.tmpDir = args.TmpDir
	btw.indexFile = args.IndexFile
	_, fname := filepath.Split(btw.indexFile)
	btw.indexFileName = fname
	//btw.baseDataID = args.BaseDataID
	btw.etlBufLimit = args.EtlBufLimit
	if btw.etlBufLimit == 0 {
		btw.etlBufLimit = etl.BufferOptimalSize
	}

	btw.bucketCollector = etl.NewCollector(BtreeLogPrefix+" "+fname, btw.tmpDir, etl.NewSortableBuffer(btw.etlBufLimit))
	btw.bucketCollector.LogLvl(log.LvlDebug)
	//btw.offsetCollector = etl.NewCollector(BtreeLogPrefix+" "+fname, btw.tmpDir, etl.NewSortableBuffer(btw.etlBufLimit))
	//btw.offsetCollector.LogLvl(log.LvlDebug)

	btw.maxOffset = 0
	return btw, nil
}

// loadFuncBucket is required to satisfy the type etl.LoadFunc type, to use with collector.Load
func (btw *BtIndexWriter) loadFuncBucket(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
	// k is the BigEndian encoding of the bucket number, and the v is the key that is assigned into that bucket
	//if uint64(len(btw.vals)) >= btw.batchSizeLimit {
	//	if err := btw.drainBatch(); err != nil {
	//		return err
	//	}
	//}

	// if _, err := btw.indexW.Write(k); err != nil {
	// 	return err
	// }
	if _, err := btw.indexW.Write(v[8-btw.bytesPerRec:]); err != nil {
		return err
	}

	//btw.keys = append(btw.keys, binary.BigEndian.Uint64(k), binary.BigEndian.Uint64(k[8:]))
	//btw.vals = append(btw.vals, binary.BigEndian.Uint64(v))
	return nil
}

//
//func (rs *BtIndexWriter) drainBatch() error {
//	// Extend rs.bucketSizeAcc to accomodate current bucket index + 1
//	//for len(rs.bucketSizeAcc) <= int(rs.currentBucketIdx)+1 {
//	//	rs.bucketSizeAcc = append(rs.bucketSizeAcc, rs.bucketSizeAcc[len(rs.bucketSizeAcc)-1])
//	//}
//	//rs.bucketSizeAcc[int(rs.currentBucketIdx)+1] += uint64(len(rs.currentBucket))
//	//// Sets of size 0 and 1 are not further processed, just write them to index
//	//if len(rs.currentBucket) > 1 {
//	//	for i, key := range rs.currentBucket[1:] {
//	//		if key == rs.currentBucket[i] {
//	//			rs.collision = true
//	//			return fmt.Errorf("%w: %x", ErrCollision, key)
//	//		}
//	//	}
//	//	bitPos := rs.gr.bitCount
//	//	if rs.buffer == nil {
//	//		rs.buffer = make([]uint64, len(rs.currentBucket))
//	//		rs.offsetBuffer = make([]uint64, len(rs.currentBucketOffs))
//	//	} else {
//	//		for len(rs.buffer) < len(rs.currentBucket) {
//	//			rs.buffer = append(rs.buffer, 0)
//	//			rs.offsetBuffer = append(rs.offsetBuffer, 0)
//	//		}
//	//	}
//	//	unary, err := rs.recsplit(0 /* level */, rs.currentBucket, rs.currentBucketOffs, nil /* unary */)
//	//	if err != nil {
//	//		return err
//	//	}
//	//	rs.gr.appendUnaryAll(unary)
//	//	if rs.trace {
//	//		fmt.Printf("recsplitBucket(%d, %d, bitsize = %d)\n", rs.currentBucketIdx, len(rs.currentBucket), rs.gr.bitCount-bitPos)
//	//	}
//	//} else {
//	var j int
//	for _, offset := range rs.vals {
//		binary.BigEndian.PutUint64(rs.numBuf[:], offset)
//		rs.indexW.Write(rs.keys[j])
//		if _, err := rs.indexW.Write(rs.numBuf[8-rs.bytesPerRec:]); err != nil {
//			return err
//		}
//	}
//	//}
//	//// Extend rs.bucketPosAcc to accomodate current bucket index + 1
//	//for len(rs.bucketPosAcc) <= int(rs.currentBucketIdx)+1 {
//	//	rs.bucketPosAcc = append(rs.bucketPosAcc, rs.bucketPosAcc[len(rs.bucketPosAcc)-1])
//	//}
//	//rs.bucketPosAcc[int(rs.currentBucketIdx)+1] = uint64(rs.gr.Bits())
//	rs.keys = rs.keys[:0]
//	rs.vals = rs.vals[:0]
//	return nil
//}

// Build has to be called after all the keys have been added, and it initiates the process
// of building the perfect hash function and writing index into a file
func (btw *BtIndexWriter) Build() error {
	tmpIdxFilePath := btw.indexFile + ".tmp"

	if btw.built {
		return fmt.Errorf("already built")
	}
	//if btw.keysAdded != btw.keyCount {
	//	return fmt.Errorf("expected keys %d, got %d", btw.keyCount, btw.keysAdded)
	//}
	var err error
	if btw.indexF, err = os.Create(tmpIdxFilePath); err != nil {
		return fmt.Errorf("create index file %s: %w", btw.indexFile, err)
	}
	defer btw.indexF.Sync()
	defer btw.indexF.Close()
	btw.indexW = bufio.NewWriterSize(btw.indexF, etl.BufIOSize)
	defer btw.indexW.Flush()
	// Write minimal app-specific dataID in this index file
	//binary.BigEndian.PutUint64(btw.numBuf[:], btw.baseDataID)
	//if _, err = btw.indexW.Write(btw.numBuf[:]); err != nil {
	//	return fmt.Errorf("write baseDataID: %w", err)
	//}

	// Write number of keys
	binary.BigEndian.PutUint64(btw.numBuf[:], btw.keyCount)
	if _, err = btw.indexW.Write(btw.numBuf[:]); err != nil {
		return fmt.Errorf("write number of keys: %w", err)
	}
	// Write number of bytes per index record
	btw.bytesPerRec = (bits.Len64(btw.maxOffset) + 7) / 8
	if err = btw.indexW.WriteByte(byte(btw.bytesPerRec)); err != nil {
		return fmt.Errorf("write bytes per record: %w", err)
	}

	defer btw.bucketCollector.Close()
	log.Log(btw.lvl, "[index] calculating", "file", btw.indexFileName)
	if err := btw.bucketCollector.Load(nil, "", btw.loadFuncBucket, etl.TransformArgs{}); err != nil {
		return err
	}

	//if ASSERT {
	//	btw.indexW.Flush()
	//	btw.indexF.Seek(0, 0)
	//	b, _ := io.ReadAll(btw.indexF)
	//	if len(b) != 9+int(btw.keysAdded)*btw.bytesPerRec {
	//		panic(fmt.Errorf("expected: %d, got: %d; btw.keysAdded=%d, btw.bytesPerRec=%d, %s", 9+int(btw.keysAdded)*btw.bytesPerRec, len(b), btw.keysAdded, btw.bytesPerRec, btw.indexFile))
	//	}
	//}

	log.Log(btw.lvl, "[index] write", "file", btw.indexFileName)
	btw.built = true

	_ = btw.indexW.Flush()
	_ = btw.indexF.Sync()
	_ = btw.indexF.Close()
	_ = os.Rename(tmpIdxFilePath, btw.indexFile)
	return nil
}

func (btw *BtIndexWriter) Close() {
	if btw.indexF != nil {
		btw.indexF.Close()
	}
	if btw.bucketCollector != nil {
		btw.bucketCollector.Close()
	}
	//if btw.offsetCollector != nil {
	//	btw.offsetCollector.Close()
	//}
}

// func (btw *BtIndexWriter) Add(key, value []byte) error {

// }

func (btw *BtIndexWriter) AddKey(key []byte, offset uint64) error {
	if btw.built {
		return fmt.Errorf("cannot add keys after perfect hash function had been built")
	}

	binary.BigEndian.PutUint64(btw.numBuf[:], offset)
	if offset > btw.maxOffset {
		btw.maxOffset = offset
	}
	if btw.keyCount > 0 {
		delta := offset - btw.prevOffset
		if btw.keyCount == 1 || delta < btw.minDelta {
			btw.minDelta = delta
		}
	}

	if err := btw.bucketCollector.Collect(key[:], btw.numBuf[:]); err != nil {
		return err
	}
	btw.keyCount++
	btw.prevOffset = offset
	return nil
}

type BtIndex struct {
	alloc       *btAlloc
	mmapWin     *[mmap.MaxMapSize]byte
	mmapUnix    []byte
	data        []byte
	file        *os.File
	size        int64
	modTime     time.Time
	filePath    string
	keyCount    uint64
	baseDataID  uint64
	bytesPerRec int
	dataoffset  uint64

	auxBuf       []byte
	decompressor *compress.Decompressor
	getter       *compress.Getter
}

func BuildBtreeIndex(dataPath, indexPath string) error {
	decomp, err := compress.NewDecompressor(dataPath)
	if err != nil {
		return err
	}

	args := BtIndexWriterArgs{
		IndexFile: indexPath,
		TmpDir:    filepath.Dir(indexPath),
	}

	iw, err := NewBtIndexWriter(args)
	if err != nil {
		return err
	}

	getter := decomp.MakeGetter()
	getter.Reset(0)

	key := make([]byte, 0, 64)

	var pos uint64
	for getter.HasNext() {
		key, _ := getter.Next(key[:0])
		err = iw.AddKey(key[:], uint64(pos))
		if err != nil {
			return err
		}

		pos = getter.Skip()
	}
	decomp.Close()

	if err := iw.Build(); err != nil {
		return err
	}
	iw.Close()
	return nil
}

func OpenBtreeIndex(indexPath, dataPath string, M uint64) (*BtIndex, error) {
	s, err := os.Stat(indexPath)
	if err != nil {
		return nil, err
	}

	idx := &BtIndex{
		filePath: indexPath,
		size:     s.Size(),
		modTime:  s.ModTime(),
		auxBuf:   make([]byte, 64),
	}

	idx.file, err = os.Open(indexPath)
	if err != nil {
		return nil, err
	}

	if idx.mmapUnix, idx.mmapWin, err = mmap.Mmap(idx.file, int(idx.size)); err != nil {
		return nil, err
	}
	idx.data = idx.mmapUnix[:idx.size]

	// Read number of keys and bytes per record
	pos := 8
	idx.keyCount = binary.BigEndian.Uint64(idx.data[:pos])
	//idx.baseDataID = binary.BigEndian.Uint64(idx.data[pos:8])
	idx.bytesPerRec = int(idx.data[pos])
	pos += 1

	// offset := int(idx.keyCount) * idx.bytesPerRec //+ (idx.keySize * int(idx.keyCount))
	// if offset < 0 {
	// 	return nil, fmt.Errorf("offset is: %d which is below zero, the file: %s is broken", offset, indexPath)
	// }

	//p := (*[]byte)(unsafe.Pointer(&idx.data[pos]))
	//l := int(idx.keyCount)*idx.bytesPerRec + (16 * int(idx.keyCount))

	idx.decompressor, err = compress.NewDecompressor(dataPath)
	if err != nil {
		idx.Close()
		return nil, err
	}
	idx.getter = idx.decompressor.MakeGetter()

	idx.alloc = newBtAlloc(idx.keyCount, M)
	idx.alloc.dataLookup = idx.dataLookup
	idx.dataoffset = uint64(pos)
	idx.alloc.traverseDfs()
	idx.alloc.printSearchMx()
	return idx, nil
}

func (b *BtIndex) dataLookup(di uint64) ([]byte, []byte, error) {
	if b.keyCount <= di {
		return nil, nil, fmt.Errorf("ki is greater than key count in index")
	}

	p := b.dataoffset + di*uint64(b.bytesPerRec)
	if uint64(len(b.data)) < p+uint64(b.bytesPerRec) {
		return nil, nil, fmt.Errorf("data lookup gone too far (%d after %d)", p+uint64(b.bytesPerRec)-uint64(len(b.data)), len(b.data))
	}

	offt := b.data[p : p+uint64(b.bytesPerRec)]
	var aux [8]byte
	copy(aux[8-len(offt):], offt)

	offset := binary.BigEndian.Uint64(aux[:])
	b.getter.Reset(offset)
	if !b.getter.HasNext() {
		return nil, nil, fmt.Errorf("pair %d not found", di)
	}

	key, _ := b.getter.Next(nil)

	if !b.getter.HasNext() {
		return nil, nil, fmt.Errorf("pair %d not found", di)
	}
	val, _ := b.getter.Next(nil)
	return key, val, nil
}

func (b *BtIndex) Size() int64 { return b.size }

func (b *BtIndex) ModTime() time.Time { return b.modTime }

// Deprecated
func (b *BtIndex) BaseDataID() uint64 { return b.baseDataID }

func (b *BtIndex) FilePath() string { return b.filePath }

func (b *BtIndex) FileName() string { return path.Base(b.filePath) }

func (b *BtIndex) Empty() bool { return b.keyCount == 0 }

func (b *BtIndex) KeyCount() uint64 { return b.keyCount }

func (b *BtIndex) Close() error {
	if b == nil {
		return nil
	}
	if err := mmap.Munmap(b.mmapUnix, b.mmapWin); err != nil {
		return err
	}
	if err := b.file.Close(); err != nil {
		return err
	}
	return nil
}

func (b *BtIndex) Lookup(key []byte) uint64 {
	cursor, err := b.alloc.seek(key)
	if err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(cursor.value)
}

func (b *BtIndex) OrdinalLookup(i uint64) uint64 {
	//TODO implement me
	panic("implement me")
}

func (b *BtIndex) ExtractOffsets() map[uint64]uint64 {
	//TODO implement me
	panic("implement me")
}

func (b *BtIndex) DisableReadAhead() {
	//TODO implement me
	panic("implement me")
}

func (b *BtIndex) EnableReadAhead() *interface{} {
	//TODO implement me
	panic("implement me")
}

func (b *BtIndex) EnableMadvNormal() *interface{} {
	//TODO implement me
	panic("implement me")
}

func (b *BtIndex) EnableWillNeed() *interface{} {
	//TODO implement me
	panic("implement me")
}
