package state

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path"
	"time"

	"github.com/google/btree"

	"github.com/ledgerwatch/erigon-lib/mmap"
)

type BtIndex struct {
	bt         *btree.BTreeG[uint64]
	mmapWin    *[mmap.MaxMapSize]byte
	mmapUnix   []byte
	data       []byte
	file       *os.File
	size       int64
	modTime    time.Time
	filePath   string
	keyCount   uint64
	baseDataID uint64
}

type page struct {
	i     uint64
	keys  uint64
	size  uint64
	nodes []*node
}

type inode struct {
	page *page
	node *node
}

type cursor struct {
	stack []inode
}

func isEven(n uint64) bool {
	return n&1 == 0
}

type btAlloc struct {
	d       uint64 // depth
	M       uint64 // child limit of any node
	N       uint64
	K       uint64
	vx      []uint64   // vertex count on level
	sons    [][]uint64 // i - level; 0 <= i < d; j_k - amount, j_k+1 - child count
	cursors []cur
	nodes   [][]node
	data    []uint64
	naccess uint64
}

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

func newBtAlloc(k, M uint64) *btAlloc {
	d := logBase(k, M)
	m := max64(2, M>>1)

	fmt.Printf("k=%d d=%d, M=%d m=%d\n", k, d, M, m)
	a := &btAlloc{
		vx:      make([]uint64, d+1),
		sons:    make([][]uint64, d+1),
		cursors: make([]cur, d),
		nodes:   make([][]node, d),
		data:    make([]uint64, k),
		M:       M,
		K:       k,
		d:       d,
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

type cur struct {
	l, p, di, si uint64

	//l - level
	//p - pos inside level
	//si - current, actual son index
	//di - data array index
}

type node struct {
	p, d, s, fc uint64
}

func (a *btAlloc) traverseTrick() {
	for l := 0; l < len(a.sons)-1; l++ {
		if len(a.sons[l]) < 2 {
			panic("invalid btree allocation markup")
		}
		a.cursors[l] = cur{uint64(l), 1, 0, 0}
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
		a.cursors[l] = cur{uint64(l), 1, 0, 0}
		a.nodes[l] = make([]node, 0)
	}

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

func (a *btAlloc) traverse() {
	var sum uint64
	for l := 0; l < len(a.sons)-1; l++ {
		if len(a.sons[l]) < 2 {
			panic("invalid btree allocation markup")
		}
		a.cursors[l] = cur{uint64(l), 1, 0, 0}

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

func (a *btAlloc) fetchByDi(i uint64) (uint64, bool) {
	if int(i) >= len(a.data) {
		return 0, true
	}
	return a.data[i], false
}

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
			di = a.data[m]
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

func (a *btAlloc) bsNode(i, x, l, r uint64, direct bool) (*node, uint64) {
	var exit bool
	var di, lm uint64
	n := new(node)

	for l <= r {
		m := (l + r) >> 1
		if l == r {
			m = l
			exit = true
		}
		lm = m

		switch direct {
		case true:
			di = a.data[m]
		case false:
			di = a.nodes[i][m].d
			n = &a.nodes[i][m]
		}

		mkey, nf := a.fetchByDi(di)
		a.naccess++
		switch {
		case nf:
			break
		case mkey == x:
			return n, m
		case mkey < x:
			//if exit {
			//	break
			//}
			if m+1 == r {
				return n, m
			}
			l = m
		default:
			//if exit {
			//	break
			//}
			if m == l {
				return n, m
			}
			r = m
		}
		if exit {
			break
		}
	}
	return nil, lm
}

type pt struct {
	l, n uint64
}

func (a *btAlloc) findNode(ik uint64) *node {
	var L, m uint64
	R := uint64(len(a.nodes[0]) - 1)

	lhn := new(node)
	for l, level := range a.nodes {
		lhn, m = a.bsNode(uint64(l), ik, L, R, false)
		if lhn == nil {
			L = 0
			fmt.Printf("found nil key %d lvl=%d naccess=%d\n", level[m].d, l, a.naccess)
			break
		}

		k := lhn.d
		//k, found := a.fetchByDi(lhn.d)
		if k > ik {
			if lhn.fc > 0 {
				L = lhn.fc - 1
			} else {
				L = 0
			}
		} else if k == ik {
			fmt.Printf("found key %d naccess=%d\n", level[m].d, a.naccess)
			//return true
			break
		} else {
			if m < uint64(len(level)) {
				R = level[m+1].fc
				//R = level[m].fc
			} else {
				R = uint64(len(a.nodes[l+1]) - 1)
			}
		}
		fmt.Printf("range={%+v} (%d, %d) L=%d naccess=%d\n", lhn, L, R, l, a.naccess)
	}

	if ik < lhn.d {
		L = 0
	} else if ik == lhn.d {
		fmt.Printf("last found key %d naccess=%d\n", lhn.d, a.naccess)
		return nil
	} else {
		L = lhn.d
	}

	a.naccess = 0
	mk, _, found := a.bs(a.d-1, ik, L, a.nodes[a.d-1][m+1].d, true)
	if found {
		//if trace {
		fmt.Printf("last found key %d naccess=%d\n", mk, a.naccess)
		//}
		//return true
	}

	return nil
}

func (a *btAlloc) lookup(ik uint64) bool {
	trace, direct, found := true, false, false
	mk, l, r := uint64(0), uint64(0), uint64(len(a.nodes[0])-1)

	for i := 0; i < len(a.nodes); i++ {
		mk, r, found = a.bs(uint64(i), ik, l, r, direct)
		if found {
			if trace {
				fmt.Printf("found key %d naccess=%d\n", a.nodes[i][mk].d, a.naccess)
			}
			return true
		}
		if trace {
			fmt.Printf("range={%d,%d} (%d, %d) L=%d naccess=%d\n", a.nodes[i][l].d, a.nodes[i][r].d, l, r, i, a.naccess)
		}

		l, r = a.nodes[i][mk].fc, a.nodes[i][r].fc
		if trace && i < len(a.nodes)-1 {
			fmt.Printf("next range={%d,%d} (%d, %d) L=%d naccess=%d\n", a.nodes[i+1][l].d, a.nodes[i+1][r].d, l, r, i+1, a.naccess)
		}
	}

	mindi, maxdi := uint64(0), a.nodes[a.d-1][l+1].d
	if l > 0 {
		mindi = a.nodes[a.d-1][l-1].d
	}
	if trace {
		fmt.Printf("smallest range {%d-%d} (%d-%d)\n", mindi, maxdi, l-1, l+1)
	}

	// search in smallest found interval
	direct = true
	mk, _, found = a.bs(a.d-1, ik, mindi, maxdi, direct)
	if found {
		if trace {
			fmt.Printf("last found key %d naccess=%d\n", mk, a.naccess)
		}
		return true
	}

	return false
}

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
		for _, s := range n {
			fmt.Printf("%d ", s.d)
		}
		fmt.Printf("\n")
	}
}

func OpenBtreeIndex(indexPath string) (*BtIndex, error) {
	s, err := os.Stat(indexPath)
	if err != nil {
		return nil, err
	}

	idx := &BtIndex{
		filePath: indexPath,
		size:     s.Size(),
		modTime:  s.ModTime(),
		//idx:      btree.NewG[uint64](32, commitmentItemLess),
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
	idx.baseDataID = binary.BigEndian.Uint64(idx.data[:8])
	idx.keyCount = binary.BigEndian.Uint64(idx.data[8:16])
	return idx, nil
}

func (b *BtIndex) Size() int64 { return b.size }

func (b *BtIndex) ModTime() time.Time { return b.modTime }

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

func (b *BtIndex) Lookup(bucketHash, fingerprint uint64) uint64 {
	//TODO implement me
	panic("implement me")
}

func (b *BtIndex) OrdinalLookup(i uint64) uint64 {
	//TODO implement me
	panic("implement me")
}

func (b *BtIndex) ExtractOffsets() map[uint64]uint64 {
	//TODO implement me
	panic("implement me")
}

func (b *BtIndex) RewriteWithOffsets(w *bufio.Writer, m map[uint64]uint64) error {
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
