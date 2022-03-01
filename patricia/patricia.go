/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package patricia

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"

	"github.com/flanglet/kanzi-go/transform"
)

// Implementation of paticia tree for efficient search of substrings from a dictionary in a given string
type node struct {
	p0 uint32 // Number of bits in the left prefix is encoded into the lower 5 bits, the remaining 27 bits are left prefix
	p1 uint32 // Number of bits in the right prefix is encoding into the lower 5 bits, the remaining 27 bits are right prefix
	//size   uint64
	n0  *node
	n1  *node
	val interface{} // value associated with the key
}

func tostr(x uint32) string {
	str := fmt.Sprintf("%b", x)
	for len(str) < 32 {
		str = "0" + str
	}
	return str[:x&0x1f]
}

// print assumes values are byte slices
func (n *node) print(sb *strings.Builder, indent string) {
	sb.WriteString(indent)
	fmt.Fprintf(sb, "%p ", n)
	sb.WriteString(tostr(n.p0))
	sb.WriteString("\n")
	if n.n0 != nil {
		n.n0.print(sb, indent+"    ")
	}
	sb.WriteString(indent)
	fmt.Fprintf(sb, "%p ", n)
	sb.WriteString(tostr(n.p1))
	sb.WriteString("\n")
	if n.n1 != nil {
		n.n1.print(sb, indent+"    ")
	}
	if n.val != nil {
		sb.WriteString(indent)
		sb.WriteString("val:")
		fmt.Fprintf(sb, " %x", n.val.([]byte))
		sb.WriteString("\n")
	}
}

func (n *node) String() string {
	var sb strings.Builder
	n.print(&sb, "")
	return sb.String()
}

// state represent a position anywhere inside patricia tree
// position can be identified by combination of node, and the partitioning
// of that node's p0 or p1 into head and tail.
// As with p0 and p1, head and tail are encoded as follows:
// lowest 5 bits encode the length in bits, and the remaining 27 bits
// encode the actual head or tail.
// For example, if the position is at the beginning of a node,
// head would be zero, and tail would be equal to either p0 or p1,
// depending on whether the position corresponds to going left (0) or right (1).
type state struct {
	n    *node
	head uint32
	tail uint32
}

func (s *state) String() string {
	return fmt.Sprintf("%p head %s tail %s", s.n, tostr(s.head), tostr(s.tail))
}

func (s *state) reset(n *node) {
	s.n = n
	s.head = 0
	s.tail = 0
}

func makestate(n *node) *state {
	return &state{n: n, head: 0, tail: 0}
}

// transition consumes next byte of the key, moves the state to corresponding
// node of the patricia tree and returns divergence prefix (0 if there is no divergence)
func (s *state) transition(b byte, readonly bool) uint32 {
	bitsLeft := 8 // Bits in b to process
	b32 := uint32(b) << 24
	for bitsLeft > 0 {
		if s.head == 0 {
			// tail has not been determined yet, do it now
			if b32&0x80000000 == 0 {
				s.tail = s.n.p0
			} else {
				s.tail = s.n.p1
			}
		}
		if s.tail == 0 {
			// state positioned at the end of the current node
			return b32 | uint32(bitsLeft)
		}
		tailLen := int(s.tail & 0x1f)
		firstDiff := bits.LeadingZeros32(s.tail ^ b32) // First bit where b32 and tail are different
		if firstDiff < bitsLeft {
			// divergence (where the key being searched and the existing structure of patricia tree becomes incompatible) is within currently supplied byte of the search key, b
			if firstDiff >= tailLen {
				// divergence is within currently supplied byte of the search key, b, but outside of the current node
				bitsLeft -= tailLen
				b32 <<= tailLen
				// Need to switch to the next node
				if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
					if s.n.n0 == nil {
						panic("")
					}
					s.n = s.n.n0
				} else {
					if s.n.n1 == nil {
						panic("")
					}
					s.n = s.n.n1
				}
				s.head = 0
				s.tail = 0
			} else {
				// divergence is within currently supplied byte of the search key, b, and within the current node
				bitsLeft -= firstDiff
				b32 <<= firstDiff
				// there is divergence, move head and tail
				mask := ^(uint32(1)<<(32-firstDiff) - 1)
				s.head |= (s.tail & mask) >> (s.head & 0x1f)
				s.head += uint32(firstDiff)
				s.tail = (s.tail&0xffffffe0)<<firstDiff | (s.tail & 0x1f)
				s.tail -= uint32(firstDiff)
				return b32 | uint32(bitsLeft)
			}
		} else if tailLen < bitsLeft {
			// divergence is outside of currently supplied byte of the search key, b
			bitsLeft -= tailLen
			b32 <<= tailLen
			// Switch to the next node
			if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
				if s.n.n0 == nil {
					if readonly {
						return b32 | uint32(bitsLeft)
					}
					s.n.n0 = &node{}
					if b32&0x80000000 == 0 {
						s.n.n0.p0 = b32 | uint32(bitsLeft)
					} else {
						s.n.n0.p1 = b32 | uint32(bitsLeft)
					}
				}
				s.n = s.n.n0
			} else {
				if s.n.n1 == nil {
					if readonly {
						return b32 | uint32(bitsLeft)
					}
					s.n.n1 = &node{}
					if b32&0x80000000 == 0 {
						s.n.n1.p0 = b32 | uint32(bitsLeft)
					} else {
						s.n.n1.p1 = b32 | uint32(bitsLeft)
					}
				}
				s.n = s.n.n1
			}
			s.head = 0
			s.tail = 0
		} else {
			// key byte is consumed, but stay on the same node
			mask := ^(uint32(1)<<(32-bitsLeft) - 1)
			s.head |= (s.tail & mask) >> (s.head & 0x1f)
			s.head += uint32(bitsLeft)
			s.tail = (s.tail&0xffffffe0)<<bitsLeft | (s.tail & 0x1f)
			s.tail -= uint32(bitsLeft)
			bitsLeft = 0
			if s.tail == 0 {
				if s.head&0x80000000 == 0 {
					if s.n.n0 != nil {
						s.n = s.n.n0
						s.head = 0
					}
				} else {
					if s.n.n1 != nil {
						s.n = s.n.n1
						s.head = 0
					}
				}
			}
		}
	}
	return 0
}

func (s *state) diverge(divergence uint32) {
	if s.tail == 0 {
		// try to add to the existing head
		//fmt.Printf("adding divergence to existing head\n")
		dLen := int(divergence & 0x1f)
		headLen := int(s.head & 0x1f)
		d32 := divergence & 0xffffffe0
		//fmt.Printf("headLen %d + dLen %d = %d\n", headLen, dLen, headLen+dLen)
		if headLen+dLen > 27 {
			mask := ^(uint32(1)<<(headLen+5) - 1)
			//fmt.Printf("mask = %b\n", mask)
			s.head |= (d32 & mask) >> headLen
			s.head += uint32(27 - headLen)
			//fmt.Printf("s.head %s\n", tostr(s.head))
			var dn node
			if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
				s.n.p0 = s.head
				s.n.n0 = &dn
			} else {
				s.n.p1 = s.head
				s.n.n1 = &dn
			}
			s.n = &dn
			s.head = 0
			s.tail = 0
			d32 <<= 27 - headLen
			dLen -= (27 - headLen)
			headLen = 0
		}
		//fmt.Printf("headLen %d + dLen %d = %d\n", headLen, dLen, headLen+dLen)
		mask := ^(uint32(1)<<(32-dLen) - 1)
		//fmt.Printf("mask = %b\n", mask)
		s.head |= (d32 & mask) >> headLen
		s.head += uint32(dLen)
		//fmt.Printf("s.head %s\n", tostr(s.head))
		if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
			s.n.p0 = s.head
		} else {
			s.n.p1 = s.head
		}
		return
	}
	// create a new node
	var dn node
	if divergence&0x80000000 == 0 {
		dn.p0 = divergence
		dn.p1 = s.tail
		if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
			dn.n1 = s.n.n0
		} else {
			dn.n1 = s.n.n1
		}
	} else {
		dn.p1 = divergence
		dn.p0 = s.tail
		if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
			dn.n0 = s.n.n0
		} else {
			dn.n0 = s.n.n1
		}
	}
	if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
		s.n.n0 = &dn
		s.n.p0 = s.head
	} else {
		s.n.n1 = &dn
		s.n.p1 = s.head
	}
	s.n = &dn
	s.head = divergence
	s.tail = 0
}

func (n *node) insert(key []byte, value interface{}) {
	s := makestate(n)
	for _, b := range key {
		divergence := s.transition(b, false /* readonly */)
		if divergence != 0 {
			s.diverge(divergence)
		}
	}
	s.insert(value)
}

func (s *state) insert(value interface{}) {
	if s.tail != 0 {
		s.diverge(0)
	}
	if s.head != 0 {
		var dn node
		if s.head&0x80000000 == 0 {
			s.n.n0 = &dn
		} else {
			s.n.n1 = &dn
		}
		s.n = &dn
		s.head = 0
	}
	s.n.val = value
}

func (n *node) get(key []byte) (interface{}, bool) {
	s := makestate(n)
	for _, b := range key {
		divergence := s.transition(b, true /* readonly */)
		//fmt.Printf("get %x, b = %x, divergence = %s\nstate=%s\n", key, b, tostr(divergence), s)
		if divergence != 0 {
			return nil, false
		}
	}
	if s.tail != 0 {
		return nil, false
	}
	return s.n.val, s.n.val != nil
}

type PatriciaTree struct {
	root node
}

func (pt *PatriciaTree) Insert(key []byte, value interface{}) {
	//fmt.Printf("Insert [%x]\n", key)
	pt.root.insert(key, value)
}

func (pt PatriciaTree) Get(key []byte) (interface{}, bool) {
	return pt.root.get(key)
}

type Match struct {
	Start int
	End   int
	Val   interface{}
}

type Matches []Match

func (m Matches) Len() int {
	return len(m)
}

func (m Matches) Less(i, j int) bool {
	return m[i].Start < m[j].Start
}

func (m *Matches) Swap(i, j int) {
	(*m)[i], (*m)[j] = (*m)[j], (*m)[i]
}

type MatchFinder struct {
	s       state
	matches []Match
	pt      *PatriciaTree
}

func NewMatchFinder(pt *PatriciaTree) *MatchFinder {
	return &MatchFinder{pt: pt}
}

type MatchFinder2 struct {
	nodeStack    []*node
	top          *node // Top of nodeStack
	head         uint32
	tail         uint32
	matches      Matches
	divsufsort   *transform.DivSufSort
	pt           *PatriciaTree
	sa, lcp, inv []int32
}

func NewMatchFinder2(pt *PatriciaTree) *MatchFinder2 {
	divsufsort, err := transform.NewDivSufSort()
	if err != nil {
		panic(err)
	}
	return &MatchFinder2{divsufsort: divsufsort, pt: pt, top: &pt.root, nodeStack: []*node{&pt.root}}
}

// unfold consumes next byte of the key, moves the state to corresponding
// node of the patricia tree and returns divergence prefix (0 if there is no divergence)
func (mf2 *MatchFinder2) unfold(b byte) uint32 {
	//fmt.Printf("unfold %b, head = %b, tail = %b, nodeStackLen = %d\n", b, mf2.head, mf2.tail, len(mf2.nodeStack))
	bitsLeft := 8 // Bits in b to process
	b32 := uint32(b) << 24
	for bitsLeft > 0 {
		if mf2.head == 0 {
			// tail has not been determined yet, do it now
			if b32&0x80000000 == 0 {
				mf2.tail = mf2.top.p0
			} else {
				mf2.tail = mf2.top.p1
			}
		}
		if mf2.tail == 0 {
			// state positioned at the end of the current node
			return b32 | uint32(bitsLeft)
		}
		headLen := mf2.head & 0x1f
		tailLen := int(mf2.tail & 0x1f)
		firstDiff := bits.LeadingZeros32(mf2.tail ^ b32) // First bit where b32 and tail are different
		if firstDiff < bitsLeft {
			// divergence (where the key being searched and the existing structure of patricia tree becomes incompatible) is within currently supplied byte of the search key, b
			if firstDiff >= tailLen {
				// divergence is within currently supplied byte of the search key, b, but outside of the current node
				bitsLeft -= tailLen
				b32 <<= tailLen
				// Need to switch to the next node
				if (mf2.head == 0 && mf2.tail&0x80000000 == 0) || (mf2.head != 0 && mf2.head&0x80000000 == 0) {
					if mf2.top.n0 == nil {
						panic("")
					}
					mf2.nodeStack = append(mf2.nodeStack, mf2.top.n0)
					mf2.top = mf2.top.n0
					//fmt.Printf("add node 1, tailLen = %d\n", tailLen)
				} else {
					if mf2.top.n1 == nil {
						panic("")
					}
					mf2.nodeStack = append(mf2.nodeStack, mf2.top.n1)
					mf2.top = mf2.top.n1
					//fmt.Printf("add node 2, tailLen = %d\n", tailLen)
				}
				mf2.head = 0
				mf2.tail = 0
			} else {
				// divergence is within currently supplied byte of the search key, b, and within the current node
				bitsLeft -= firstDiff
				b32 <<= firstDiff
				// there is divergence, move head and tail
				mask := ^(uint32(1)<<(32-firstDiff) - 1)
				mf2.head |= (mf2.tail & 0xffffffe0 & mask) >> headLen
				mf2.head += uint32(firstDiff)
				mf2.tail = (mf2.tail&0xffffffe0)<<firstDiff | uint32(tailLen-firstDiff)
				return b32 | uint32(bitsLeft)
			}
		} else if tailLen < bitsLeft {
			// divergence is outside of currently supplied byte of the search key, b
			bitsLeft -= tailLen
			b32 <<= tailLen
			// Switch to the next node
			if (mf2.head == 0 && mf2.tail&0x80000000 == 0) || (mf2.head != 0 && mf2.head&0x80000000 == 0) {
				if mf2.top.n0 == nil {
					// there is divergence, move head and tail
					mf2.head |= (mf2.tail & 0xffffffe0) >> headLen
					mf2.head += uint32(tailLen)
					mf2.tail = 0
					return b32 | uint32(bitsLeft)
				}
				mf2.nodeStack = append(mf2.nodeStack, mf2.top.n0)
				mf2.top = mf2.top.n0
				//fmt.Printf("add node 3, tailLen = %d\n", tailLen)
			} else {
				if mf2.top.n1 == nil {
					// there is divergence, move head and tail
					mf2.head |= (mf2.tail & 0xffffffe0) >> headLen
					mf2.head += uint32(tailLen)
					mf2.tail = 0
					return b32 | uint32(bitsLeft)
				}
				mf2.nodeStack = append(mf2.nodeStack, mf2.top.n1)
				mf2.top = mf2.top.n1
				//fmt.Printf("add node 4, tailLen = %d\n", tailLen)
			}
			mf2.head = 0
			mf2.tail = 0
		} else {
			// key byte is consumed, but stay on the same node
			mask := ^(uint32(1)<<(32-bitsLeft) - 1)
			mf2.head |= (mf2.tail & 0xffffffe0 & mask) >> headLen
			mf2.head += uint32(bitsLeft)
			mf2.tail = (mf2.tail&0xffffffe0)<<bitsLeft | uint32(tailLen-bitsLeft)
			bitsLeft = 0
			if mf2.tail == 0 {
				if mf2.head&0x80000000 == 0 {
					if mf2.top.n0 != nil {
						mf2.nodeStack = append(mf2.nodeStack, mf2.top.n0)
						mf2.top = mf2.top.n0
						mf2.head = 0
						//fmt.Printf("add node 5, bitsLeft = %d\n", bitsLeft)
					}
				} else {
					if mf2.top.n1 != nil {
						mf2.nodeStack = append(mf2.nodeStack, mf2.top.n1)
						mf2.top = mf2.top.n1
						mf2.head = 0
						//fmt.Printf("add node 6, bitsLeft = %d\n", bitsLeft)
					}
				}
			}
		}
	}
	return 0
}

// unfold moves the match finder back up the stack by specified number of bits
func (mf2 *MatchFinder2) fold(bits int) {
	//fmt.Printf("fold %d, head = %b, tail = %b, nodeStackLen = %d\n", bits, mf2.head, mf2.tail, len(mf2.nodeStack))
	bitsLeft := bits
	for bitsLeft > 0 {
		headLen := int(mf2.head & 0x1f)
		tailLen := int(mf2.tail & 0x1f)
		//fmt.Printf("headLen = %d, bitsLeft = %d, head = %b, tail = %b, nodeStackLen = %d\n", headLen, bitsLeft, mf2.head, mf2.tail, len(mf2.nodeStack))
		if headLen == bitsLeft {
			mf2.head = 0
			mf2.tail = 0
			bitsLeft = 0
		} else if headLen > bitsLeft {
			// folding only affects top node, take bits from end of the head and prepend it to the tail
			mf2.tail = ((mf2.tail & 0xffffffe0) >> bitsLeft) | ((mf2.head & 0xffffffe0) << (headLen - bitsLeft)) | uint32(tailLen+bitsLeft)
			mask := ^(uint32(1)<<(headLen-bitsLeft) - 1)
			mf2.head = (mf2.head & 0xffffffe0 & mask) | uint32(headLen-bitsLeft)
			bitsLeft = 0
		} else {
			// folding affects not only top node, remove top node
			bitsLeft -= headLen
			mf2.nodeStack = mf2.nodeStack[:len(mf2.nodeStack)-1]
			prevTop := mf2.top
			mf2.top = mf2.nodeStack[len(mf2.nodeStack)-1]
			if mf2.top.n0 == prevTop {
				mf2.head = mf2.top.p0
				//fmt.Printf("mf2.head = p0 %b\n", mf2.head)
			} else if mf2.top.n1 == prevTop {
				mf2.head = mf2.top.p1
				//fmt.Printf("mf2.head = p1 %b\n", mf2.head)
			} else {
				panic("")
			}
			mf2.tail = 0
		}
	}
}

func (mf2 *MatchFinder2) FindLongestMatches(data []byte) []Match {
	//fmt.Printf("data=[%x]\n", data)
	mf2.matches = mf2.matches[:0]
	if len(data) < 2 {
		return mf2.matches
	}
	mf2.nodeStack = append(mf2.nodeStack[:0], &mf2.pt.root)
	mf2.top = &mf2.pt.root
	mf2.head = 0
	mf2.tail = 0
	n := len(data)
	if cap(mf2.sa) < n {
		mf2.sa = make([]int32, n)
	} else {
		mf2.sa = mf2.sa[:n]
	}
	mf2.divsufsort.ComputeSuffixArray(data, mf2.sa)
	if cap(mf2.inv) < n {
		mf2.inv = make([]int32, n)
	} else {
		mf2.inv = mf2.inv[:n]
	}
	for i := 0; i < n; i++ {
		mf2.inv[mf2.sa[i]] = int32(i)
	}
	var k int
	// Process all suffixes one by one starting from
	// first suffix in txt[]
	if cap(mf2.lcp) < n {
		mf2.lcp = make([]int32, n)
	} else {
		mf2.lcp = mf2.lcp[:n]
	}
	for i := 0; i < n; i++ {
		/* If the current suffix is at n-1, then we don’t
		   have next substring to consider. So lcp is not
		   defined for this substring, we put zero. */
		if mf2.inv[i] == int32(n-1) {
			k = 0
			continue
		}

		/* j contains index of the next substring to
		   be considered  to compare with the present
		   substring, i.e., next string in suffix array */
		j := int(mf2.sa[mf2.inv[i]+1])

		// Directly start matching from k'th index as
		// at-least k-1 characters will match
		for i+k < n && j+k < n && data[i+k] == data[j+k] {
			k++
		}
		mf2.lcp[mf2.inv[i]] = int32(k) // lcp for the present suffix.

		// Deleting the starting character from the string.
		if k > 0 {
			k--
		}
	}
	//fmt.Printf("sa=[%d]\n", mf2.sa)
	//fmt.Printf("lcp=[%d]\n", mf2.lcp)
	depth := 0 // Depth in bits
	var emitted bool
	var lastMatch *Match
	for i := 0; i < n; i++ {
		// Skip this starting position if a longer suffix containing this one is present
		// lcp[i] is the Longest Common Prefix of suffixes starting from sa[i] and sa[i+1]
		//fmt.Printf("Suffix [%x], depth = %d\n", data[mf2.sa[i]:n], depth)
		if i > 0 {
			// lcp[i-1] is the Longest Common Prefix of suffixes starting from sa[i-1] and sa[i]
			if depth > 8*int(mf2.lcp[i-1]) {
				//fmt.Printf("before fold depth = %d, mf2.lcp[i-1] = %d\n", depth, mf2.lcp[i-1])
				mf2.fold(depth - 8*int(mf2.lcp[i-1]))
				depth = 8 * int(mf2.lcp[i-1])
				//fmt.Printf("after fold depth = %d\n", depth)
			}
		}
		if emitted && lastMatch.End-lastMatch.Start <= depth/8 {
			if cap(mf2.matches) == len(mf2.matches) {
				mf2.matches = append(mf2.matches, Match{})
			} else {
				mf2.matches = mf2.matches[:len(mf2.matches)+1]
			}
			m := &mf2.matches[len(mf2.matches)-1]
			m.Start = int(mf2.sa[i])
			m.End = m.Start + lastMatch.End - lastMatch.Start
			m.Val = lastMatch.Val
			lastMatch = m
			//fmt.Printf("Added new Match 1: %d\n", len(mf2.matches))
		} else {
			emitted = false
		}
		start := int(mf2.sa[i]) + depth/8
		for end := start + 1; end <= n; end++ {
			//fmt.Printf("Looking at [%x], start=%d, depth = %d\n", data[mf2.sa[i]:end], start, depth)
			d := mf2.unfold(data[end-1])
			depth += 8 - int(d&0x1f)
			//fmt.Printf("after unfold depth=%d\n", depth)
			if d != 0 {
				//fmt.Printf("divergence found: %b\n", d)
				break
			}
			if mf2.tail != 0 || mf2.top.val == nil {
				continue
			}
			if !emitted {
				if cap(mf2.matches) == len(mf2.matches) {
					mf2.matches = append(mf2.matches, Match{})
				} else {
					mf2.matches = mf2.matches[:len(mf2.matches)+1]
				}
				lastMatch = &mf2.matches[len(mf2.matches)-1]
				//fmt.Printf("Added new Match 2: %d\n", len(mf2.matches))
				emitted = true
			}
			// This possibly overwrites previous match for the same start position
			lastMatch.Start = int(mf2.sa[i])
			lastMatch.End = end
			lastMatch.Val = mf2.top.val
		}
	}
	if len(mf2.matches) < 2 {
		return mf2.matches
	}
	sort.Sort(&mf2.matches)
	lastEnd := mf2.matches[0].End
	j := 1
	for i, m := range mf2.matches {
		if i > 0 {
			if m.End > lastEnd {
				if i != j {
					mf2.matches[j] = m
				}
				lastEnd = m.End
				j++
			}
		}
	}
	return mf2.matches[:j]
}

func (mf *MatchFinder) FindLongestMatches(data []byte) []Match {
	matchCount := 0
	s := &mf.s
	lastEnd := 0
	for start := 0; start < len(data); start++ {
		s.reset(&mf.pt.root)
		emitted := false
		for end := start + 1; end <= len(data); end++ {
			if d := s.transition(data[end-1], true /* readonly */); d != 0 {
				break
			}
			if s.tail != 0 || s.n.val == nil || end <= lastEnd {
				continue
			}
			var m *Match
			if emitted {
				m = &mf.matches[matchCount-1]
			} else {
				if matchCount == len(mf.matches) {
					mf.matches = append(mf.matches, Match{})
					m = &mf.matches[len(mf.matches)-1]
				} else {
					m = &mf.matches[matchCount]
				}
				matchCount++
				emitted = true
			}
			// This possibly overwrites previous match for the same start position
			m.Start = start
			m.End = end
			m.Val = s.n.val
			lastEnd = end
		}
	}
	return mf.matches[:matchCount]
}
