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
	"encoding/hex"
	"fmt"
	"testing"
)

func TestInserts1(t *testing.T) {
	n := &node{}
	s := makestate(n)
	d := s.transition(0x34, true)
	fmt.Printf("1 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("2 tree:\n%sstate: %s\n\n", n, s)
	d = s.transition(0x56, true)
	fmt.Printf("3 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("4 tree:\n%sstate: %s\n\n", n, s)
	d = s.transition(0xff, true)
	fmt.Printf("5 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("6 tree:\n%sstate: %s\n\n", n, s)
	d = s.transition(0xcc, true)
	fmt.Printf("7 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("8 tree:\n%sstate: %s\n\n", n, s)
	s.insert(nil)
	s = makestate(n)
	d = s.transition(0x34, true)
	fmt.Printf("9 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	d = s.transition(0x66, true)
	fmt.Printf("10 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("11 tree:\n%sstate: %s\n\n", n, s)

	n.insert([]byte{0xff, 0xff, 0xff, 0xff, 0xff}, []byte{0x01})
	fmt.Printf("12 tree:\n%s\n", n)

	n.insert([]byte{0xff, 0xff, 0xff, 0xff, 0x0f}, []byte{0x02})
	fmt.Printf("13 tree:\n%s\n", n)

	n.insert([]byte{0xff, 0xff, 0xff, 0xff, 0xff}, []byte{0x03})
	fmt.Printf("14 tree:\n%s\n", n)

	vs, ok := n.get([]byte{0xff, 0xff, 0xff, 0xff, 0x0f})
	fmt.Printf("15 vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{0xff, 0xff, 0xff, 0xff, 0xff})
	fmt.Printf("16 vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x56})
	fmt.Printf("17 vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{0x34, 0x56, 0xff, 0xcc})
	fmt.Printf("18 vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{})
	fmt.Printf("19 vs = %v, ok = %t\n", vs, ok)
}

func TestInserts2(t *testing.T) {
	var n node
	n.insert([]byte{0xff}, []byte{0x03, 0x03, 0x03, 0x1a, 0xed, 0xed})
	n.insert([]byte{0xed}, []byte{})
	fmt.Printf("tree:\n%s", &n)

	vs, ok := n.get([]byte{0xff})
	fmt.Printf("vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{0xed})
	fmt.Printf("vs = %v, ok = %t\n", vs, ok)
}

func TestFindMatches1(t *testing.T) {
	var pt PatriciaTree
	pt.Insert([]byte("wolf"), []byte{1})
	pt.Insert([]byte("winter"), []byte{2})
	pt.Insert([]byte("wolfs"), []byte{3})
	fmt.Printf("n\n%s", &pt.root)
	mf := NewMatchFinder(&pt)
	data := []byte("Who lives here in winter, wolfs")
	matches := mf.FindLongestMatches(data)
	for _, m := range matches {
		fmt.Printf("%+v, match [%s]\n", m, data[m.Start:m.End])
	}
}

func TestFindMatches2(t *testing.T) {
	var pt PatriciaTree
	pt.Insert([]byte("wolf"), []byte{1})
	pt.Insert([]byte("winter"), []byte{2})
	pt.Insert([]byte("wolfs?"), []byte{3})
	fmt.Printf("n\n%s", &pt.root)
	mf2 := NewMatchFinder2(&pt)
	data := []byte("Who lives here in winter, wolfs?")
	matches := mf2.FindLongestMatches(data)
	for _, m := range matches {
		fmt.Printf("%+v, match: [%s]\n", m, data[m.Start:m.End])
	}
}

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

func TestFundMatches3(t *testing.T) {
	var pt PatriciaTree
	v := []byte{1}
	pt.Insert(decodeHex("00000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("00000000000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000000000000000"), v)
	pt.Insert(decodeHex("0000000000000000000000000000"), v)
	pt.Insert(decodeHex("0100000000000000000000003b30000001000003"), v)
	pt.Insert(decodeHex("0000000000000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("000000000000000000003b300000010000030001"), v)
	pt.Insert(decodeHex("00000000000000000000003b3000000100000300"), v)
	pt.Insert(decodeHex("00000000000000000000000000"), v)
	pt.Insert(decodeHex("00000000000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("0000000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("00000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("0000003b30000001000003000100"), v)
	pt.Insert(decodeHex("00003b30000001000003000100"), v)
	pt.Insert(decodeHex("0100000000000000"), v)
	pt.Insert(decodeHex("003b30000001000003000100"), v)
	pt.Insert(decodeHex("3b30000001000003000100"), v)
	pt.Insert(decodeHex("00000000000000003b3000000100000300010000"), v)
	pt.Insert(decodeHex("0100000000000000000000003a30000001000000"), v)
	pt.Insert(decodeHex("000000003a300000010000000000010010000000"), v)
	pt.Insert(decodeHex("00000000003a3000000100000000000100100000"), v)
	pt.Insert(decodeHex("0000000000003a30000001000000000001001000"), v)
	pt.Insert(decodeHex("000000000000003a300000010000000000010010"), v)
	pt.Insert(decodeHex("00000000000000003a3000000100000000000100"), v)
	pt.Insert(decodeHex("0000000000000000003a30000001000000000001"), v)
	pt.Insert(decodeHex("000000000000000000003a300000010000000000"), v)
	pt.Insert(decodeHex("00000000000000000000003a3000000100000000"), v)
	mf2 := NewMatchFinder2(&pt)
	data := decodeHex("0100000000000000000000003a30000001000000000001001000000044004500")
	matches := mf2.FindLongestMatches(data)
	for _, m := range matches {
		fmt.Printf("%+v, match: [%x]\n", m, data[m.Start:m.End])
	}
}

func TestFundMatches4(t *testing.T) {
	var pt PatriciaTree
	v := []byte{1}
	pt.Insert(decodeHex("00000000000000000000000000000000000000"), v)
	mf2 := NewMatchFinder2(&pt)
	data := decodeHex("01")
	matches := mf2.FindLongestMatches(data)
	for _, m := range matches {
		fmt.Printf("%+v, match: [%x]\n", m, data[m.Start:m.End])
	}
}

func TestFundMatches5(t *testing.T) {
	var pt PatriciaTree
	v := []byte{1}
	pt.Insert(decodeHex("c64a7e3632cde8f4689f47acfc0760e35bce43af"), v)
	pt.Insert(decodeHex("bdc64a7e3632cde8f4689f47acfc0760e35bce43"), v)
	pt.Insert(decodeHex("9f47acfc0760e35bce43af50d4b1f5973463bde6"), v)
	pt.Insert(decodeHex("90bdc64a7e3632cde8f4689f47acfc0760e35bce"), v)
	pt.Insert(decodeHex("7e3632cde8f4689f47acfc0760e35bce43af50d4"), v)
	pt.Insert(decodeHex("689f47acfc0760e35bce43af50d4b1f5973463bd"), v)
	pt.Insert(decodeHex("4a7e3632cde8f4689f47acfc0760e35bce43af50"), v)
	pt.Insert(decodeHex("3632cde8f4689f47acfc0760e35bce43af50d4b1"), v)
	pt.Insert(decodeHex("32cde8f4689f47acfc0760e35bce43af50d4b1f5"), v)
	pt.Insert(decodeHex("b7ae2363ecbb6a595f07cbc2fc0fd417d3"), v)
	pt.Insert(decodeHex("47acfc0760e35bce43af50d4b1f5973463bde6"), v)
	pt.Insert(decodeHex("ae2363ecbb6a595f07cbc2fc0fd417d3"), v)
	pt.Insert(decodeHex("acfc0760e35bce43af50d4b1f5973463bde6"), v)
	pt.Insert(decodeHex("2363ecbb6a595f07cbc2fc0fd417d3"), v)
	pt.Insert(decodeHex("fc0760e35bce43af50d4b1f5973463bde6"), v)
	pt.Insert(decodeHex("63ecbb6a595f07cbc2fc0fd417d3"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000000001"), v)
	pt.Insert(decodeHex("0760e35bce43af50d4b1f5973463bde6"), v)
	pt.Insert(decodeHex("bc63768597761b6c198fd8bd0feded3970bcdafd"), v)
	pt.Insert(decodeHex("97761b6c198fd8bd0feded3970bcdafd3adaa9dc"), v)
	pt.Insert(decodeHex("8fd8bd0feded3970bcdafd3adaa9dce41b48747f"), v)
	pt.Insert(decodeHex("8597761b6c198fd8bd0feded3970bcdafd3adaa9"), v)
	pt.Insert(decodeHex("7e04bc63768597761b6c198fd8bd0feded3970bc"), v)
	pt.Insert(decodeHex("768597761b6c198fd8bd0feded3970bcdafd3ada"), v)
	pt.Insert(decodeHex("761b6c198fd8bd0feded3970bcdafd3adaa9dce4"), v)
	pt.Insert(decodeHex("6c198fd8bd0feded3970bcdafd3adaa9dce41b48"), v)
	pt.Insert(decodeHex("63768597761b6c198fd8bd0feded3970bcdafd3a"), v)
	pt.Insert(decodeHex("1b6c198fd8bd0feded3970bcdafd3adaa9dce41b"), v)
	pt.Insert(decodeHex("198fd8bd0feded3970bcdafd3adaa9dce41b4874"), v)
	pt.Insert(decodeHex("04bc63768597761b6c198fd8bd0feded3970bcda"), v)
	pt.Insert(decodeHex("00000000000000000000000000000000000001"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000000000000000000000000000000002"), v)
	pt.Insert(decodeHex("ecbb6a595f07cbc2fc0fd417d3"), v)
	pt.Insert(decodeHex("60e35bce43af50d4b1f5973463bde6"), v)
	pt.Insert(decodeHex("d8bd0feded3970bcdafd3adaa9dce41b48747f"), v)
	pt.Insert(decodeHex("000000000000000000000000000000000001"), v)
	pt.Insert(decodeHex("60e3997d5a409c25fe09d77351b6"), v)
	pt.Insert(decodeHex("bd0feded3970bcdafd3adaa9dce41b48747f"), v)
	mf2 := NewMatchFinder2(&pt)
	data := decodeHex("1a5f1a5f0434ef514634519d0d610f2cfd020859cb1157b1807cc2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85e0434ef514634519d0d610f2cfd020859cb1157b1807cc2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85c08202d3381848e9412ddc8240cf54b9591609212e0f8144cb1d618d97c0beaa2ab780434ef514634519d0d610f2cfd020859cb1157b1807cc2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85d0434ef514634519d0d610f2cfd020859cb1157b1807c00000000000000000000000000000000000000000000000000000000000000020434ef514634519d0d610f2cfd020859cb1157b1807cc2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85f0434ef514634519d0d610f2cfd020859cb1157b1807cc2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f8600434ef514634519d0d610f2cfd020859cb1157b1807c00000000000000000000000000000000000000000000000000000000000000010434ef514634519d0d610f2cfd020859cb1157b1807c0000000000000000000000000000000000000000000000000000000000000003")
	matches := mf2.FindLongestMatches(data)
	for _, m := range matches {
		fmt.Printf("%+v, match: [%x]\n", m, data[m.Start:m.End])
	}
}
