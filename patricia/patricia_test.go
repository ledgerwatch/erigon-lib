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
	mf := NewMatchFinder2(&pt)
	data := []byte("Who lives here in winter, wolfs")
	matches := mf.FindLongestMatches(data)
	for _, m := range matches {
		fmt.Printf("%+v, match [%s]\n", m, data[m.Start:m.End])
	}
	if len(matches) != 2 {
		t.Errorf("expected matches: %d, got %d", 2, len(matches))
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
	if len(matches) != 2 {
		t.Errorf("expected matches: %d, got %d", 2, len(matches))
	}
}

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

func TestFindMatches3(t *testing.T) {
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
	if len(matches) != 9 {
		t.Errorf("expected matches: %d, got %d", 9, len(matches))
	}
}

func TestFindMatches4(t *testing.T) {
	var pt PatriciaTree
	v := []byte{1}
	pt.Insert(decodeHex("00000000000000000000000000000000000000"), v)
	mf2 := NewMatchFinder2(&pt)
	data := decodeHex("01")
	matches := mf2.FindLongestMatches(data)
	for _, m := range matches {
		fmt.Printf("%+v, match: [%x]\n", m, data[m.Start:m.End])
	}
	if len(matches) != 0 {
		t.Errorf("expected matches: %d, got %d", 0, len(matches))
	}
}

func TestFindMatches5(t *testing.T) {
	var pt PatriciaTree
	v := []byte{1}
	pt.Insert(decodeHex("0434e37673a8e0aaa536828f0d5b0ddba12fece1"), v)
	pt.Insert(decodeHex("e28e72fcf78647adce1f1252f240bbfaebd63bcc"), v)
	pt.Insert(decodeHex("34e28e72fcf78647adce1f1252f240bbfaebd63b"), v)
	pt.Insert(decodeHex("0434e28e72fcf78647adce1f1252f240bbfaebd6"), v)
	pt.Insert(decodeHex("090bdc64a7e3632cde8f4689f47acfc0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("00090bdc64a7e3632cde8f4689f47acfc0760e35bce43af50d4b1f5973463bde"), v)
	pt.Insert(decodeHex("0000000000"), v)
	pt.Insert(decodeHex("00000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000000000000000"), v)
	pt.Insert(decodeHex("0000000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000"), v)
	pt.Insert(decodeHex("0000000000000000"), v)
	pt.Insert(decodeHex("00000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000000000"), v)
	pt.Insert(decodeHex("f47acfc0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("e3632cde8f4689f47acfc0760e35bce43af50d4b"), v)
	pt.Insert(decodeHex("de8f4689f47acfc0760e35bce43af50d4b1f5973"), v)
	pt.Insert(decodeHex("dc64a7e3632cde8f4689f47acfc0760e35bce43a"), v)
	pt.Insert(decodeHex("a7e3632cde8f4689f47acfc0760e35bce43af50d"), v)
	pt.Insert(decodeHex("8f4689f47acfc0760e35bce43af50d4b1f597346"), v)
	pt.Insert(decodeHex("89f47acfc0760e35bce43af50d4b1f5973463bde"), v)
	pt.Insert(decodeHex("64a7e3632cde8f4689f47acfc0760e35bce43af5"), v)
	pt.Insert(decodeHex("632cde8f4689f47acfc0760e35bce43af50d4b1f"), v)
	pt.Insert(decodeHex("4689f47acfc0760e35bce43af50d4b1f5973463b"), v)
	pt.Insert(decodeHex("2cde8f4689f47acfc0760e35bce43af50d4b1f59"), v)
	pt.Insert(decodeHex("0bdc64a7e3632cde8f4689f47acfc0760e35bce4"), v)
	pt.Insert(decodeHex("7acfc0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0000000000000000000000"), v)
	pt.Insert(decodeHex("cfc0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("00000000000000000000000000000000"), v)
	pt.Insert(decodeHex("c0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("00000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("0e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("e43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("1090bdc64a7e3632cde8f4689f47acfc0760e35bce43af50d4b1f5973463bde6"), v)
	pt.Insert(decodeHex("3af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("f50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126f130193790f769"), v)
	pt.Insert(decodeHex("ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db1"), v)
	pt.Insert(decodeHex("df415bb7ae2363ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e"), v)
	pt.Insert(decodeHex("d417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126f130193790f7698ee4"), v)
	pt.Insert(decodeHex("cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126f130193790"), v)
	pt.Insert(decodeHex("c2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126f130193790f7"), v)
	pt.Insert(decodeHex("bb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126"), v)
	pt.Insert(decodeHex("b7ae2363ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7"), v)
	pt.Insert(decodeHex("ae2363ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dc"), v)
	pt.Insert(decodeHex("6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126f1"), v)
	pt.Insert(decodeHex("63ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22d"), v)
	pt.Insert(decodeHex("5f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126f13019"), v)
	pt.Insert(decodeHex("5bb7ae2363ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fc"), v)
	pt.Insert(decodeHex("595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126f130"), v)
	pt.Insert(decodeHex("415bb7ae2363ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28"), v)
	pt.Insert(decodeHex("34df415bb7ae2363ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d303"), v)
	pt.Insert(decodeHex("2363ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf2"), v)
	pt.Insert(decodeHex("0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126f130193790f7698e"), v)
	pt.Insert(decodeHex("07cbc2fc0fd417d3a29f2962b8badecbf4d3036e28fcd7dcf22db126f1301937"), v)
	pt.Insert(decodeHex("0434df415bb7ae2363ecbb6a595f07cbc2fc0fd417d3a29f2962b8badecbf4d3"), v)
	pt.Insert(decodeHex("0d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("fc0fd417d37e04bc63768597761b6c198fd8bd0feded3970bcdafd3adaa9dce4"), v)
	pt.Insert(decodeHex("ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0feded3970"), v)
	pt.Insert(decodeHex("df415bb7ae2363ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c198f"), v)
	pt.Insert(decodeHex("d417d37e04bc63768597761b6c198fd8bd0feded3970bcdafd3adaa9dce41b48"), v)
	pt.Insert(decodeHex("d37e04bc63768597761b6c198fd8bd0feded3970bcdafd3adaa9dce41b48747f"), v)
	pt.Insert(decodeHex("cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0feded3970bcdafd3adaa9"), v)
	pt.Insert(decodeHex("c2fc0fd417d37e04bc63768597761b6c198fd8bd0feded3970bcdafd3adaa9dc"), v)
	pt.Insert(decodeHex("bb6a595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0feded3970bc"), v)
	pt.Insert(decodeHex("b7ae2363ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0f"), v)
	pt.Insert(decodeHex("ae2363ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0fed"), v)
	pt.Insert(decodeHex("6a595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0feded3970bcda"), v)
	pt.Insert(decodeHex("63ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0feded39"), v)
	pt.Insert(decodeHex("5f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0feded3970bcdafd3a"), v)
	pt.Insert(decodeHex("5bb7ae2363ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd"), v)
	pt.Insert(decodeHex("595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0feded3970bcdafd"), v)
	pt.Insert(decodeHex("415bb7ae2363ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8"), v)
	pt.Insert(decodeHex("34df415bb7ae2363ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c19"), v)
	pt.Insert(decodeHex("2363ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0feded"), v)
	pt.Insert(decodeHex("17d37e04bc63768597761b6c198fd8bd0feded3970bcdafd3adaa9dce41b4874"), v)
	pt.Insert(decodeHex("0fd417d37e04bc63768597761b6c198fd8bd0feded3970bcdafd3adaa9dce41b"), v)
	pt.Insert(decodeHex("07cbc2fc0fd417d37e04bc63768597761b6c198fd8bd0feded3970bcdafd3ada"), v)
	pt.Insert(decodeHex("0434df415bb7ae2363ecbb6a595f07cbc2fc0fd417d37e04bc63768597761b6c"), v)
	pt.Insert(decodeHex("df415bb7ae2363ecbb6a595f07cbc2fc0fd417d3"), v)
	pt.Insert(decodeHex("34df415bb7ae2363ecbb6a595f07cbc2fc0fd417"), v)
	pt.Insert(decodeHex("0434df415bb7ae2363ecbb6a595f07cbc2fc0fd4"), v)
	pt.Insert(decodeHex("4b1f5973463bde62"), v)
	pt.Insert(decodeHex("415bb7ae2363ecbb6a595f07cbc2fc0fd417d3"), v)
	pt.Insert(decodeHex("5bb7ae2363ecbb6a595f07cbc2fc0fd417d3"), v)
	pt.Insert(decodeHex("f4689f47acfc0760e35bce43af50d4b1f5973463"), v)
	pt.Insert(decodeHex("e8f4689f47acfc0760e35bce43af50d4b1f59734"), v)
	pt.Insert(decodeHex("cde8f4689f47acfc0760e35bce43af50d4b1f597"), v)
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
	data := decodeHex("9d7d9d7d082073e2920896915d0e0239a7e852d86b26e03a188bc5b947972aeec206d63b6744043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000007bfa482043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000000000011043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000000000002043493d38e72c5281e78f6b364eacac6fa907ecba164000000000000000000000000000000000000000000000000000000000000001e0820a516e4eeef0852f3c4ee0f11237e5e5127ed67a64e43a2f2ebef2d6bc26bb384082073404b8fb6bb42e5a0c9bb7d6253d9d72084bed3991df1efd25512e7f713e796043493d38e72c5281e78f6b364eacac6fa907ecba164000000000000000000000000000000000000000000000000000000000000001f043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000000000012082010db8a472df5096168436e756dbf37edce306a01f4fa7a889f7ad8195e1154a9043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000000000006")
	matches := mf2.FindLongestMatches(data)
	for _, m := range matches {
		fmt.Printf("%+v, match: [%x]\n", m, data[m.Start:m.End])
	}
	if len(matches) != 88 {
		t.Errorf("expected matches: %d, got %d", 88, len(matches))
	}
}
