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
	"testing"
)

func TestInserts(t *testing.T) {
	n := &node{}
	s := makestate(n)
	d := s.transition(0x34)
	fmt.Printf("1 tree:\n%s\nstate: %s\ndivergence %s\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("2 tree:\n%s\nstate: %s\n", n, s)
	d = s.transition(0x56)
	fmt.Printf("3 tree:\n%s\nstate: %s\ndivergence %s\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("4 tree:\n%s\nstate: %s\n", n, s)
	d = s.transition(0xff)
	fmt.Printf("5 tree:\n%s\nstate: %s\ndivergence %s\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("6 tree:\n%s\nstate: %s\n", n, s)
	d = s.transition(0xcc)
	fmt.Printf("7 tree:\n%s\nstate: %s\ndivergence %s\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("8 tree:\n%s\nstate: %s\n", n, s)
	s = makestate(n)
	d = s.transition(0x34)
	fmt.Printf("9 tree:\n%s\nstate: %s\ndivergence %s\n", n, s, tostr(d))
	d = s.transition(0x66)
	fmt.Printf("10 tree:\n%s\nstate: %s\ndivergence %s\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("11 tree:\n%s\nstate: %s\n", n, s)

	n.insert([]byte{0xff, 0xff, 0xff, 0xff, 0xff})
	fmt.Printf("12 tree:\n%s\n", n)

	n.insert([]byte{0xff, 0xff, 0xff, 0xff, 0x0f})
	fmt.Printf("12 tree:\n%s\n", n)
}
