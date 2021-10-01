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

// Implementation of paticia tree for efficient search of substrings from a dictionary in a given string

type Patricia struct {
	leftPrefix  uint32 // Number of bits in the left prefix is encoded into the lower 5 bits, the remaining 27 bits are left prefix
	rightPrefix uint32 // Number of bits in the right prefix is encoding into the lower 5 bits, the remaining 27 bits are right prefix
	leftChild   *Patricia
	rightChild  *Patricia
	value       [][]byte
	size        uint64
}

// Insert adds a new key to the patricia tree (can be called on a nil tree) and returns the new tree
func (p *Patricia) Insert(key []byte) *Patricia {
	return p.insert(key, 0)
}

func computePrefix(key []byte, offsetBits int, bits int) uint32 {
	var prefix uint32
	idx := offsetBits >> 3
	b := bits
	prefixShift := 24 + offsetBits&7
	for b > 0 {
		prefix |= uint32(key[idx]) << prefixShift
		bitsUsed := 8 - offsetBits&7
		b -= bitsUsed
		prefixShift -= bitsUsed
		offsetBits += bitsUsed
		idx++
	}
	// clear 5 lowest bits and write number of bits there
	return prefix&0xffffffe0 | uint32(bits)
}

// insert adds a new key to the patricia tree and returns new tree
// offsetBits is number of bits in the key that needs to be ignored
// as those bits were already used in the higher nodes of the tree
func (p *Patricia) insert(key []byte, offsetBits int) *Patricia {
	if p == nil {
		remainingBits := len(key)*8 - offsetBits
		var pp Patricia
		if remainingBits <= 27 {
			pp.leftPrefix = computePrefix(key, offsetBits, remainingBits)
		} else {
			pp.leftPrefix = computePrefix(key, offsetBits, 27)
			pp.leftChild = pp.insert(key, offsetBits-27)
		}
		return &pp
	}
	leftPrefix := computePrefix(key, offsetBits, int(p.leftPrefix&31))
	rightPrefix := computePrefix(key, offsetBits, int(p.rightPrefix&31))
	return nil
}
