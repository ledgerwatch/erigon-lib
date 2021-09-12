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

package recsplit

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/spaolacci/murmur3"
)

const RecSplitLogPrefix = "recsplit"

// RecSplit is the implementation of Recursive Split algorithm for constructing perfect hash mapping, described in
// https://arxiv.org/pdf/1910.06416.pdf Emmanuel Esposito, Thomas Mueller Graf, and Sebastiano Vigna.
// Recsplit: Minimal perfect hashing via recursive splitting. In 2020 Proceedings of the Symposium on Algorithm Engineering and Experiments (ALENEX),
// pages 175−185. SIAM, 2020.
type RecSplit struct {
	keyExpectedCount uint64 // Number of keys in the hash table
	keyCount         uint64 // Number of keys actually added to the recSplit (to check the match with keyExpectedCount)
	bucketCount      uint64 // Number of buckets
	collector        *etl.Collector
	built            bool     // Flag indicating that the hash function has been built and no more keys can be added
	currentBucketIdx uint64   // Current bucket being accumulated
	currentBucket    [][]byte // Keys in the current bucket accumulated before the recsplit is performed for that bucket
	builder          Builder
	bucketSizeAcc    []int // Bucket size accumulator
	bucketPosAcc     []int // Accumulator for position of every bucket in the encoding of the hash function
}

// NewRecSplit creates a new RecSplit instance with given number of keys and given bucket size
// Typical bucket size is 100 - 2000, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
func NewRecSplit(keyCount, bucketSize int, tmpDir string) *RecSplit {
	rs := &RecSplit{keyExpectedCount: uint64(keyCount), bucketCount: uint64((keyCount + bucketSize - 1) / bucketSize)}
	rs.collector = etl.NewCollector(tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	rs.currentBucket = make([][]byte, 0, bucketSize)
	rs.bucketSizeAcc = make([]int, 1)
	rs.bucketPosAcc = make([]int, 1)
	return rs
}

// Builder builds up the representation of the hash function and is capable of then outputting
// the compact endoing of this representation.
type Builder struct {
}

func (b *Builder) appendUnaryAll(unary []uint32) {

}

func (b Builder) bits() int {
	return 0
}

// remap converts the number x which is assumed to be uniformly distributed over the range [0..2^64) to the number that is uniformly
// distributed over the range [0..n)
func remap(x uint64, n uint64) uint64 {
	hi, _ := bits.Mul64(x, n)
	return hi
}

// Add key to the RecSplit. There can be many more keys than what fits in RAM, and RecSplit
// spills data onto disk to accomodate that. The key gets copied by the collector, therefore
// the slice underlying key is not getting accessed by RecSplit after this invocation.
func (rs *RecSplit) AddKey(key []byte) error {
	if rs.built {
		return fmt.Errorf("cannot add keys after perfect hash function had been built")
	}
	hash := murmur3.Sum64(key)
	var bucket [8]byte
	binary.BigEndian.PutUint64(bucket[:], remap(hash, rs.bucketCount))
	rs.keyCount++
	return rs.collector.Collect(bucket[:], key)
}

func (rs RecSplit) recsplitCurrentBucket() {
	// Extend rs.bucketSizeAcc to accomodate current bucket index + 1
	for len(rs.bucketSizeAcc) <= int(rs.currentBucketIdx)+1 {
		rs.bucketSizeAcc = append(rs.bucketSizeAcc, rs.bucketSizeAcc[len(rs.bucketSizeAcc)-1])
	}
	rs.bucketSizeAcc[int(rs.currentBucketIdx)+1] += len(rs.currentBucket)
	if len(rs.currentBucket) > 1 {
		unary := rs.recsplit(rs.currentBucket, nil /* unary */)
		rs.builder.appendUnaryAll(unary)
	}
	// Extend rs.bucketPosAcc to accomodate current bucket index + 1
	for len(rs.bucketPosAcc) <= int(rs.currentBucketIdx)+1 {
		rs.bucketPosAcc = append(rs.bucketPosAcc, rs.bucketPosAcc[len(rs.bucketPosAcc)-1])
	}
	rs.bucketPosAcc[int(rs.currentBucketIdx)+1] = rs.builder.bits()
	// clear for the next buckey
	rs.currentBucket = rs.currentBucket[:0]
}

// recsplit applies recSplit algorithm to the given bucket
func (rs *RecSplit) recsplit(bucket [][]byte, unary []uint32) []uint32 {
	fmt.Printf("recsplit for bucket %d\n", rs.currentBucketIdx)
	for _, key := range rs.currentBucket {
		fmt.Printf("%s\n", key)
	}
	fmt.Printf("----------------\n")
	return unary
}

// loadFunc is required to satisfy the type etl.LoadFunc type, to use with collector.Load
func (rs *RecSplit) loadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	// k is the BigEndian encoding of the bucket number, and the v is the key that is assigned into that bucket
	bucketIdx := binary.BigEndian.Uint64(k)
	if rs.currentBucketIdx != bucketIdx {
		if rs.currentBucketIdx != math.MaxUint64 {
			rs.recsplitCurrentBucket()
		}
		rs.currentBucketIdx = bucketIdx
	}
	rs.currentBucket = append(rs.currentBucket, v)
	return nil
}

// Build has to be called after all the keys have been added, and it initiates the process
// of building the perfect hash function.
func (rs *RecSplit) Build() error {
	if rs.built {
		return fmt.Errorf("already built")
	}
	if rs.keyCount != rs.keyExpectedCount {
		return fmt.Errorf("expected keys %d, got %d", rs.keyExpectedCount, rs.keyCount)
	}
	rs.currentBucketIdx = math.MaxUint64 // To make sure 0 bucket is detected
	defer rs.collector.Close(RecSplitLogPrefix)
	if err := rs.collector.Load(RecSplitLogPrefix, nil /* db */, "" /* toBucket */, rs.loadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	if len(rs.currentBucket) > 0 {
		rs.recsplitCurrentBucket()
	}
	rs.built = true
	return nil
}
