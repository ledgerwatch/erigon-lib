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
	bucketSize       uint64 // Typical values are 100 - 2000, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
	collector        *etl.Collector
	built            bool     // Flag indicating that the hash function has been built and no more keys can be added
	currentBucketIdx uint64   // Current bucket being accumulated
	currentBucket    [][]byte // Keys in the current bucket accumulated before the recsplit is performed for that bucket
}

// NewRecSplit creates a new RecSplit instance with given bucket size
func NewRecSplit(bucketSize int, tmpDir string) *RecSplit {
	rs := &RecSplit{bucketSize: uint64(bucketSize)}
	rs.collector = etl.NewCollector(tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	rs.currentBucket = make([][]byte, 0, bucketSize)
	return rs
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
	binary.BigEndian.PutUint64(bucket[:], remap(hash, rs.bucketSize))
	return rs.collector.Collect(bucket[:], key)
}

// recsplitCurrentBucket applies recSplit algorithm to the current bucket
func (rs *RecSplit) recsplitCurrentBucket() {
	fmt.Printf("recsplit for bucket %d\n", rs.currentBucketIdx)
	for _, key := range rs.currentBucket {
		fmt.Printf("%s\n", key)
	}
	fmt.Printf("----------------\n")
	// clear for the next buckey
	rs.currentBucket = rs.currentBucket[:0]
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
