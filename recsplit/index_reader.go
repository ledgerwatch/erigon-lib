package recsplit

import (
	"sync"

	"github.com/spaolacci/murmur3"
)

// IndexReader encapsulates Hash128 to allow concurrent access to Index
type IndexReader struct {
	mu     sync.RWMutex
	hasher murmur3.Hash128
	index  *Index
}

// NewIndexReader creates new IndexReader
func NewIndexReader(index *Index) *IndexReader {
	return &IndexReader{
		hasher: murmur3.New128WithSeed(index.salt),
		index:  index,
	}
}

func (r *IndexReader) sum(key []byte) (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hasher.Reset()
	r.hasher.Write(key) //nolint:errcheck
	return r.hasher.Sum128()
}

// Lookup wraps index Lookup
func (r *IndexReader) Lookup(key []byte) uint64 {
	bucketHash, fingerprint := r.sum(key)
	if r.index != nil {
		return r.index.Lookup(bucketHash, fingerprint)
	}
	return 0
}
