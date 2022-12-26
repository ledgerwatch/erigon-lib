package bitmapdb

import (
	"bufio"
	"math/bits"
	"os"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/kelindar/bitmap"
	"github.com/ledgerwatch/erigon-lib/mmap"
)

type FixedSizeBitmaps struct {
	bitmap.Bitmap
	f           *os.File
	mmapHandle2 *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	indexFile   string
	data        []byte // slice of correct size for the index to work with
	mmapHandle1 []byte // mmap handle for unix (this is used to close mmap)

	size    int64
	modTime time.Time
}

func OpenFixedSizeBitmaps(indexFile string, bitsPerBitmap int) (*FixedSizeBitmaps, error) {
	idx := &FixedSizeBitmaps{
		indexFile: indexFile,
	}
	var err error
	idx.f, err = os.Open(indexFile)
	if err != nil {
		return nil, err
	}
	var stat os.FileInfo
	if stat, err = idx.f.Stat(); err != nil {
		return nil, err
	}
	idx.size = stat.Size()
	idx.modTime = stat.ModTime()
	if idx.mmapHandle1, idx.mmapHandle2, err = mmap.Mmap(idx.f, int(idx.size)); err != nil {
		return nil, err
	}
	idx.data = idx.mmapHandle1[:idx.size]
	idx.Bitmap = bitmap.FromBytes(idx.data)
	return idx, nil
}

type FixedSizeBitmapsWriter struct {
	bitmap.Bitmap
	f *os.File

	mmapHandle2 *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	indexFile   string
	data        []byte // slice of correct size for the index to work with
	mmapHandle1 []byte // mmap handle for unix (this is used to close mmap)

	w             *bufio.Writer
	size          int
	bitsPerBitmap uint64
}

func NewFixedSizeBitmapsWriter(indexFile string, bitsPerBitmap int, amount uint64) (*FixedSizeBitmapsWriter, error) {
	idx := &FixedSizeBitmapsWriter{
		indexFile:     indexFile,
		bitsPerBitmap: uint64(bitsPerBitmap),
	}
	var err error
	idx.f, err = os.Create(indexFile)
	if err != nil {
		return nil, err
	}
	idx.size = bitsPerBitmap * int(amount)
	if idx.mmapHandle1, idx.mmapHandle2, err = mmap.Mmap(idx.f, int(idx.size)); err != nil {
		return nil, err
	}
	idx.data = idx.mmapHandle1[:idx.size]
	idx.Bitmap = bitmap.FromBytes(idx.data)
	if err := mmap.MadviseNormal(idx.mmapHandle1); err != nil {
		return nil, err
	}
	return idx, nil
}

func (w *FixedSizeBitmapsWriter) AddArray(item uint64, listOfValues []uint64) {
	for _, v := range listOfValues {
		n := item*w.bitsPerBitmap + v
		blkAt, bitAt := int(n/8), int(n%8)
		w.data[blkAt] |= (1 << bitAt)
	}
}

func (w *FixedSizeBitmapsWriter) Build() error {
	if err := w.f.Sync(); err != nil {
		return err
	}
	return nil
}

type RoaringEncodedFixedBitamps struct {
	bm            *roaring64.Bitmap
	mask          *roaring64.Bitmap
	small         *roaring64.Bitmap
	bitsPerBitmap uint64
	i             uint64
}

func NewFixedBitamps(bitsPerBitmap uint64) *RoaringEncodedFixedBitamps {
	return &RoaringEncodedFixedBitamps{bitsPerBitmap: bitsPerBitmap, bm: roaring64.New(), mask: roaring64.New(), small: roaring64.New()}
}
func (l *RoaringEncodedFixedBitamps) AddUint64EncodedBitmap(filesBitmap uint64) {
	for n := bits.TrailingZeros64(filesBitmap); filesBitmap != 0; n = bits.TrailingZeros64(filesBitmap) {
		filesBitmap = (filesBitmap >> (n + 1)) << (n + 1) // clear first N bits
		l.bm.Add(l.bitsPerBitmap*l.i + uint64(n))
	}
	l.i++
}
func (l *RoaringEncodedFixedBitamps) AddArrayUint16(v []uint16) {
	for _, n := range v {
		l.bm.Add(l.bitsPerBitmap*l.i + uint64(n))
	}
	l.i++
}
func (l *RoaringEncodedFixedBitamps) AddArrayUint64(v []uint64) {
	for _, n := range v {
		l.bm.Add(l.bitsPerBitmap*l.i + n)
	}
	l.i++
}
func (l *RoaringEncodedFixedBitamps) At(i int) *roaring64.Bitmap {
	base := uint64(i) * l.bitsPerBitmap
	l.mask.Clear()
	l.mask.AddRange(base, base+l.bitsPerBitmap)
	l.mask.And(l.bm)
	it := l.mask.Iterator()

	//TODO: maybe use roaring.AddOffset
	l.small.Clear()
	for it.HasNext() {
		l.small.Add(it.Next() - base)
	}
	return l.small
}
