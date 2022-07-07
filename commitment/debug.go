package commitment

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

type debugBuffer struct {
	buf *bytes.Buffer
}

func NewDebugBuffer() *debugBuffer {
	return &debugBuffer{buf: new(bytes.Buffer)}
}

func (d *debugBuffer) Write(p []byte) (int, error) {
	return d.buf.Write(p)
}

// Sum appends the current hash to b and returns the resulting slice.
// It does not change the underlying hash state.

func (d *debugBuffer) Sum(b []byte) []byte {
	d.buf.Write(b)
	fmt.Printf("dbuf-sum: %v\n", d.buf.String())
	return nil
}

func (d *debugBuffer) Reset() {
	d.buf.Reset()
}

func (d *debugBuffer) Size() int {
	return d.buf.Len()
}

func (d *debugBuffer) BlockSize() int {
	return 0
}

func (d *debugBuffer) Read(p []byte) (n int, err error) {
	fmt.Printf("dbuf-read: %v\n", hex.EncodeToString(d.buf.Bytes()))
	return 0, nil
}

func newDebugBuffer() *debugBuffer {
	return &debugBuffer{buf: new(bytes.Buffer)}
}

type multiKeccak struct {
	k []keccakState
}

func (m *multiKeccak) Write(p []byte) (n int, err error) {
	for _, k := range m.k {
		n, err = k.Write(p)
		if err != nil {
			return
		}
	}
	return n, nil
}

func (m *multiKeccak) Reset() {
	for _, k := range m.k {
		k.Reset()
	}
}

func (m *multiKeccak) Sum(b []byte) (sum []byte) {
	sum = make([]byte, 0)
	for i, k := range m.k {
		if i == 0 {
			sum = k.Sum(b)
		} else {
			k.Sum(b)
		}
	}
	return sum
}

func (m *multiKeccak) Read(p []byte) (n int, err error) {
	for i, k := range m.k {
		if i == 0 {
			n, err = k.Read(p)
		} else {
			k.Read(nil)
		}
	}
	return
}

func (m *multiKeccak) Size() int {
	return m.k[0].Size()
}

func (m *multiKeccak) BlockSize() int {
	return m.k[0].BlockSize()
}

func MultiKeccak(a, b keccakState) keccakState {
	return &multiKeccak{
		k: []keccakState{a, b},
	}
}
