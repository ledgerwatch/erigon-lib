package rlp

import (
	"fmt"
	"io"
)

type Decoder struct {
	buf *buf
}

func NewDecoder(buf []byte) *Decoder {
	return &Decoder{
		buf: newBuf(buf, 0),
	}
}

func (d *Decoder) Consumed() []byte {
	return d.buf.u[:d.buf.off]
}

func (d *Decoder) Underlying() []byte {
	return d.buf.u
}

func (d *Decoder) Len() int {
	return d.buf.Len()
}

func (d *Decoder) Offset() int {
	return d.buf.Offset()
}

func (d *Decoder) Bytes() []byte {
	return d.buf.Bytes()
}

func (d *Decoder) ReadByte() (n byte, err error) {
	return d.buf.ReadByte()
}

func (d *Decoder) ElemDec() (*Decoder, Token, error) {
	a, t, err := d.Elem()
	return NewDecoder(a), t, err
}

func (d *Decoder) Elem() ([]byte, Token, error) {
	w := d.buf
	// figure out what we are reading
	prefix, err := w.ReadByte()
	if err != nil {
		return nil, TokenUnknown, err
	}
	token := identifyToken(prefix)

	var (
		buf   []byte
		sz    int
		lenSz int
	)
	// switch on the token
	switch token {
	case TokenDecimal:
		// in this case, the value is just the byte itself
		buf = []byte{prefix}
	case TokenShortList:
		sz = int(token.Diff(prefix))
		buf, err = nextFull(w, sz)
	case TokenLongList:
		lenSz = int(token.Diff(prefix))
		sz, err = nextBeInt(w, lenSz)
		if err != nil {
			return nil, token, err
		}
		buf, err = nextFull(w, sz)
	case TokenShortBlob:
		sz := int(token.Diff(prefix))
		buf, err = nextFull(w, sz)
	case TokenLongBlob:
		lenSz := int(token.Diff(prefix))
		sz, err := nextBeInt(w, lenSz)
		if err != nil {
			return nil, token, err
		}
		buf, err = nextFull(w, sz)
	default:
		return nil, token, fmt.Errorf("%w: unknown token", ErrDecode)
	}
	if err != nil {
		return nil, token, err
	}
	return buf, token, nil
}

func ReadElem[T any](d *Decoder, fn func(*T, []byte) error, receiver *T) error {
	buf, token, err := d.Elem()
	if err != nil {
		return err
	}
	switch token {
	case TokenDecimal,
		TokenShortBlob,
		TokenLongBlob,
		TokenShortList,
		TokenLongList:
		return fn(receiver, buf)
	default:
		return fmt.Errorf("%w: ReadElem found unexpected token", ErrDecode)
	}
}

func (d *Decoder) ForList(fn func(*Decoder) error) error {
	// grab the list bytes
	buf, token, err := d.Elem()
	if err != nil {
		return err
	}
	switch token {
	case TokenShortList, TokenLongList:
		dec := NewDecoder(buf)
		for dec.buf.Len() > 0 {
			err := fn(dec)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: ForList on non-list", ErrDecode)
	}
}

type buf struct {
	u   []byte
	off int
}

func newBuf(u []byte, off int) *buf {
	return &buf{u: u, off: off}
}

func (b *buf) empty() bool { return len(b.u) <= b.off }

func (b *buf) PeekByte() (n byte, err error) {
	if len(b.u) <= b.off {
		return 0, io.EOF
	}
	return b.u[b.off], nil
}
func (b *buf) ReadByte() (n byte, err error) {
	if len(b.u) <= b.off {
		return 0, io.EOF
	}
	b.off++
	return b.u[b.off-1], nil
}

func (b *buf) Next(n int) (xs []byte) {
	m := b.Len()
	if n > m {
		n = m
	}
	data := b.u[b.off : b.off+n]
	b.off += n
	return data
}

func (b *buf) Offset() int {
	return b.off
}

func (b *buf) Bytes() []byte { return b.u[b.off:] }

func (b *buf) String() string {
	if b == nil {
		// Special case, useful in debugging.
		return "<nil>"
	}
	return string(b.u[b.off:])
}

func (b *buf) Len() int { return len(b.u) - b.off }
