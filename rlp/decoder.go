package rlp

import (
	"bytes"
	"fmt"
)

type Decoder struct {
	*bytes.Buffer
}

func NewDecoder(buf []byte) *Decoder {
	return &Decoder{
		Buffer: bytes.NewBuffer(buf),
	}
}

func (d *Decoder) List() (*Decoder, error) {
	w := d.Buffer
	// figure out what we are reading
	prefix, err := w.ReadByte()
	if err != nil {
		return nil, err
	}
	token := identifyToken(prefix)
	// switch
	switch token {
	case TokenShortList:
		sz := int(token.Diff(prefix))
		buf, err := nextFull(w, sz)
		if err != nil {
			return nil, err
		}
		return NewDecoder(buf), nil
	case TokenLongList:
		lenSz := int(token.Diff(prefix))
		sz, err := nextBeInt(w, lenSz)
		if err != nil {
			return nil, err
		}
		buf, err := nextFull(w, sz)
		if err != nil {
			return nil, err
		}
		return NewDecoder(buf), nil
	default:
		return nil, fmt.Errorf("%w: List on non-list token", ErrDecode)
	}
}

func DecodeBlob[T any](fn func(*T, []byte) error, receiver *T) func(d *Decoder) error {
	return func(d *Decoder) error {
		// figure out what we are reading
		prefix, err := d.ReadByte()
		if err != nil {
			return err
		}
		token := identifyToken(prefix)
		switch token {
		case TokenDecimal:
			// in this case, the value is just the byte itself
			return fn(receiver, []byte{prefix})
		case TokenShortBlob:
			sz := int(token.Diff(prefix))
			str, err := nextFull(d.Buffer, sz)
			if err != nil {
				return err
			}
			return fn(receiver, str)
		case TokenLongBlob:
			lenSz := int(token.Diff(prefix))
			sz, err := nextBeInt(d.Buffer, lenSz)
			if err != nil {
				return err
			}
			str, err := nextFull(d.Buffer, sz)
			if err != nil {
				return err
			}
			return fn(receiver, str)
		default:
			return fmt.Errorf("%w: DecodeBlob on list token", ErrDecode)
		}
	}
}

func DecodeDecimal[T any](fn func(*T, byte) error, receiver *T) func(d *Decoder) error {
	return func(d *Decoder) error {
		// figure out what we are reading
		prefix, err := d.ReadByte()
		if err != nil {
			return err
		}
		token := identifyToken(prefix)
		switch token {
		case TokenDecimal:
			// in this case, the value is just the byte itself
			return fn(receiver, prefix)
		default:
			return fmt.Errorf("%w: DecodeDecimal on non-decimal token", ErrDecode)
		}
	}
}
