package rlp

import (
	"bytes"
	"fmt"
	"reflect"
)

func Unmarshal(data []byte, val any) error {
	buf := bytes.NewBuffer(data)
	return unmarshal(buf, val)

}
func unmarshal(buf *bytes.Buffer, val any) error {
	rv := reflect.ValueOf(val)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("%w: v must be ptr", ErrDecode)
	}
	v := rv.Elem()
	err := reflectAny(buf, v, rv)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDecode, err)
	}
	return nil
}

func nextFull(dat *bytes.Buffer, size int) ([]byte, error) {
	d := dat.Next(size)
	if len(d) != size {
		return nil, ErrUnexpectedEOF
	}
	return d, nil
}

// BeInt parses Big Endian representation of an integer from given payload at given position
func decodeBeInt(w *bytes.Buffer, length int) (int, error) {
	var r int
	dat, err := nextFull(w, length)
	if err != nil {
		return 0, ErrUnexpectedEOF
	}
	if length > 0 && dat[0] == 0 {
		return 0, fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: %x", ErrParse, dat)
	}
	for _, b := range dat[0:length] {
		r = (r << 8) | int(b)
	}
	return r, nil
}

func reflectAny(w *bytes.Buffer, v reflect.Value, rv reflect.Value) error {
	// figure out what we are reading
	prefix, err := w.ReadByte()
	if err != nil {
		return err
	}
	token := identifyToken(prefix)
	// switch
	switch token {
	case TokenDecimal:
		// in this case, the value is just the byte itself
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v.SetInt(int64(prefix))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			v.SetUint(uint64(prefix))
		case reflect.Invalid:
			// do nothing
		default:
			return fmt.Errorf("%w: decimal must be unmarshal into integer type", ErrDecode)
		}
	case TokenShortString:
		sz := int(token.Diff(prefix))
		str, err := nextFull(w, sz)
		if err != nil {
			return err
		}
		return putString(str, v, rv)
	case TokenLongString:
		lenSz := int(token.Diff(prefix))
		sz, err := decodeBeInt(w, lenSz)
		if err != nil {
			return err
		}
		str, err := nextFull(w, sz)
		if err != nil {
			return err
		}
		return putString(str, v, rv)
	case TokenShortList:
		sz := int(token.Diff(prefix))
		buf, err := nextFull(w, sz)
		if err != nil {
			return err
		}
		return reflectList(bytes.NewBuffer(buf), v, rv)
	case TokenLongList:
		lenSz := int(token.Diff(prefix))
		sz, err := decodeBeInt(w, lenSz)
		if err != nil {
			return err
		}
		buf, err := nextFull(w, sz)
		if err != nil {
			return err
		}
		return reflectList(bytes.NewBuffer(buf), v, rv)
	case TokenUnknown:
		return fmt.Errorf("%w: unknown token", ErrDecode)
	}
	return nil
}

func putString(w []byte, v reflect.Value, rv reflect.Value) error {
	switch v.Kind() {
	case reflect.String:
		v.SetString(string(w))
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("%w: need to use uint8 as underlying if want slice output from longstring", ErrDecode)
		}
		v.SetBytes(w)
	case reflect.Array:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("%w: need to use uint8 as underlying if want array output from longstring", ErrDecode)
		}
		reflect.Copy(v, reflect.ValueOf(w))
	case reflect.Invalid:
		// do nothing
		return nil
	}
	return nil
}

func reflectList(w *bytes.Buffer, v reflect.Value, rv reflect.Value) error {
	switch v.Kind() {
	case reflect.Invalid:
		// do nothing
		return nil
	case reflect.Map:
		rv1 := reflect.New(v.Type().Key())
		v1 := rv1.Elem()
		err := reflectAny(w, v1, rv1)
		if err != nil {
			return err
		}
		rv2 := reflect.New(v.Type().Elem())
		v2 := rv2.Elem()
		err = reflectAny(w, v2, rv2)
		if err != nil {
			return err
		}
		v.SetMapIndex(rv1, rv2)
	case reflect.Array, reflect.Slice:
		idx := 0
		for {
			if idx >= v.Cap() {
				v.Grow(1)
			}
			if idx >= v.Len() {
				v.SetLen(idx + 1)
			}
			if idx < v.Len() {
				// Decode into element.
				rv1 := v.Index(idx)
				v1 := rv1.Elem()
				err := reflectAny(w, v1, rv1)
				if err != nil {
					return err
				}
			} else {
				// Ran out of fixed array: skip.
				rv1 := reflect.Value{}
				err := reflectAny(w, rv1, rv1)
				if err != nil {
					return err
				}
			}
			idx++
		}
	}
	return nil
}
