package rlp

import (
	"fmt"
	"reflect"
)

func unmarshal(data []byte, val any) error {
	rv := reflect.ValueOf(val)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("%w: v must be ptr", ErrDecode)
	}

	v := rv.Elem()

	// read the first byte
	if len(data) == 0 {
		return ErrUnexpectedEOF
	}

	// figure out what we are reading
	token := identifyToken(data[0])

	// switch
	switch token {
	case TokenDecimal:
		// in this case, the value is just the byte itself
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v.SetInt(int64(data[0]))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			v.SetUint(uint64(data[0]))
		default:
			return fmt.Errorf("%w: decimal must be unmarshal into integer type", ErrDecode)
		}
	case TokenShortString:
		sz := int(token.Diff(data[0]))
		if len(data) <= 1+sz {
			return ErrUnexpectedEOF
		}
		dat := data[1 : 1+sz]
		return reflectString(dat, v, rv)
	case TokenLongString:
		lenSz := int(token.Diff(data[0]))
		if len(data) <= 1+lenSz {
			return ErrUnexpectedEOF
		}
		sz, err := BeInt(data, 1, lenSz)
		if err != nil {
			return err
		}
		if len(data) <= 1+sz {
			return ErrUnexpectedEOF
		}
		dat := data[1+lenSz : 1+sz+lenSz]
		return reflectString(dat, v, rv)
	case TokenShortList:
		sz := int(token.Diff(data[0]))
		if len(data) <= 1+sz {
			return ErrUnexpectedEOF
		}
		dat := data[1 : 1+sz]
		return reflectList(dat, v, rv)
	case TokenLongList:
		lenSz := int(token.Diff(data[0]))
		if len(data) <= 1+lenSz {
			return ErrUnexpectedEOF
		}
		sz, err := BeInt(data, 1, lenSz)
		if err != nil {
			return err
		}
		if len(data) <= 1+sz {
			return ErrUnexpectedEOF
		}
		dat := data[1+lenSz : 1+sz+lenSz]
		return reflectList(dat, v, rv)
	case TokenUnknown:
		return fmt.Errorf("%w: unknown token", ErrDecode)
	}
	return nil
}

func reflectString(dat []byte, v reflect.Value, rv reflect.Value) error {
	switch v.Kind() {
	case reflect.String:
		v.SetString(string(dat))
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("%w: need to use uint8 as underlying if want slice output from longstring", ErrDecode)
		}
		v.SetBytes(dat)
	case reflect.Array:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("%w: need to use uint8 as underlying if want array output from longstring", ErrDecode)
		}
		reflect.Copy(v, reflect.ValueOf(dat))
	}

	return nil
}

func reflectList(dat []byte, v reflect.Value, rv reflect.Value) error {
	switch v.Kind() {
	case reflect.Map:
		// TODO: read two elements.
		rv1 := reflect.New(v.Type().Key())
		v1 := rv1.Elem()
		err := reflectString(dat, v1, rv1)
		if err != nil {
			return err
		}
		//TODO: need to advance dat cursor - create helper class
		rv2 := reflect.New(v.Type().Elem())
		v2 := rv1.Elem()
		err = reflectString(dat, v2, rv2)
		if err != nil {
			return err
		}
	case reflect.Array:
		// TODO: read up to N elements
	case reflect.Slice:
		// TODO: read all elements into slice, creating more if needed
	}
	return nil
}
