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
		switch v.Kind() {
		case reflect.String:
			v.SetString(string(dat))
		case reflect.Slice:
			if v.Type().Elem().Kind() != reflect.Uint8 {
				return fmt.Errorf("%w: need to use uint8 as underlying if want slice output from shortstring", ErrDecode)
			}
			v.SetBytes(dat)
		case reflect.Array:
			if v.Type().Elem().Kind() != reflect.Uint8 {
				return fmt.Errorf("%w: need to use uint8 as underlying if want array output from shortstring", ErrDecode)
			}
			reflect.Copy(v, reflect.ValueOf(dat))
		}
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
	case TokenShortList:
	case TokenLongList:
	case TokenUnknown:
		return fmt.Errorf("%w: unknown token", ErrDecode)
	}
	return nil
}
