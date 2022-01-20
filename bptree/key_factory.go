package bptree

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

type KeyFactory interface {
	NewUniqueKeyValues(reader *bufio.Reader) KeyValues
	NewUniqueKeys(reader *bufio.Reader) Keys
}

type KeyBinaryFactory struct {
	keySize int
}

func NewKeyBinaryFactory(keySize int) KeyFactory {
	return &KeyBinaryFactory{keySize: keySize}
}

func (factory *KeyBinaryFactory) NewUniqueKeyValues(reader *bufio.Reader) KeyValues {
	kvPairs := factory.readUniqueKeyValues(reader)
	sort.Sort(kvPairs)
	return kvPairs
}

func (factory *KeyBinaryFactory) NewUniqueKeys(reader *bufio.Reader) Keys {
	keys := factory.readUniqueKeys(reader)
	sort.Sort(keys)
	return keys
}

func (factory *KeyBinaryFactory) readUniqueKeyValues(reader *bufio.Reader) KeyValues {
	kvPairs := KeyValues{make([]*Felt, 0), make([]*Felt, 0)}
	keyRegistry := make(map[Felt]bool)
	buffer := make([]byte, BufferSize)
	for {
		bytes_read, err := reader.Read(buffer)
		ensure(err == nil || err == io.EOF, fmt.Sprintf("readUniqueKeyValues: read error %s\n", err))
		if err == io.EOF {
			break
		}
		key_bytes_count := factory.keySize * (bytes_read / factory.keySize)
		duplicated_keys := 0
		for i := 0; i < key_bytes_count; i += factory.keySize {
			key := factory.readKey(buffer, i)
			if _, duplicated := keyRegistry[key]; duplicated {
				duplicated_keys++
				continue
			}
			keyRegistry[key] = true
			value := key // Shortcut: value equal to key
			kvPairs.keys = append(kvPairs.keys, &key)
			kvPairs.values = append(kvPairs.values, &value)
		}
	}
	return kvPairs
}

func (factory *KeyBinaryFactory) readUniqueKeys(reader *bufio.Reader) Keys {
	keys := make(Keys, 0)
	keyRegistry := make(map[Felt]bool)
	buffer := make([]byte, BufferSize)
	for {
		bytes_read, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		key_bytes_count := factory.keySize * (bytes_read / factory.keySize)
		duplicated_keys := 0
		for i := 0; i < key_bytes_count; i += factory.keySize {
			key := factory.readKey(buffer, i)
			if _, duplicated := keyRegistry[key]; duplicated {
				duplicated_keys++
				continue
			}
			keyRegistry[key] = true
			keys = append(keys, Felt(key))
		}
	}
	return keys
}

func (factory *KeyBinaryFactory) readKey(buffer []byte, offset int) Felt {
	keySlice := buffer[offset:offset+factory.keySize]
	switch factory.keySize {
	case 1:
		return Felt(keySlice[0])
	case 2:
		return Felt(binary.BigEndian.Uint16(keySlice))
	case 4:
		return Felt(binary.BigEndian.Uint32(keySlice))
	default:
		return Felt(binary.BigEndian.Uint64(keySlice))
	}
}
