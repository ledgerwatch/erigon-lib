package types

// Minimal Blob Transaction parser for txpool purposes

import (
	"encoding/binary"
	"fmt"

	"github.com/holiman/uint256"
	"github.com/protolambda/go-kzg/eth"
)

const (
	FieldElementsPerBlob = 4096

	FieldElementSize = 32

	BlobSize = FieldElementsPerBlob * FieldElementSize // blob size in bytes

	ProofSize = 48 // kzg proof size

	MaxBlobsPerBlock = 4
)

type wrapper struct {
	proof                    eth.KZGProof
	sigOffset                int // where the 65 byte signature starts in the payload
	sigHashStart, sigHashEnd int // the portion of the payload to hash to get the signing hash

	chainID                uint256.Int
	nonce                  uint64
	maxPriorityFeePerGas   uint256.Int
	maxFeePerGas           uint256.Int
	gas                    uint64
	creation               bool
	value                  uint256.Int
	dataLen                int
	dataNonZeroLen         int
	maxFeePerDataGas       uint256.Int
	accessListAddressCount int
	accessListKeyCount     int // number of storage keys in all access lists

	blobHashesOffset int
	numBlobHashes    int

	blobKzgsOffset int
	numBlobKzgs    int

	blobsOffset int
	numBlobs    int
}

type wrapperBlobSequence struct {
	payload []byte
	num     int
}
type wrapperKzgSequence struct {
	payload []byte
	num     int
}
type wrapperBlob []byte

func (s wrapperBlobSequence) Len() int {
	return s.num
}
func (s wrapperBlobSequence) At(i int) eth.Blob {
	if i >= s.num {
		return nil
	}
	r := wrapperBlob(s.payload[i*BlobSize : i*BlobSize+BlobSize])
	return &r

}
func (s wrapperKzgSequence) Len() int {
	return s.num
}
func (s wrapperKzgSequence) At(i int) eth.KZGCommitment {
	kzg := eth.KZGCommitment{}
	if i >= s.num {
		return kzg
	}
	copy(kzg[:], s.payload[i*ProofSize:i*ProofSize+ProofSize])
	return kzg
}

func (s wrapperBlob) Len() int { return len(s) / FieldElementSize }
func (s wrapperBlob) At(i int) [FieldElementSize]byte {
	r := [FieldElementSize]byte{}
	if i*FieldElementSize+FieldElementSize > len(s) {
		return r
	}
	copy(r[:], s[i*FieldElementSize:i*FieldElementSize+FieldElementSize])
	return r
}

func readUint256(payload []byte, offset int) (uint256.Int, error) {
	if len(payload) < offset+32 {
		return uint256.Int{}, fmt.Errorf("payload too short len=%v offset=%v", len(payload), offset)
	}
	r := uint256.Int{}
	data := payload[offset : offset+32]
	r[0] = binary.LittleEndian.Uint64(data[0:8])
	r[1] = binary.LittleEndian.Uint64(data[8:16])
	r[2] = binary.LittleEndian.Uint64(data[16:24])
	r[3] = binary.LittleEndian.Uint64(data[24:32])
	return r, nil
}

func readUint64(payload []byte, offset int) (uint64, error) {
	if len(payload) < offset+8 {
		return 0, fmt.Errorf("payload too short len=%v offset=%v", len(payload), offset)
	}
	return binary.LittleEndian.Uint64(payload[offset : offset+8]), nil
}

func readUint32(payload []byte, offset int) (uint32, error) {
	if len(payload) < offset+4 {
		return 0, fmt.Errorf("payload too short len=%v offset=%v", len(payload), offset)
	}
	return binary.LittleEndian.Uint32(payload[offset : offset+4]), nil
}

func readOffset(payload []byte, offset int, end int, prevOffset int) (int, error) {
	v, err := readUint32(payload, offset)
	r := int(v)
	if r > end {
		return 0, fmt.Errorf("Bad offset at %v: %v > %v", offset, v, end)
	}
	if r < prevOffset {
		return 0, fmt.Errorf("Bad offset at %v: %v < %v", offset, v, prevOffset)
	}
	return r, err
}

func readLength(payload []byte, offset int, limit int) (int, error) {
	v, err := readUint32(payload, offset)
	if err != nil {
		return 0, err
	}
	r := int(v)
	if r%4 != 0 {
		return 0, fmt.Errorf("length invalid, expected multiple of 4, got: %d", r)
	}
	r /= 4
	if r > limit {
		return 0, fmt.Errorf("length exceeds limit: %v > %v", r, limit)
	}
	return r, nil
}

func (w *wrapper) Deserialize(payload []byte) error {
	end := len(payload)
	txOffset, err := readOffset(payload, 0, end, 0)
	if err != nil {
		return err
	}
	kzgsOffset, err := readOffset(payload, 4, end, txOffset)
	if err != nil {
		return err
	}

	blobsOffset, err := readOffset(payload, 8, end, kzgsOffset)
	if err != nil {
		return err
	}

	w.blobKzgsOffset = kzgsOffset
	w.numBlobKzgs = blobsOffset - kzgsOffset
	if w.numBlobKzgs%ProofSize != 0 {
		return fmt.Errorf("expected multiple of proofsize, got: %v", w.numBlobKzgs)
	}
	w.numBlobKzgs /= ProofSize

	w.blobsOffset = blobsOffset
	w.numBlobs = len(payload) - blobsOffset
	if w.numBlobs%BlobSize != 0 {
		return fmt.Errorf("expected multiple of blobsize, got: %v", w.numBlobs)
	}
	w.numBlobs /= BlobSize

	if len(payload) < 12+ProofSize {
		return fmt.Errorf("payload too short: %v", len(payload))
	}
	copy(w.proof[:], payload[12:12+ProofSize])

	err = w.DeserializeTx(payload, txOffset, kzgsOffset)
	if err != nil {
		return err
	}

	return nil
}

func (w *wrapper) DeserializeTx(payload []byte, begin, end int) error {
	messageOffset, err := readOffset(payload, begin, end, 0)
	if err != nil {
		return err
	}
	w.sigOffset = begin + 4
	return w.DeserializeMessage(payload, begin+messageOffset, end)
}

func (w *wrapper) DeserializeMessage(payload []byte, begin, end int) error {
	w.sigHashStart = begin
	w.sigHashEnd = end
	originalBegin := begin
	var err error
	w.chainID, err = readUint256(payload, begin)
	if err != nil {
		return err
	}
	begin += 32

	w.nonce, err = readUint64(payload, begin)
	if err != nil {
		return err
	}
	begin += 8

	w.maxPriorityFeePerGas, err = readUint256(payload, begin)
	if err != nil {
		return err
	}
	begin += 32

	w.maxFeePerGas, err = readUint256(payload, begin)
	if err != nil {
		return err
	}
	begin += 32

	w.gas, err = readUint64(payload, begin)
	if err != nil {
		return err
	}
	begin += 8

	addressOffset, err := readOffset(payload, begin, end, 0)
	if err != nil {
		return err
	}
	begin += 4

	w.value, err = readUint256(payload, begin)
	if err != nil {
		return err
	}
	begin += 32

	dataOffset, err := readOffset(payload, begin, end, addressOffset)
	if err != nil {
		return err
	}
	begin += 4

	accessListOffset, err := readOffset(payload, begin, end, dataOffset)
	if err != nil {
		return err
	}
	begin += 4

	w.dataLen = accessListOffset - dataOffset
	w.dataNonZeroLen = 0
	for i := dataOffset; i < accessListOffset; i++ {
		if payload[i+originalBegin] != 0 {
			w.dataNonZeroLen++
		}
	}

	len := dataOffset - addressOffset
	if len == 1 {
		w.creation = true
	} else if len != 21 {
		return fmt.Errorf("expected 1 or 21 bytes for address, got %v", len)
	} else {
		w.creation = false
	}

	w.maxFeePerDataGas, err = readUint256(payload, begin)
	if err != nil {
		return err
	}
	begin += 32

	blobHashesOffset, err := readOffset(payload, begin, end, accessListOffset)
	if err != nil {
		return err
	}
	begin += 4

	w.blobHashesOffset = originalBegin + blobHashesOffset
	w.numBlobHashes = end - w.blobHashesOffset
	if w.numBlobHashes%32 != 0 {
		return fmt.Errorf("expected multiple of 32, got: %v", w.numBlobHashes)
	}
	w.numBlobHashes /= 32

	err = w.DeserializeAccessList(payload, originalBegin+accessListOffset, originalBegin+blobHashesOffset)
	if err != nil {
		return err
	}
	begin += 4

	return nil
}

func (w *wrapper) DeserializeAccessList(payload []byte, begin, end int) error {
	originalBegin := begin
	length, err := readLength(payload, begin, 1<<24)
	if err != nil {
		return err
	}
	w.accessListAddressCount = length
	begin += 4

	offset := length * 4
	nextOffset := 0
	for i := 0; i < length; i++ {
		if i == length-1 {
			nextOffset = end - originalBegin
		} else {
			nextOffset, err = readOffset(payload, begin, end, offset)
			if err != nil {
				return err
			}
			begin += 4
		}
		// an access list tuple consists of 20 bytes for the address, and then 4 bytes for the
		// "offset", followed by the list of 32-byte storage keys.
		keyLenBuf := nextOffset - offset - 24
		if keyLenBuf%32 != 0 {
			return fmt.Errorf("key list not a multiple of 32, got: %v", keyLenBuf)
		}
		w.accessListKeyCount += (keyLenBuf / 32)
		offset = nextOffset
	}
	return nil
}

func (w *wrapper) VerifyBlobs(payload []byte) error {
	blobs := wrapperBlobSequence{payload: payload[w.blobsOffset:], num: w.numBlobs}
	kzgs := wrapperKzgSequence{payload: payload[w.blobKzgsOffset:], num: w.numBlobKzgs}
	l1 := blobs.num
	l2 := kzgs.num
	l3 := w.numBlobHashes
	if l1 != l2 || l2 != l3 {
		return fmt.Errorf("lengths don't match %v %v %v", l1, l2, l3)
	}
	if l1 > MaxBlobsPerBlock {
		return fmt.Errorf("number of blobs exceeds max: %v", l1)
	}

	for i := 0; i < l3; i++ {
		computed := eth.KZGToVersionedHash(kzgs.At(i))
		h := [32]byte{}
		offset := w.blobHashesOffset
		copy(h[:], payload[offset+i*32:offset+i*32+32])
		if computed != h {
			return fmt.Errorf("versioned hash %d supposedly %x but does not match computed %x", i, h, computed)
		}
	}

	ok, err := eth.VerifyAggregateKZGProof(blobs, kzgs, w.proof)
	if err != nil {
		return fmt.Errorf("error during proof verification: %v", err)
	}
	if !ok {
		return fmt.Errorf("failed to verify kzg")
	}

	return nil
}
