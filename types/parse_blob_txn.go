package types

// Minimal Blob Transaction parser for txpool purposes

import (
	"encoding/binary"
	"fmt"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/crypto/kzg"
)

const (
	FieldElementsPerBlob = 4096

	FieldElementSize = 32

	BlobSize = FieldElementsPerBlob * FieldElementSize // blob size in bytes

	ProofSize = 48 // kzg proof & commitment size

	MaxBlobsPerBlock = 4
)

type wrapper struct {
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

	commitmentsOffset int
	numCommitments    int

	blobsOffset int
	numBlobs    int

	proofsOffset int
	numProofs    int
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
	pos := 0
	end := len(payload)
	txOffset, err := readOffset(payload, pos, end, 0)
	if err != nil {
		return err
	}
	pos += 4
	commitmentsOffset, err := readOffset(payload, pos, end, txOffset)
	if err != nil {
		return err
	}
	pos += 4
	blobsOffset, err := readOffset(payload, pos, end, commitmentsOffset)
	if err != nil {
		return err
	}
	pos += 4
	proofsOffset, err := readOffset(payload, pos, end, blobsOffset)
	if err != nil {
		return err
	}
	pos += 4

	offsetsEnd := pos
	if offsetsEnd != txOffset {
		return fmt.Errorf("signed tx offset not at expected position. got: %d, expected %d", txOffset, offsetsEnd)
	}

	w.commitmentsOffset = commitmentsOffset
	w.numCommitments = blobsOffset - commitmentsOffset
	if w.numCommitments%ProofSize != 0 {
		return fmt.Errorf("expected multiple of proofsize, got: %v", w.numCommitments)
	}
	w.numCommitments /= ProofSize

	w.blobsOffset = blobsOffset
	w.numBlobs = proofsOffset - blobsOffset
	if w.numBlobs%BlobSize != 0 {
		return fmt.Errorf("expected multiple of blobsize, got: %v", w.numBlobs)
	}
	w.numBlobs /= BlobSize

	w.proofsOffset = proofsOffset
	w.numProofs = len(payload) - proofsOffset
	if w.numProofs%ProofSize != 0 {
		return fmt.Errorf("expected multiple of proofsize, got: %v", w.numProofs)
	}
	w.numProofs /= ProofSize

	err = w.DeserializeTx(payload, txOffset, commitmentsOffset)
	if err != nil {
		return err
	}

	return nil
}

func (w *wrapper) DeserializeTx(payload []byte, begin, end int) error {
	pos := begin
	messageOffset, err := readOffset(payload, pos, end, 0)
	if err != nil {
		return err
	}
	pos += 4

	w.sigOffset = pos
	pos += 65 // signature is fixed 65 bytes

	offsetsEnd := pos - begin
	if offsetsEnd != messageOffset {
		return fmt.Errorf("message offset not at expected position. got: %d, expected %d", messageOffset, offsetsEnd)
	}

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

	offsetsEnd := begin - originalBegin
	if addressOffset != offsetsEnd {
		return fmt.Errorf("address offset not at expected position. got: %d, expected %d", addressOffset, offsetsEnd)
	}

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

	return nil
}

func (w *wrapper) DeserializeAccessList(payload []byte, begin, end int) error {
	if begin == end {
		return nil
	}
	pos := begin
	length, err := readLength(payload, pos, 1<<24)
	if err != nil {
		return err
	}
	w.accessListAddressCount = length
	pos += 4

	offset := length * 4
	nextOffset := 0
	for i := 0; i < length; i++ {
		if i == length-1 {
			nextOffset = end - begin
		} else {
			nextOffset, err = readOffset(payload, pos, end, offset)
			if err != nil {
				return err
			}
			pos += 4
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
	l1 := w.numBlobHashes
	if l1 == 0 {
		return fmt.Errorf("blob txs must contain at least one blob")
	}
	l2 := w.numBlobs
	l3 := w.numCommitments
	l4 := w.numProofs
	if l1 != l2 || l1 != l3 || l1 != l4 {
		return fmt.Errorf("lengths don't match %v %v %v %v", l1, l2, l3, l4)
	}
	// The following check isn't strictly necessary as it would be caught by data gas processing
	// (and hence it is not explicitly in the spec for this function), but we prefer to fail
	// early in case we are getting spammed with too many blobs or there is a bug somewhere.
	if l1 > MaxBlobsPerBlock {
		return fmt.Errorf("number of blobs exceeds max: %v", l1)
	}

	comms := make([]gokzg4844.KZGCommitment, l1)
	p := payload[w.commitmentsOffset:]
	for i := range comms {
		copy(comms[i][:], p[i*ProofSize:i*ProofSize+ProofSize])
	}

	blobs := make([]gokzg4844.Blob, l1)
	p = payload[w.blobsOffset:]
	for i := range blobs {
		copy(blobs[i][:], p[i*BlobSize:i*BlobSize+BlobSize])
	}

	proofs := make([]gokzg4844.KZGProof, l1)
	p = payload[w.proofsOffset:]
	for i := range proofs {
		copy(proofs[i][:], p[i*ProofSize:i*ProofSize+ProofSize])
	}

	kzgCtx := kzg.Ctx()
	if err := kzgCtx.VerifyBlobKZGProofBatch(blobs, comms, proofs); err != nil {
		return fmt.Errorf("error during proof verification: %w", err)
	}

	for i := 0; i < l1; i++ {
		computed := kzg.KZGToVersionedHash(comms[i])
		h := kzg.VersionedHash{}
		offset := w.blobHashesOffset
		copy(h[:], payload[offset+i*32:offset+i*32+32])
		if computed != h {
			return fmt.Errorf("versioned hash %d supposedly %x but does not match computed %x", i, h, computed)
		}
	}

	return nil
}
