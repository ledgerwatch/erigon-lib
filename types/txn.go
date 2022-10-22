/*
   Copyright 2021 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package types

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"sort"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/secp256k1"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/crypto"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/rlp"
)

type TxParseConfig struct {
	ChainID uint256.Int
}

// TxParseContext is object that is required to parse transactions and turn transaction payload into TxSlot objects
// usage of TxContext helps avoid extra memory allocations
type TxParseContext struct {
	Keccak2         hash.Hash
	Keccak1         hash.Hash
	validateRlp     func([]byte) error
	ChainID         uint256.Int // Signature values
	R               uint256.Int // Signature values
	S               uint256.Int // Signature values
	V               uint256.Int // Signature values
	ChainIDMul      uint256.Int
	DeriveChainID   uint256.Int // pre-allocated variable to calculate Sub(&ctx.v, &ctx.chainIDMul)
	cfg             TxParseConfig
	buf             [65]byte // buffer needs to be enough for hashes (32 bytes) and for public key (65 bytes)
	Sig             [65]byte
	Sighash         [32]byte
	withSender      bool
	allowPreEip2s   bool // Allow s > secp256k1n/2; see EIP-2
	chainIDRequired bool
	IsProtected     bool
}

func NewTxParseContext(chainID uint256.Int) *TxParseContext {
	if chainID.IsZero() {
		panic("wrong chainID")
	}
	ctx := &TxParseContext{
		withSender: true,
		Keccak1:    sha3.NewLegacyKeccak256(),
		Keccak2:    sha3.NewLegacyKeccak256(),
	}

	// behave as of London enabled
	ctx.cfg.ChainID.Set(&chainID)
	ctx.ChainIDMul.Mul(&chainID, u256.N2)
	return ctx
}

// TxSlot contains information extracted from an Ethereum transaction, which is enough to manage it inside the transaction.
// Also, it contains some auxillary information, like ephemeral fields, and indices within priority queues
type TxSlot struct {
	Rlp            []byte      // TxPool set it to nil after save it to db
	Value          uint256.Int // Value transferred by the transaction
	Tip            uint256.Int // Maximum tip that transaction is giving to miner/block proposer
	FeeCap         uint256.Int // Maximum fee that transaction burns and gives to the miner/block proposer
	SenderID       uint64      // SenderID - require external mapping to it's address
	Nonce          uint64      // Nonce of the transaction
	DataLen        int         // Length of transaction's data (for calculation of intrinsic gas)
	DataNonZeroLen int
	AlAddrCount    int      // Number of addresses in the access list
	AlStorCount    int      // Number of storage keys in the access list
	Gas            uint64   // Gas limit of the transaction
	IDHash         [32]byte // Transaction hash for the purposes of using it as a transaction Id
	Traced         bool     // Whether transaction needs to be traced throughout transaction pool code and generate debug printing
	Creation       bool     // Set to true if "To" field of the transaction is not set
}

const (
	LegacyTxType     int = 0
	AccessListTxType int = 1
	DynamicFeeTxType int = 2
	StarknetTxType   int = 3
)

var ErrParseTxn = fmt.Errorf("%w transaction", rlp.ErrParse)

var ErrRejected = errors.New("rejected")
var ErrAlreadyKnown = errors.New("already known")
var ErrRlpTooBig = errors.New("txn rlp too big")

func (ctx *TxParseContext) ValidateRLP(f func(txnRlp []byte) error) { ctx.validateRlp = f }
func (ctx *TxParseContext) WithSender(v bool)                       { ctx.withSender = v }
func (ctx *TxParseContext) WithAllowPreEip2s(v bool)                { ctx.allowPreEip2s = v }
func (ctx *TxParseContext) ChainIDRequired() *TxParseContext {
	ctx.chainIDRequired = true
	return ctx
}

// ParseTransaction extracts all the information from the transactions's payload (RLP) necessary to build TxSlot
// it also performs syntactic validation of the transactions
func (ctx *TxParseContext) ParseTransaction(payload []byte, pos int, slot *TxSlot, sender []byte, hasEnvelope bool, validateHash func([]byte) error) (p int, err error) {
	if len(payload) == 0 {
		return 0, fmt.Errorf("%w: empty rlp", ErrParseTxn)
	}
	if ctx.withSender && len(sender) != 20 {
		return 0, fmt.Errorf("%w: expect sender buffer of len 20", ErrParseTxn)
	}
	// Compute transaction hash
	ctx.Keccak1.Reset()
	ctx.Keccak2.Reset()
	// Legacy transactions have list Prefix, whereas EIP-2718 transactions have string Prefix
	// therefore we assign the first returned value of Prefix function (list) to legacy variable
	dataPos, dataLen, legacy, err := rlp.Prefix(payload, pos)
	if err != nil {
		return 0, fmt.Errorf("%w: size Prefix: %s", ErrParseTxn, err)
	}
	// This handles the transactions coming from other Erigon peers of older versions, which add 0x80 (empty) transactions into packets
	if dataLen == 0 {
		return 0, fmt.Errorf("%w: transaction must be either 1 list or 1 string", ErrParseTxn)
	}
	if dataLen == 1 && !legacy {
		if hasEnvelope {
			return 0, fmt.Errorf("%w: expected envelope in the payload, got %x", ErrParseTxn, payload[dataPos:dataPos+dataLen])
		}
	}

	p = dataPos

	var txType int
	// If it is non-legacy transaction, the transaction type follows, and then the the list
	if !legacy {
		txType = int(payload[p])
		if _, err = ctx.Keccak1.Write(payload[p : p+1]); err != nil {
			return 0, fmt.Errorf("%w: computing IdHash (hashing type Prefix): %s", ErrParseTxn, err)
		}
		if _, err = ctx.Keccak2.Write(payload[p : p+1]); err != nil {
			return 0, fmt.Errorf("%w: computing signHash (hashing type Prefix): %s", ErrParseTxn, err)
		}
		p++
		if p >= len(payload) {
			return 0, fmt.Errorf("%w: unexpected end of payload after txType", ErrParseTxn)
		}
		dataPos, dataLen, err = rlp.List(payload, p)
		if err != nil {
			return 0, fmt.Errorf("%w: envelope Prefix: %s", ErrParseTxn, err)
		}
		// Hash the envelope, not the full payload
		if _, err = ctx.Keccak1.Write(payload[p : dataPos+dataLen]); err != nil {
			return 0, fmt.Errorf("%w: computing IdHash (hashing the envelope): %s", ErrParseTxn, err)
		}
		// For legacy transaction, the entire payload in expected to be in "rlp" field
		// whereas for non-legacy, only the content of the envelope (start with position p)
		slot.Rlp = payload[p-1 : dataPos+dataLen]
		p = dataPos
	} else {
		slot.Rlp = payload[pos : dataPos+dataLen]
	}

	if ctx.validateRlp != nil {
		if err := ctx.validateRlp(slot.Rlp); err != nil {
			return p, err
		}
	}

	// Remember where signing hash data begins (it will need to be wrapped in an RLP list)
	sigHashPos := p
	if !legacy {
		p, err = rlp.U256(payload, p, &ctx.ChainID)
		if err != nil {
			return 0, fmt.Errorf("%w: chainId len: %s", ErrParseTxn, err)
		}
		if ctx.ChainID.IsZero() { // zero indicates that the chain ID was not specified in the tx.
			if ctx.chainIDRequired {
				return 0, fmt.Errorf("%w: chainID is required", ErrParseTxn)
			}
			ctx.ChainID.Set(&ctx.cfg.ChainID)
		}
		if !ctx.ChainID.Eq(&ctx.cfg.ChainID) {
			return 0, fmt.Errorf("%w: %s, %d (expected %d)", ErrParseTxn, "invalid chainID", ctx.ChainID.Uint64(), ctx.cfg.ChainID.Uint64())
		}
	}
	// Next follows the nonce, which we need to parse
	p, slot.Nonce, err = rlp.U64(payload, p)
	if err != nil {
		return 0, fmt.Errorf("%w: nonce: %s", ErrParseTxn, err)
	}
	// Next follows gas price or tip
	// Although consensus rules specify that tip can be up to 256 bit long, we narrow it to 64 bit
	p, err = rlp.U256(payload, p, &slot.Tip)
	if err != nil {
		return 0, fmt.Errorf("%w: tip: %s", ErrParseTxn, err)
	}
	// Next follows feeCap, but only for dynamic fee transactions, for legacy transaction, it is
	// equal to tip
	if txType < DynamicFeeTxType {
		slot.FeeCap = slot.Tip
	} else {
		// Although consensus rules specify that feeCap can be up to 256 bit long, we narrow it to 64 bit
		p, err = rlp.U256(payload, p, &slot.FeeCap)
		if err != nil {
			return 0, fmt.Errorf("%w: feeCap: %s", ErrParseTxn, err)
		}
	}
	// Next follows gas
	p, slot.Gas, err = rlp.U64(payload, p)
	if err != nil {
		return 0, fmt.Errorf("%w: gas: %s", ErrParseTxn, err)
	}
	// Next follows the destination address (if present)
	dataPos, dataLen, err = rlp.String(payload, p)
	if err != nil {
		return 0, fmt.Errorf("%w: to len: %s", ErrParseTxn, err)
	}
	if dataLen != 0 && dataLen != 20 {
		return 0, fmt.Errorf("%w: unexpected length of to field: %d", ErrParseTxn, dataLen)
	}

	// Only note if To field is empty or not
	slot.Creation = dataLen == 0
	p = dataPos + dataLen
	// Next follows value
	p, err = rlp.U256(payload, p, &slot.Value)
	if err != nil {
		return 0, fmt.Errorf("%w: value: %s", ErrParseTxn, err)
	}
	// Next goes data, but we are only interesting in its length
	dataPos, dataLen, err = rlp.String(payload, p)
	if err != nil {
		return 0, fmt.Errorf("%w: data len: %s", ErrParseTxn, err)
	}
	slot.DataLen = dataLen

	// Zero and non-zero bytes are priced differently
	slot.DataNonZeroLen = 0
	for _, byt := range payload[dataPos : dataPos+dataLen] {
		if byt != 0 {
			slot.DataNonZeroLen++
		}
	}

	p = dataPos + dataLen

	// Next goes starknet tx salt, but we are only interesting in its length
	if txType == StarknetTxType {
		dataPos, dataLen, err = rlp.String(payload, p)
		if err != nil {
			return 0, fmt.Errorf("%w: data len: %s", ErrParseTxn, err)
		}
		p = dataPos + dataLen
	}

	// Next follows access list for non-legacy transactions, we are only interesting in number of addresses and storage keys
	if !legacy {
		dataPos, dataLen, err = rlp.List(payload, p)
		if err != nil {
			return 0, fmt.Errorf("%w: access list len: %s", ErrParseTxn, err)
		}
		tuplePos := dataPos
		var tupleLen int
		for tuplePos < dataPos+dataLen {
			tuplePos, tupleLen, err = rlp.List(payload, tuplePos)
			if err != nil {
				return 0, fmt.Errorf("%w: tuple len: %s", ErrParseTxn, err)
			}
			var addrPos int
			addrPos, err = rlp.StringOfLen(payload, tuplePos, 20)
			if err != nil {
				return 0, fmt.Errorf("%w: tuple addr len: %s", ErrParseTxn, err)
			}
			slot.AlAddrCount++
			var storagePos, storageLen int
			storagePos, storageLen, err = rlp.List(payload, addrPos+20)
			if err != nil {
				return 0, fmt.Errorf("%w: storage key list len: %s", ErrParseTxn, err)
			}
			skeyPos := storagePos
			for skeyPos < storagePos+storageLen {
				skeyPos, err = rlp.StringOfLen(payload, skeyPos, 32)
				if err != nil {
					return 0, fmt.Errorf("%w: tuple storage key len: %s", ErrParseTxn, err)
				}
				slot.AlStorCount++
				skeyPos += 32
			}
			if skeyPos != storagePos+storageLen {
				return 0, fmt.Errorf("%w: extraneous space in the tuple after storage key list", ErrParseTxn)
			}
			tuplePos += tupleLen
		}
		if tuplePos != dataPos+dataLen {
			return 0, fmt.Errorf("%w: extraneous space in the access list after all tuples", ErrParseTxn)
		}
		p = dataPos + dataLen
	}
	// This is where the data for Sighash ends
	// Next follows V of the signature
	var vByte byte
	sigHashEnd := p
	sigHashLen := uint(sigHashEnd - sigHashPos)
	var chainIDBits, chainIDLen int
	if legacy {
		p, err = rlp.U256(payload, p, &ctx.V)
		if err != nil {
			return 0, fmt.Errorf("%w: V: %s", ErrParseTxn, err)
		}
		ctx.IsProtected = ctx.V.Eq(u256.N27) || ctx.V.Eq(u256.N28)
		// Compute chainId from V
		if ctx.IsProtected {
			// Do not add chain id and two extra zeros
			vByte = byte(ctx.V.Uint64() - 27)
			ctx.ChainID.Set(&ctx.cfg.ChainID)
		} else {
			ctx.ChainID.Sub(&ctx.V, u256.N35)
			ctx.ChainID.Rsh(&ctx.ChainID, 1)
			if !ctx.ChainID.Eq(&ctx.cfg.ChainID) {
				return 0, fmt.Errorf("%w: %s, %d (expected %d)", ErrParseTxn, "invalid chainID", ctx.ChainID.Uint64(), ctx.cfg.ChainID.Uint64())
			}

			chainIDBits = ctx.ChainID.BitLen()
			if chainIDBits <= 7 {
				chainIDLen = 1
			} else {
				chainIDLen = (chainIDBits + 7) / 8 // It is always < 56 bytes
				sigHashLen++                       // For chainId len Prefix
			}
			sigHashLen += uint(chainIDLen) // For chainId
			sigHashLen += 2                // For two extra zeros

			ctx.DeriveChainID.Sub(&ctx.V, &ctx.ChainIDMul)
			vByte = byte(ctx.DeriveChainID.Sub(&ctx.DeriveChainID, u256.N8).Uint64() - 27)
		}
	} else {
		var v uint64
		p, v, err = rlp.U64(payload, p)
		if err != nil {
			return 0, fmt.Errorf("%w: V: %s", ErrParseTxn, err)
		}
		if v > 1 {
			return 0, fmt.Errorf("%w: V is loo large: %d", ErrParseTxn, v)
		}
		vByte = byte(v)
		ctx.IsProtected = true
	}

	// Next follows R of the signature
	p, err = rlp.U256(payload, p, &ctx.R)
	if err != nil {
		return 0, fmt.Errorf("%w: R: %s", ErrParseTxn, err)
	}
	// New follows S of the signature
	p, err = rlp.U256(payload, p, &ctx.S)
	if err != nil {
		return 0, fmt.Errorf("%w: S: %s", ErrParseTxn, err)
	}

	// For legacy transactions, hash the full payload
	if legacy {
		if _, err = ctx.Keccak1.Write(payload[pos:p]); err != nil {
			return 0, fmt.Errorf("%w: computing IdHash: %s", ErrParseTxn, err)
		}
	}
	//ctx.keccak1.Sum(slot.IdHash[:0])
	_, _ = ctx.Keccak1.(io.Reader).Read(slot.IDHash[:32])
	if validateHash != nil {
		if err := validateHash(slot.IDHash[:32]); err != nil {
			return p, err
		}
	}

	if !ctx.withSender {
		return p, nil
	}

	if !crypto.TransactionSignatureIsValid(vByte, &ctx.R, &ctx.S, ctx.allowPreEip2s && legacy) {
		return 0, fmt.Errorf("%w: invalid v, r, s: %d, %s, %s", ErrParseTxn, vByte, &ctx.R, &ctx.S)
	}

	// Computing sigHash (hash used to recover sender from the signature)
	// Write len Prefix to the Sighash
	if sigHashLen < 56 {
		ctx.buf[0] = byte(sigHashLen) + 192
		if _, err := ctx.Keccak2.Write(ctx.buf[:1]); err != nil {
			return 0, fmt.Errorf("%w: computing signHash (hashing len Prefix): %s", ErrParseTxn, err)
		}
	} else {
		beLen := (bits.Len(sigHashLen) + 7) / 8
		binary.BigEndian.PutUint64(ctx.buf[1:], uint64(sigHashLen))
		ctx.buf[8-beLen] = byte(beLen) + 247
		if _, err := ctx.Keccak2.Write(ctx.buf[8-beLen : 9]); err != nil {
			return 0, fmt.Errorf("%w: computing signHash (hashing len Prefix): %s", ErrParseTxn, err)
		}
	}
	if _, err = ctx.Keccak2.Write(payload[sigHashPos:sigHashEnd]); err != nil {
		return 0, fmt.Errorf("%w: computing signHash: %s", ErrParseTxn, err)
	}
	if legacy {
		if chainIDLen > 0 {
			if chainIDBits <= 7 {
				ctx.buf[0] = byte(ctx.ChainID.Uint64())
				if _, err := ctx.Keccak2.Write(ctx.buf[:1]); err != nil {
					return 0, fmt.Errorf("%w: computing signHash (hashing legacy chainId): %s", ErrParseTxn, err)
				}
			} else {
				binary.BigEndian.PutUint64(ctx.buf[1:9], ctx.ChainID[3])
				binary.BigEndian.PutUint64(ctx.buf[9:17], ctx.ChainID[2])
				binary.BigEndian.PutUint64(ctx.buf[17:25], ctx.ChainID[1])
				binary.BigEndian.PutUint64(ctx.buf[25:33], ctx.ChainID[0])
				ctx.buf[32-chainIDLen] = 128 + byte(chainIDLen)
				if _, err = ctx.Keccak2.Write(ctx.buf[32-chainIDLen : 33]); err != nil {
					return 0, fmt.Errorf("%w: computing signHash (hashing legacy chainId): %s", ErrParseTxn, err)
				}
			}
			// Encode two zeros
			ctx.buf[0] = 128
			ctx.buf[1] = 128
			if _, err := ctx.Keccak2.Write(ctx.buf[:2]); err != nil {
				return 0, fmt.Errorf("%w: computing signHash (hashing zeros after legacy chainId): %s", ErrParseTxn, err)
			}
		}
	}
	// Squeeze Sighash
	_, _ = ctx.Keccak2.(io.Reader).Read(ctx.Sighash[:32])
	//ctx.keccak2.Sum(ctx.Sighash[:0])
	binary.BigEndian.PutUint64(ctx.Sig[0:8], ctx.R[3])
	binary.BigEndian.PutUint64(ctx.Sig[8:16], ctx.R[2])
	binary.BigEndian.PutUint64(ctx.Sig[16:24], ctx.R[1])
	binary.BigEndian.PutUint64(ctx.Sig[24:32], ctx.R[0])
	binary.BigEndian.PutUint64(ctx.Sig[32:40], ctx.S[3])
	binary.BigEndian.PutUint64(ctx.Sig[40:48], ctx.S[2])
	binary.BigEndian.PutUint64(ctx.Sig[48:56], ctx.S[1])
	binary.BigEndian.PutUint64(ctx.Sig[56:64], ctx.S[0])
	ctx.Sig[64] = vByte
	// recover sender
	if _, err = secp256k1.RecoverPubkeyWithContext(secp256k1.DefaultContext, ctx.Sighash[:], ctx.Sig[:], ctx.buf[:0]); err != nil {
		return 0, fmt.Errorf("%w: recovering sender from signature: %s", ErrParseTxn, err)
	}
	//apply keccak to the public key
	ctx.Keccak2.Reset()
	if _, err = ctx.Keccak2.Write(ctx.buf[1:65]); err != nil {
		return 0, fmt.Errorf("%w: computing sender from public key: %s", ErrParseTxn, err)
	}
	// squeeze the hash of the public key
	//ctx.keccak2.Sum(ctx.buf[:0])
	_, _ = ctx.Keccak2.(io.Reader).Read(ctx.buf[:32])
	//take last 20 bytes as address
	copy(sender, ctx.buf[12:32])

	return p, nil
}

type PeerID *types.H512

type Hashes []byte // flatten list of 32-byte hashes

func (h Hashes) At(i int) []byte { return h[i*length.Hash : (i+1)*length.Hash] }
func (h Hashes) Len() int        { return len(h) / length.Hash }
func (h Hashes) Less(i, j int) bool {
	return bytes.Compare(h[i*length.Hash:(i+1)*length.Hash], h[j*length.Hash:(j+1)*length.Hash]) < 0
}
func (h Hashes) Swap(i, j int) {
	ii := i * length.Hash
	jj := j * length.Hash
	for k := 0; k < length.Hash; k++ {
		h[ii], h[jj] = h[jj], h[ii]
		ii++
		jj++
	}
}

// DedupCopy sorts hashes, and creates deduplicated copy
func (h Hashes) DedupCopy() Hashes {
	if len(h) == 0 {
		return h
	}
	sort.Sort(h)
	unique := 1
	for i := length.Hash; i < len(h); i += length.Hash {
		if !bytes.Equal(h[i:i+length.Hash], h[i-length.Hash:i]) {
			unique++
		}
	}
	c := make(Hashes, unique*length.Hash)
	copy(c[:], h[0:length.Hash])
	dest := length.Hash
	for i := dest; i < len(h); i += length.Hash {
		if !bytes.Equal(h[i:i+length.Hash], h[i-length.Hash:i]) {
			copy(c[dest:dest+length.Hash], h[i:i+length.Hash])
			dest += length.Hash
		}
	}
	return c
}

type Addresses []byte // flatten list of 20-byte addresses

func (h Addresses) At(i int) []byte { return h[i*length.Addr : (i+1)*length.Addr] }
func (h Addresses) Len() int        { return len(h) / length.Addr }

type TxSlots struct {
	Txs     []*TxSlot
	Senders Addresses
	IsLocal []bool
}

func (s *TxSlots) Valid() error {
	if len(s.Txs) != len(s.IsLocal) {
		return fmt.Errorf("TxSlots: expect equal len of isLocal=%d and txs=%d", len(s.IsLocal), len(s.Txs))
	}
	if len(s.Txs) != s.Senders.Len() {
		return fmt.Errorf("TxSlots: expect equal len of senders=%d and txs=%d", s.Senders.Len(), len(s.Txs))
	}
	return nil
}

var zeroAddr = make([]byte, 20)

// Resize internal arrays to len=targetSize, shrinks if need. It rely on `append` algorithm to realloc
func (s *TxSlots) Resize(targetSize uint) {
	for uint(len(s.Txs)) < targetSize {
		s.Txs = append(s.Txs, nil)
	}
	for uint(s.Senders.Len()) < targetSize {
		s.Senders = append(s.Senders, addressesGrowth...)
	}
	for uint(len(s.IsLocal)) < targetSize {
		s.IsLocal = append(s.IsLocal, false)
	}
	//todo: set nil to overflow txs
	oldLen := uint(len(s.Txs))
	s.Txs = s.Txs[:targetSize]
	for i := oldLen; i < targetSize; i++ {
		s.Txs[i] = nil
	}
	s.Senders = s.Senders[:length.Addr*targetSize]
	for i := oldLen; i < targetSize; i++ {
		copy(s.Senders.At(int(i)), zeroAddr)
	}
	s.IsLocal = s.IsLocal[:targetSize]
	for i := oldLen; i < targetSize; i++ {
		s.IsLocal[i] = false
	}
}
func (s *TxSlots) Append(slot *TxSlot, sender []byte, isLocal bool) {
	n := len(s.Txs)
	s.Resize(uint(len(s.Txs) + 1))
	s.Txs[n] = slot
	s.IsLocal[n] = isLocal
	copy(s.Senders.At(n), sender)
}

type TxsRlp struct {
	Txs     [][]byte
	Senders Addresses
	IsLocal []bool
}

// Resize internal arrays to len=targetSize, shrinks if need. It rely on `append` algorithm to realloc
func (s *TxsRlp) Resize(targetSize uint) {
	for uint(len(s.Txs)) < targetSize {
		s.Txs = append(s.Txs, nil)
	}
	for uint(s.Senders.Len()) < targetSize {
		s.Senders = append(s.Senders, addressesGrowth...)
	}
	for uint(len(s.IsLocal)) < targetSize {
		s.IsLocal = append(s.IsLocal, false)
	}
	//todo: set nil to overflow txs
	s.Txs = s.Txs[:targetSize]
	s.Senders = s.Senders[:length.Addr*targetSize]
	s.IsLocal = s.IsLocal[:targetSize]
}

var addressesGrowth = make([]byte, length.Addr)

func EncodeSenderLengthForStorage(nonce uint64, balance uint256.Int) uint {
	var structLength uint = 1 // 1 byte for fieldset
	if !balance.IsZero() {
		structLength += uint(balance.ByteLen()) + 1
	}
	if nonce > 0 {
		structLength += uint((bits.Len64(nonce)+7)/8) + 1
	}
	return structLength
}

func EncodeSender(nonce uint64, balance uint256.Int, buffer []byte) {
	var fieldSet = 0 // start with first bit set to 0
	var pos = 1
	if nonce > 0 {
		fieldSet = 1
		nonceBytes := (bits.Len64(nonce) + 7) / 8
		buffer[pos] = byte(nonceBytes)
		var nonce = nonce
		for i := nonceBytes; i > 0; i-- {
			buffer[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}

	// Encoding balance
	if !balance.IsZero() {
		fieldSet |= 2
		balanceBytes := balance.ByteLen()
		buffer[pos] = byte(balanceBytes)
		pos++
		balance.WriteToSlice(buffer[pos : pos+balanceBytes])
		pos += balanceBytes //nolint
	}

	buffer[0] = byte(fieldSet)
}
func DecodeSender(enc []byte) (nonce uint64, balance uint256.Int, err error) {
	if len(enc) == 0 {
		return
	}

	var fieldSet = enc[0]
	var pos = 1

	if fieldSet&1 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return nonce, balance, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		nonce = bytesToUint64(enc[pos+1 : pos+decodeLength+1])
		pos += decodeLength + 1
	}

	if fieldSet&2 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return nonce, balance, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		(&balance).SetBytes(enc[pos+1 : pos+decodeLength+1])
	}
	return
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

// nolint
func (tx *TxSlot) PrintDebug(prefix string) {
	fmt.Printf("%s: senderID=%d,nonce=%d,tip=%d,v=%d\n", prefix, tx.SenderID, tx.Nonce, tx.Tip, tx.Value.Uint64())
	//fmt.Printf("%s: senderID=%d,nonce=%d,tip=%d,hash=%x\n", prefix, tx.senderID, tx.nonce, tx.tip, tx.IdHash)
}

// AccessList is an EIP-2930 access list.
type AccessList []AccessTuple

// AccessTuple is the element type of an access list.
type AccessTuple struct {
	StorageKeys [][32]byte `json:"storageKeys"    gencodec:"required"`
	Address     [20]byte   `json:"address"        gencodec:"required"`
}

// StorageKeys returns the total number of storage keys in the access list.
func (al AccessList) StorageKeys() int {
	sum := 0
	for _, tuple := range al {
		sum += len(tuple.StorageKeys)
	}
	return sum
}
