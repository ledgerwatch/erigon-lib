package types

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/crypto"
	"github.com/ledgerwatch/erigon-lib/crypto/cryptopool"
	"github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/secp256k1"
)

// DecodeTransaction extracts all the information from the transactions's payload (RLP) necessary to build TxSlot.
// It also performs syntactic validation of the transactions.
// wrappedWithBlobs means that for blob (type 3) transactions the full version with blobs/commitments/proofs is expected
// (see https://eips.ethereum.org/EIPS/eip-4844#networking).
//
//	// [  ] || [          nonce,      price, limit, to, value, data,                                     y,r,s]
//	// 0x01 || [chain_id, nonce,      price, limit, to, value, data, access_list,                        y,r,s]
//	// 0x02 || [chain_id, nonce, tip, price, limit, to, value, data, access_list,                        y,r,s]
//	// 0x03 || [chain_id, nonce, tip, price, limit, to, value, data, access_list, blob_price, blob_hash, y,r,s]
//	// 0x03 ||[[chain_id, nonce, tip, price, limit, to, value, data, access_list, blob_price, blob_hash, y,r,s], blobs, commitments, proofs]
func (ctx *TxParseContext) DecodeTransaction(decoder *rlp.Decoder, slot *TxSlot, sender []byte, hasEnvelope, wrappedWithBlobs bool, validateHash func([]byte) error) (err error) {
	err = ctx.decodeTransaction(decoder, slot, sender, hasEnvelope, wrappedWithBlobs, validateHash)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrParseTxn, err)
	}
	return nil
}

func (ctx *TxParseContext) decodeTransaction(decoder *rlp.Decoder, slot *TxSlot, sender []byte, hasEnvelope, wrappedWithBlobs bool, validateHash func([]byte) error) (err error) {
	od := decoder
	if decoder.Empty() {
		return fmt.Errorf("empty rlp")
	}
	if ctx.withSender && len(sender) != 20 {
		return fmt.Errorf("expect sender buffer of len 20")
	}
	// start classification
	token, err := decoder.PeekToken()
	if err != nil {
		return err
	}
	// means that this is non-enveloped non-legacy transaction
	if token == rlp.TokenDecimal {
		if hasEnvelope {
			return fmt.Errorf("expected envelope in the payload, got %s", token)
		}
	}

	var (
		dec *rlp.Decoder
	)
	var (
		parent      *rlp.Decoder // the parent should contain in its underlying buffer the rlp used for txn hash creation sans txn type
		bodyDecoder *rlp.Decoder // the bodyDecoder should be an rlp decoder primed at the top of the list body for txn
	)

	switch {
	default:
		return fmt.Errorf("expected list or blob token, got %s", token)
	case token.IsListType(): // Legacy transactions have list Prefix,
		// enter the list
		parent = decoder
		bodyDecoder, _, err = decoder.ElemDec()
		if err != nil {
			return fmt.Errorf("size prefix: %w", err) //nolint
		}
		slot.Rlp = append([]byte{}, decoder.Consumed()...)
		slot.Type = LegacyTxType
	case token.IsBlobType() || token == rlp.TokenDecimal: // EIP-2718 transactions have string Prefix
		// if is blob type, it means that its an envelope, so we need to get out of that
		if token.IsBlobType() {
			decoder, _, err = decoder.ElemDec()
			if err != nil {
				return fmt.Errorf("size prefix: %w", err) //nolint
			}
			if decoder.Empty() {
				return fmt.Errorf("transaction must be either 1 list or 1 string")
			}
		}
		slot.Rlp = append([]byte{}, decoder.Bytes()...)
		// at this point, the next byte is the type
		slot.Type, err = decoder.ReadByte()
		if err != nil {
			return fmt.Errorf("couldnt read txn type: %w", err)
		}
		if slot.Type > BlobTxType {
			return fmt.Errorf("unknown transaction type: %d", slot.Type)
		}
		// from here to the end of the element, if this is not a blob tx type with blobs, is the parent
		parent, _, err = decoder.RawElemDec()
		if err != nil {
			return fmt.Errorf("extract txn body parent: %w", err) //nolint
		}
		// now enter the list, since that is what we are in front of now.
		bodyDecoder, _, err = parent.ElemDec()
		if err != nil {
			return fmt.Errorf("extract txn body: %w", err) //nolint
		}
		if slot.Type == BlobTxType {
			if wrappedWithBlobs {
				dec = bodyDecoder
				// if its a blob transaction and wrapped with blobs, we actually need to enter a nested list
				// in this case, "decoder" was an iterator for the array of [ [txbody...], blobs, commitments, proofs]
				// so our bodyDecoder is actually a pointer to the header of [txbody...].
				// we can extract the raw elem out of this in order to get the parent decoder
				parent, _, err = bodyDecoder.RawElemDec()
				if err != nil {
					return fmt.Errorf("wrapped blob tx body parent: %w", err) //nolint
				}
				// and then the body is actually just the parent decoder read once, since we are sitting at the top of the [txbody...] header
				bodyDecoder, _, err = parent.ElemDec()
				if err != nil {
					return fmt.Errorf("wrapped blob tx body: %w", err) //nolint
				}
			}
		}
	}
	err = ctx.decodeTransactionBody(bodyDecoder, parent, slot, sender, validateHash)
	if err != nil {
		return fmt.Errorf("txn body: %w", err)
	}

	// so its a blob transaction and we need to do the extra stuff, otherwise we are done
	if slot.Type == BlobTxType && wrappedWithBlobs {
		if err := ctx.decodeBlobs(dec, slot); err != nil {
			return fmt.Errorf("decode blobs: %w", err)
		}
		if err := ctx.decodeCommitments(dec, slot); err != nil {
			return fmt.Errorf("decode commitments: %w", err)
		}
		if err := ctx.decodeProofs(dec, slot); err != nil {
			return fmt.Errorf("decode proofs: %w", err)
		}
		if len(slot.Blobs) != len(slot.Commitments) {
			return fmt.Errorf("blob count != commitment count")
		}
		if len(slot.Commitments) != len(slot.Proofs) {
			return fmt.Errorf("commitment count != proof count")
		}
		if len(slot.BlobHashes) != len(slot.Blobs) {
			return fmt.Errorf("blob count != blob hash count")
		}
	}
	slot.Size = uint32(od.Offset())
	return err
}

func (ctx *TxParseContext) decodeTransactionBody(dec *rlp.Decoder, parent *rlp.Decoder, slot *TxSlot, sender []byte, validateHash func([]byte) error) (err error) {
	legacy := slot.Type == LegacyTxType

	k1 := cryptopool.GetLegacyKeccak256()
	k2 := cryptopool.GetLegacyKeccak256()
	defer cryptopool.ReturnLegacyKeccak256(k1)
	defer cryptopool.ReturnLegacyKeccak256(k2)

	k1.Reset()
	k2.Reset()

	// for computing tx hash
	if !legacy {
		typeByte := []byte{slot.Type}
		if _, err := k1.Write(typeByte); err != nil {
			return fmt.Errorf("compute idHash: %w", err)
		}
		if _, err := k2.Write(typeByte); err != nil {
			return fmt.Errorf("compute Hash: %w", err)
		}
		if _, err := k1.Write(parent.Underlying()); err != nil {
			return fmt.Errorf("compute idHash: %w", err)
		}
	}

	if ctx.validateRlp != nil {
		if err := ctx.validateRlp(slot.Rlp); err != nil {
			return fmt.Errorf("validate rlp: %w", err)
		}
	}

	// signing hash data starts here
	sigHashPos := dec.Offset()
	if !legacy {
		err = rlp.ReadElem(dec, rlp.Uint256, &ctx.ChainID)
		if err != nil {
			return fmt.Errorf("bad chainId: %w", err) //nolint
		}
		if ctx.ChainID.IsZero() { // zero indicates that the chain ID was not specified in the tx.
			if ctx.chainIDRequired {
				return fmt.Errorf("chainID is required")
			}
			ctx.ChainID.Set(&ctx.cfg.ChainID)
		}
		if !ctx.ChainID.Eq(&ctx.cfg.ChainID) {
			return fmt.Errorf("%s, %d (expected %d)", "invalid chainID", ctx.ChainID.Uint64(), ctx.cfg.ChainID.Uint64())
		}
	}

	// Next follows the nonce, which we need to parse
	err = rlp.ReadElem(dec, rlp.Uint64, &slot.Nonce)
	if err != nil {
		return fmt.Errorf("nonce: %s", err) //nolint
	}
	// Next follows gas price or tip
	err = rlp.ReadElem(dec, rlp.Uint256, &slot.Tip)
	if err != nil {
		return fmt.Errorf("tip: %s", err) //nolint
	}
	// Next follows feeCap, but only for dynamic fee transactions, for legacy transaction, it is
	// equal to tip
	if slot.Type < DynamicFeeTxType {
		slot.FeeCap = slot.Tip
	} else {
		err = rlp.ReadElem(dec, rlp.Uint256, &slot.FeeCap)
		if err != nil {
			return fmt.Errorf("feeCap: %s", err) //nolint
		}
	}
	// gas limit
	err = rlp.ReadElem(dec, rlp.Uint64, &slot.Gas)
	if err != nil {
		return fmt.Errorf("gas: %s", err) //nolint
	}
	// recipient
	err = rlp.ReadElem(dec, rlp.IsEmpty, &slot.Creation)
	if err != nil {
		return fmt.Errorf("value: %s", err) //nolint
	}
	// Next follows value
	err = rlp.ReadElem(dec, rlp.Uint256, &slot.Value)
	if err != nil {
		return fmt.Errorf("value: %s", err) //nolint
	}
	// Next goes data, but we are only interesting in its length
	err = rlp.ReadElem(dec, func(i *int, b []byte) error {
		slot.DataLen = len(b)
		for _, byt := range b {
			if byt != 0 {
				slot.DataNonZeroLen++
			}
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("data len: %s", err) //nolint
	}
	// Zero and non-zero bytes are priced differently
	slot.DataNonZeroLen = 0
	// Next follows access list for non-legacy transactions, we are only interesting in number of addresses and storage keys
	if !legacy {
		err = dec.ForList(func(ld *rlp.Decoder) error {
			slot.AlAddrCount++
			err := rlp.ReadElem(ld, rlp.Skip, nil)
			if err != nil {
				return fmt.Errorf("elem: %w", err)
			}
			err = ld.ForList(func(sk *rlp.Decoder) error {
				slot.AlStorCount++
				err := rlp.ReadElem(sk, rlp.Skip, nil)
				if err != nil {
					return fmt.Errorf("elem: %w", err)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("iterate: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("iterate: %w", err)
		}
	}

	if slot.Type == BlobTxType {
		err = rlp.ReadElem(dec, rlp.Uint256, &slot.BlobFeeCap)
		if err != nil {
			return fmt.Errorf("blob fee cap: %s", err) //nolint
		}
		dec.ForList(func(d *rlp.Decoder) error {
			var blob common.Hash
			blobSlice := blob[:]
			err := rlp.ReadElem(d, rlp.BytesExact, &blobSlice)
			if err != nil {
				return err
			}
			slot.BlobHashes = append(slot.BlobHashes, blob)
			return nil
		})
	}
	// This is where the data for Sighash ends
	sigHashEnd := dec.Offset()
	sigHashLen := uint(sigHashEnd - sigHashPos)

	// Next follows V of the signature
	var vByte byte
	var chainIDBits, chainIDLen int
	if legacy {
		err = rlp.ReadElem(dec, rlp.Uint256, &ctx.V)
		if err != nil {
			return fmt.Errorf("V: %s", err) //nolint
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
				return fmt.Errorf("%s, %d (expected %d)", "invalid chainID", ctx.ChainID.Uint64(), ctx.cfg.ChainID.Uint64())
			}

			chainIDBits = ctx.ChainID.BitLen()
			if chainIDBits <= 7 {
				chainIDLen = 1
			} else {
				chainIDLen = common.BitLenToByteLen(chainIDBits) // It is always < 56 bytes
				sigHashLen++                                     // For chainId len Prefix
			}
			sigHashLen += uint(chainIDLen) // For chainId
			sigHashLen += 2                // For two extra zeros
			ctx.DeriveChainID.Sub(&ctx.V, &ctx.ChainIDMul)
			vByte = byte(ctx.DeriveChainID.Sub(&ctx.DeriveChainID, u256.N8).Uint64() - 27)
		}
	} else {
		var v uint64
		err = rlp.ReadElem(dec, rlp.Uint64, &v)
		if err != nil {
			return fmt.Errorf("V: %s", err) //nolint
		}
		if v > 1 {
			return fmt.Errorf("V is loo large: %d", v)
		}
		vByte = byte(v)
		ctx.IsProtected = true
	}

	// Next follows R of the signature
	err = rlp.ReadElem(dec, rlp.Uint256, &ctx.R)
	if err != nil {
		return fmt.Errorf("R: %s", err) //nolint
	}
	// New follows S of the signature
	err = rlp.ReadElem(dec, rlp.Uint256, &ctx.S)
	if err != nil {
		return fmt.Errorf("S: %s", err) //nolint
	}
	// For legacy transactions, just hash the full payload
	if legacy {
		if _, err = k1.Write(parent.Consumed()); err != nil {
			return fmt.Errorf("computing IdHash: %s", err) //nolint
		}
	}

	// write the hash to IdHash buffer
	//k1.Sum(slot.IDHash[:0])
	_, _ = k1.(io.Reader).Read(slot.IDHash[:32])

	if validateHash != nil {
		if err := validateHash(slot.IDHash[:32]); err != nil {
			return err
		}
	}

	if !ctx.withSender {
		return nil
	}

	if !crypto.TransactionSignatureIsValid(vByte, &ctx.R, &ctx.S, ctx.allowPreEip2s && legacy) {
		return fmt.Errorf("invalid v, r, s: %d, %s, %s", vByte, &ctx.R, &ctx.S)
	}

	// Computing sigHash (hash used to recover sender from the signature)
	// Write len Prefix to the Sighash
	if sigHashLen < 56 {
		ctx.buf[0] = byte(sigHashLen) + 192
		if _, err := k2.Write(ctx.buf[:1]); err != nil {
			return fmt.Errorf("computing signHash (hashing len Prefix1): %s", err) //nolint
		}
	} else {
		beLen := common.BitLenToByteLen(bits.Len(sigHashLen))
		binary.BigEndian.PutUint64(ctx.buf[1:], uint64(sigHashLen))
		ctx.buf[8-beLen] = byte(beLen) + 247
		if _, err := k2.Write(ctx.buf[8-beLen : 9]); err != nil {
			return fmt.Errorf("computing signHash (hashing len Prefix2): %s", err) //nolint
		}
	}
	if _, err = k2.Write(dec.Underlying()[sigHashPos:sigHashEnd]); err != nil {
		return fmt.Errorf("computing signHash: %s", err) //nolint
	}
	if legacy {
		if chainIDLen > 0 {
			if chainIDBits <= 7 {
				ctx.buf[0] = byte(ctx.ChainID.Uint64())
				if _, err := k2.Write(ctx.buf[:1]); err != nil {
					return fmt.Errorf("computing signHash (hashing legacy chainId): %s", err) //nolint
				}
			} else {
				binary.BigEndian.PutUint64(ctx.buf[1:9], ctx.ChainID[3])
				binary.BigEndian.PutUint64(ctx.buf[9:17], ctx.ChainID[2])
				binary.BigEndian.PutUint64(ctx.buf[17:25], ctx.ChainID[1])
				binary.BigEndian.PutUint64(ctx.buf[25:33], ctx.ChainID[0])
				ctx.buf[32-chainIDLen] = 128 + byte(chainIDLen)
				if _, err = k2.Write(ctx.buf[32-chainIDLen : 33]); err != nil {
					return fmt.Errorf("computing signHash (hashing legacy chainId): %s", err) //nolint
				}
			}
			// Encode two zeros
			ctx.buf[0] = 128
			ctx.buf[1] = 128
			if _, err := k2.Write(ctx.buf[:2]); err != nil {
				return fmt.Errorf("computing signHash (hashing zeros after legacy chainId): %s", err) //nolint
			}
		}
	}
	// Squeeze Sighash
	_, _ = k2.(io.Reader).Read(ctx.Sighash[:32])
	//k2.Sum(ctx.Sighash[:0])
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
		return fmt.Errorf("recovering sender from signature: %s", err) //nolint
	}
	//apply keccak to the public key
	k2.Reset()
	if _, err = k2.Write(ctx.buf[1:65]); err != nil {
		return fmt.Errorf("computing sender from public key: %s", err) //nolint
	}
	// squeeze the hash of the public key
	//k2.Sum(ctx.buf[:0])
	_, _ = k2.(io.Reader).Read(ctx.buf[:32])
	//take last 20 bytes as address
	copy(sender, ctx.buf[12:32])

	return nil
}

func (ctx *TxParseContext) decodeCommitments(dec *rlp.Decoder, slot *TxSlot) (err error) {
	err = dec.ForList(func(d *rlp.Decoder) error {
		var blob gokzg4844.KZGCommitment
		blobSlice := blob[:]
		err := rlp.ReadElem(d, rlp.BytesExact, &blobSlice)
		if err != nil {
			return err
		}
		slot.Commitments = append(slot.Commitments, blob)
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

func (ctx *TxParseContext) decodeProofs(dec *rlp.Decoder, slot *TxSlot) (err error) {
	err = dec.ForList(func(d *rlp.Decoder) error {
		var blob gokzg4844.KZGProof
		blobSlice := blob[:]
		err := rlp.ReadElem(d, rlp.BytesExact, &blobSlice)
		if err != nil {
			return err
		}
		slot.Proofs = append(slot.Proofs, blob)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (ctx *TxParseContext) decodeBlobs(dec *rlp.Decoder, slot *TxSlot) (err error) {
	err = dec.ForList(func(d *rlp.Decoder) error {
		var blob []byte
		err := rlp.ReadElem(d, rlp.Bytes, &blob)
		if err != nil {
			return err
		}
		slot.Blobs = append(slot.Blobs, blob)
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
