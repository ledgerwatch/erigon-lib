package types

import (
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/rlp"
)

func DecodeTransactions(dec *rlp.Decoder, ctx *TxParseContext, txSlots *TxSlots, validateHash func([]byte) error) (err error) {
	i := 0
	err = dec.ForList(func(d *rlp.Decoder) error {
		txSlots.Resize(uint(i + 1))
		txSlots.Txs[i] = &TxSlot{}
		err = ctx.DecodeTransaction(d, txSlots.Txs[i], txSlots.Senders.At(i), true /* hasEnvelope */, true /* wrappedWithBlobs */, validateHash)
		if err != nil {
			if errors.Is(err, ErrRejected) {
				txSlots.Resize(uint(i))
				return nil
			}
			return fmt.Errorf("elem: %w", err)
		}
		i = i + 1
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
