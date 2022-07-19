package kv

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/torquem-ch/mdbx-go/mdbx"
	"go.uber.org/atomic"
)

func DefaultPageSize() uint64 {
	osPageSize := os.Getpagesize()
	if osPageSize < 4096 { // reduce further may lead to errors (because some data is just big)
		osPageSize = 4096
	} else if osPageSize > mdbx.MaxPageSize {
		osPageSize = mdbx.MaxPageSize
	}
	osPageSize = osPageSize / 4096 * 4096 // ensure it's rounded
	return uint64(osPageSize)
}

// BigChunks - read `table` by big chunks - restart read transaction after each 1 minutes
func BigChunks(db RoDB, table string, from []byte, walker func(tx Tx, k, v []byte) (bool, error)) error {
	rollbackEvery := time.NewTicker(1 * time.Minute)

	var stop bool
	for !stop {
		if err := db.View(context.Background(), func(tx Tx) error {
			c, err := tx.Cursor(table)
			if err != nil {
				return err
			}
			defer c.Close()

			k, v, err := c.Seek(from)
		Loop:
			for ; k != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}

				// break loop before walker() call, to make sure all keys are received by walker() exactly once
				select {
				case <-rollbackEvery.C:

					break Loop
				default:
				}

				ok, err := walker(tx, k, v)
				if err != nil {
					return err
				}
				if !ok {
					stop = true
					break
				}
			}

			if k == nil {
				stop = true
			}

			from = common.Copy(k) // next transaction will start from this key

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

var (
	bytesTrue  = []byte{1}
	bytesFalse = []byte{0}
)

func bytes2bool(in []byte) bool {
	if len(in) < 1 {
		return false
	}
	return in[0] == 1
}

var ErrChanged = fmt.Errorf("key must not change")

// EnsureNotChangedBool - used to store immutable config flags in db. protects from human mistakes
func EnsureNotChangedBool(tx GetPut, bucket string, k []byte, value bool) (ok, enabled bool, err error) {
	vBytes, err := tx.GetOne(bucket, k)
	if err != nil {
		return false, enabled, err
	}
	if vBytes == nil {
		if value {
			vBytes = bytesTrue
		} else {
			vBytes = bytesFalse
		}
		if err := tx.Put(bucket, k, vBytes); err != nil {
			return false, enabled, err
		}
	}

	enabled = bytes2bool(vBytes)
	return value == enabled, enabled, nil
}

func GetBool(tx Getter, bucket string, k []byte) (enabled bool, err error) {
	vBytes, err := tx.GetOne(bucket, k)
	if err != nil {
		return false, err
	}
	return bytes2bool(vBytes), nil
}

func ReadAhead(ctx context.Context, db RoDB, progress *atomic.Bool, table string, from []byte, amount uint32) {
	if progress.Load() {
		return
	}
	progress.Store(true)
	go func() {
		defer progress.Store(false)
		_ = db.View(ctx, func(tx Tx) error {
			c, err := tx.Cursor(table)
			if err != nil {
				return err
			}
			defer c.Close()

			for k, _, err := c.Seek(from); k != nil && amount > 0; k, _, err = c.Next() {
				if err != nil {
					return err
				}
				amount--
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			return nil
		})
	}()
}
