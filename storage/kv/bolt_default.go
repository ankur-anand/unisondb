//go:build !linux

package kv

import (
	"bufio"
	"io"

	"go.etcd.io/bbolt"
)

func (b *BoltDBEmbed) Snapshot(w io.Writer) error {
	// helps in performance, if the provided writer doesn't
	// have buffer.
	wn := bufio.NewWriter(w)
	return b.db.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(wn)
		return err
	})
}
