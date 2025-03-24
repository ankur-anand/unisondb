//go:build !linux

package kvdrivers

import (
	"bufio"
	"io"

	"go.etcd.io/bbolt"
)

func (b *BoltDBEmbed) Snapshot(w io.Writer) error {
	// helps in performance, if the provided writer doesn't
	// have buffer.
	wn := bufio.NewWriter(w)
	err := b.db.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(wn)
		return err
	})

	if err != nil {
		return err
	}
	return wn.Flush()
}
