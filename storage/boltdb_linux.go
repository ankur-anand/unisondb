//go:build linux

package storage

import (
	"bufio"
	"io"

	"go.etcd.io/bbolt"
)

func (b *boltdb) SnapShot(w io.Writer) error {
	// helps in performance, if the provided writer doesn't
	// have buffer.
	wn := bufio.NewWriter(w)
	return b.db.View(func(tx *bbolt.Tx) error {
		// On Linux, enable direct IO for efficient database copying.
		tx.WriteFlag = syscall.O_DIRECT

		_, err := tx.WriteTo(wn)
		return err
	})
}
