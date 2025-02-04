//go:build !linux

package storage

import (
	"bufio"
	"io"

	"go.etcd.io/bbolt"
)

func (e *Engine) PersistenceSnapShot(w io.Writer) error {
	// helps in performance, if the provided writer doesn't
	// have buffer.
	wn := bufio.NewWriter(w)
	return e.db.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(wn)
		if err != nil {
			return err
		}
		return nil
	})
}
