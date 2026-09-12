package kvdrivers

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestBoltdbNoSyncSurvivesRestore(t *testing.T) {
	for _, noSync := range []bool{false, true} {
		t.Run(fmt.Sprintf("NoSync=%t", noSync), func(t *testing.T) {
			conf := Config{Namespace: "test", NoSync: noSync}
			db, err := NewBoltdb(filepath.Join(t.TempDir(), "test.db"), conf)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			assert.Equal(t, noSync, db.db.NoSync, "new databases must honor NoSync")
			require.NoError(t, db.SetKV([]byte("key"), []byte("value")))
			var snapshot bytes.Buffer
			require.NoError(t, db.Snapshot(&snapshot))
			for i := 0; i < 2; i++ {
				require.NoError(t, db.Restore(bytes.NewReader(snapshot.Bytes())))
				assert.Equal(t, noSync, db.db.NoSync, "restore must preserve NoSync")
				value, err := db.GetKV([]byte("key"))
				require.NoError(t, err)
				require.Equal(t, []byte("value"), value)
			}
		})
	}
}

func TestBoltTxnQueueMissingBucketReleasesWriter(t *testing.T) {
	if runDriverSubprocess(t) {
		return
	}
	for _, batchSize := range []int{1, 10} {
		t.Run(fmt.Sprintf("batchSize=%d", batchSize), func(t *testing.T) {
			db, err := NewBoltdb(filepath.Join(t.TempDir(), "test.db"), Config{Namespace: "test"})
			require.NoError(t, err)
			require.NoError(t, db.db.Update(func(tx *bbolt.Tx) error {
				return tx.DeleteBucket(db.namespace)
			}))
			queue := db.NewTxnQueue(batchSize)
			err = queue.BatchPutKV([][]byte{[]byte("key")}, [][]byte{[]byte("value")})
			if batchSize > 1 {
				require.NoError(t, err)
				err = queue.Commit()
			}
			require.ErrorIs(t, err, ErrBucketNotFound)

			done := make(chan error, 1)
			go func() {
				err := db.db.Update(func(tx *bbolt.Tx) error {
					bucket, err := tx.CreateBucket(db.namespace)
					if err != nil {
						return err
					}
					return bucket.Put(KeyKV([]byte("next")), []byte("value"))
				})
				if err == nil {
					err = db.Close()
				}
				done <- err
			}()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("write or Close blocked after a missing-bucket error")
			}
		})
	}
}

func TestBoltTxnQueuePanicReleasesWriter(t *testing.T) {
	if runDriverSubprocess(t) {
		return
	}
	db, err := NewBoltdb(filepath.Join(t.TempDir(), "test.db"), Config{Namespace: "test"})
	require.NoError(t, err)
	queue := db.NewTxnQueue(10)
	queue.opsQueue = append(queue.opsQueue, func(bucket *bbolt.Bucket) error {
		if err := bucket.Put(KeyKV([]byte("uncommitted")), []byte("value")); err != nil {
			return err
		}
		panic("operation panic")
	})
	require.PanicsWithValue(t, "operation panic", func() { _ = queue.Commit() })
	done := make(chan error, 1)
	go func() {
		err := db.SetKV([]byte("next"), []byte("value"))
		if err == nil {
			_, err = db.GetKV([]byte("uncommitted"))
			if err == ErrKeyNotFound {
				err = db.Close()
			} else {
				err = fmt.Errorf("panicking transaction was not rolled back: %v", err)
			}
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("write or Close blocked after an operation panic")
	}
}
