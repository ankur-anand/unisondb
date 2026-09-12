package kvdrivers

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newRegressionLMDB(t *testing.T) *LmdbEmbed {
	t.Helper()
	db, err := NewLmdb(t.TempDir(), Config{Namespace: "test", NoSync: true, MmapSize: 64 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestLMDBBatchDeleteRowsContinuesAfterNotFound(t *testing.T) {
	for name, rows := range map[string][][]byte{
		"missing_first":     {[]byte("z"), []byte("a"), []byte("b")},
		"delete_tail_first": {[]byte("b"), []byte("a")},
		"duplicate_deleted": {[]byte("b"), []byte("b"), []byte("a")},
	} {
		t.Run(name, func(t *testing.T) {
			db := newRegressionLMDB(t)
			columns := map[string][]byte{"c1": []byte("one"), "c2": []byte("two")}
			require.NoError(t, db.BatchSetCells([][]byte{[]byte("a"), []byte("b")}, []map[string][]byte{columns, columns}))
			deleted, err := db.BatchDeleteRows(rows)
			require.NoError(t, err)
			require.Equal(t, 4, deleted)
			for _, row := range [][]byte{[]byte("a"), []byte("b")} {
				_, err := db.ScanRowCells(row, nil)
				require.ErrorIs(t, err, ErrKeyNotFound)
			}
		})
	}
}

func TestLMDBTxnQueueUsesRestoredEnvironment(t *testing.T) {
	if runDriverSubprocess(t) {
		return
	}
	db := newRegressionLMDB(t)
	require.NoError(t, db.SetKV([]byte("snapshot"), []byte("value")))
	var snapshot bytes.Buffer
	require.NoError(t, db.Snapshot(&snapshot))
	queue := db.NewTxnQueue(10)
	require.NoError(t, queue.BatchPutKV([][]byte{[]byte("queued")}, [][]byte{[]byte("value")}))
	require.NoError(t, db.Restore(bytes.NewReader(snapshot.Bytes())))
	require.NoError(t, queue.Commit())
	value, err := db.GetKV([]byte("queued"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func TestLMDBRestoreWithConcurrentOperations(t *testing.T) {
	if runDriverSubprocess(t) {
		return
	}
	db := newRegressionLMDB(t)
	require.NoError(t, db.SetKV([]byte("key"), []byte("value")))
	require.NoError(t, db.StoreMetadata([]byte("meta"), []byte("value")))
	var snapshot bytes.Buffer
	require.NoError(t, db.Snapshot(&snapshot))
	queue := db.NewTxnQueue(1)
	ops := []func() error{
		func() error {
			value, err := db.GetKV([]byte("key"))
			if err != nil {
				return err
			}
			if !bytes.Equal(value, []byte("value")) {
				return fmt.Errorf("unexpected value: %q", value)
			}
			return nil
		},
		func() error { _, err := db.RetrieveMetadata([]byte("meta")); return err },
		func() error { return db.SetKV([]byte("written"), []byte("value")) },
		func() error { return queue.BatchPutKV([][]byte{[]byte("queued")}, [][]byte{[]byte("value")}) },
		db.FSync,
		func() error { var buf bytes.Buffer; return db.Snapshot(&buf) },
	}
	stop := make(chan struct{})
	started := make(chan struct{}, len(ops))
	results := make(chan error, len(ops))
	var wg sync.WaitGroup
	for _, op := range ops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			for {
				select {
				case <-stop:
					results <- nil
					return
				default:
				}
				if err := op(); err != nil {
					results <- err
					return
				}
			}
		}()
	}
	for range ops {
		<-started
	}
	var restoreErr error
	for i := 0; i < 20; i++ {
		if restoreErr = db.Restore(bytes.NewReader(snapshot.Bytes())); restoreErr != nil {
			break
		}
	}
	close(stop)
	wg.Wait()
	require.NoError(t, restoreErr)
	for range ops {
		require.NoError(t, <-results)
	}
}

func TestLMDBRestoreWaitsForActiveRead(t *testing.T) {
	if runDriverSubprocess(t) {
		return
	}
	db := newRegressionLMDB(t)
	require.NoError(t, db.BatchSetCells([][]byte{[]byte("row")}, []map[string][]byte{{"col": []byte("value")}}))
	var snapshot bytes.Buffer
	require.NoError(t, db.Snapshot(&snapshot))
	reading := make(chan struct{})
	releaseRead := make(chan struct{})
	readResult := make(chan error, 1)
	go func() {
		_, err := db.ScanRowCells([]byte("row"), func([]byte) bool {
			close(reading)
			<-releaseRead
			return true
		})
		readResult <- err
	}()
	<-reading
	restoreResult := make(chan error, 1)
	go func() { restoreResult <- db.Restore(bytes.NewReader(snapshot.Bytes())) }()
	select {
	case err := <-restoreResult:
		close(releaseRead)
		<-readResult
		t.Fatalf("restore returned while a read transaction was active: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseRead)
	require.NoError(t, <-readResult)
	require.NoError(t, <-restoreResult)
}

func TestLMDBClosedEnvironmentReturnsErrors(t *testing.T) {
	if runDriverSubprocess(t) {
		return
	}
	for _, failedRestore := range []bool{false, true} {
		t.Run(fmt.Sprintf("failedRestore=%t", failedRestore), func(t *testing.T) {
			db := newRegressionLMDB(t)
			queue := db.NewTxnQueue(10)
			require.NoError(t, queue.BatchPutKV([][]byte{[]byte("queued")}, [][]byte{[]byte("value")}))
			if failedRestore {
				require.Error(t, db.Restore(bytes.NewReader([]byte("invalid snapshot"))))
			} else {
				require.NoError(t, db.Close())
			}
			_, err := db.GetKV([]byte("key"))
			require.ErrorIs(t, err, ErrDatabaseClosed)
			require.ErrorIs(t, db.SetKV([]byte("key"), []byte("value")), ErrDatabaseClosed)
			_, err = db.RetrieveMetadata([]byte("meta"))
			require.ErrorIs(t, err, ErrDatabaseClosed)
			require.ErrorIs(t, db.StoreMetadata([]byte("meta"), []byte("value")), ErrDatabaseClosed)
			require.ErrorIs(t, db.FSync(), ErrDatabaseClosed)
			var snapshot bytes.Buffer
			require.ErrorIs(t, db.Snapshot(&snapshot), ErrDatabaseClosed)
			require.ErrorIs(t, queue.Commit(), ErrDatabaseClosed)
		})
	}
}
