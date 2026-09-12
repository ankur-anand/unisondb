package dbkernel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/stretchr/testify/require"
)

func openTxnIndexEngine(t *testing.T, dir string, backend DBEngine, size int64) *Engine {
	t.Helper()
	conf := NewDefaultEngineConfig()
	conf.DBEngine, conf.ArenaSize = backend, minArenaSize
	conf.WalConfig.SegmentSize, conf.WalConfig.AutoCleanup = size, false
	e, err := NewStorageEngine(dir, "txn-index", conf)
	require.NoError(t, err)
	e.cancel()
	e.wg.Wait()
	t.Cleanup(func() {
		if !e.shutdown.Load() {
			require.NoError(t, e.Close(context.Background()))
		}
	})
	return e
}

type txnIndexWriter interface {
	AppendKVTxn([]byte, []byte) error
	AppendColumnTxn([]byte, map[string][]byte) error
	Commit() error
}

func newTxnIndexWriter(t *testing.T, e *Engine, kind logrecord.LogEntryType) txnIndexWriter {
	t.Helper()
	txn, err := e.NewTxn(logrecord.LogOperationTypeInsert, kind)
	require.NoError(t, err)
	return txn
}

func appendTxnIndexValue(t *testing.T, txn txnIndexWriter, kind logrecord.LogEntryType, key string, i int, value []byte) {
	t.Helper()
	switch kind {
	case logrecord.LogEntryTypeKV:
		require.NoError(t, txn.AppendKVTxn([]byte(fmt.Sprintf("%s%d", key, i)), value))
	case logrecord.LogEntryTypeRow:
		require.NoError(t, txn.AppendColumnTxn([]byte(key), map[string][]byte{fmt.Sprint(i): value}))
	case logrecord.LogEntryTypeChunked:
		require.NoError(t, txn.AppendKVTxn([]byte(key), value))
	}
}

func requireTxnIndexInvisible(t *testing.T, e *Engine, kind logrecord.LogEntryType) {
	t.Helper()
	var err error
	switch kind {
	case logrecord.LogEntryTypeKV:
		_, err = e.GetKV([]byte("main0"))
	case logrecord.LogEntryTypeRow:
		_, err = e.GetRowColumns("main", nil)
	case logrecord.LogEntryTypeChunked:
		_, err = e.GetLOB([]byte("main"))
	}
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// A source engine and its ISR follower segment the WAL differently, so the same
// logical transaction lands at different physical offsets on each side. The
// transaction chain must still resolve by logical index on the follower, across
// reopen-before-flush and reopen-after-flush.
func TestTransactionIndexesAcrossWALLayouts(t *testing.T) {
	for _, backend := range []DBEngine{BoltDBEngine, LMDBEngine} {
		for _, kind := range []logrecord.LogEntryType{logrecord.LogEntryTypeKV, logrecord.LogEntryTypeRow, logrecord.LogEntryTypeChunked} {
			t.Run(fmt.Sprintf("%s/%s", backend, kind), func(t *testing.T) {
				source := openTxnIndexEngine(t, t.TempDir(), backend, 4096)
				dir := t.TempDir()
				follower := openTxnIndexEngine(t, dir, backend, 1024)

				mainTxn := newTxnIndexWriter(t, source, kind)
				otherTxn := newTxnIndexWriter(t, source, kind)
				value := bytes.Repeat([]byte("v"), 600)
				for i := range 2 {
					appendTxnIndexValue(t, mainTxn, kind, "main", i, value)
					appendTxnIndexValue(t, otherTxn, kind, "other", i, []byte("other"))
				}
				require.NoError(t, otherTxn.Commit())
				requireTxnIndexInvisible(t, source, kind)
				require.NoError(t, mainTxn.Commit())
				checkLSNTestValue(t, source, kind, "main", value)
				expectedLast := source.OpsReceivedCount()

				var last uint64
				handler := NewReplicaWALHandler(follower)
				reader, err := source.NewReader()
				require.NoError(t, err)
				defer reader.Close()
				for {
					data, _, err := reader.Next()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					last = logrecord.GetRootAsLogRecord(data, 0).Lsn()
					if last == expectedLast {
						requireTxnIndexInvisible(t, follower, kind)
					}
					require.NoError(t, handler.ApplyRecordsLSNOnly([][]byte{bytes.Clone(data)}))
				}

				require.Equal(t, expectedLast, last)
				leaderPos, err := source.walIO.WAL().PositionForIndex(last)
				require.NoError(t, err)
				followerPos, err := follower.walIO.WAL().PositionForIndex(last)
				require.NoError(t, err)
				require.NotEqual(t, leaderPos, followerPos, "fixture must exercise distinct physical layouts")
				checkLSNTestValue(t, follower, kind, "main", value)
				checkLSNTestValue(t, follower, kind, "other", []byte("other"))

				// First reopen before flushing, rebuild the local index and replay.
				// Then flush and reopen again to verify the persisted values.
				for restart := range 2 {
					require.NoError(t, follower.Close(context.Background()))
					follower = openTxnIndexEngine(t, dir, backend, 2048)
					checkLSNTestValue(t, follower, kind, "main", value)
					checkLSNTestValue(t, follower, kind, "other", []byte("other"))
					if restart == 0 {
						checkpointLSNTestEngine(t, follower)
						checkLSNTestValue(t, follower, kind, "main", value)
					}
				}
			})
		}
	}
}
