package dbkernel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/memtable"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Stop background workers so each test controls exactly which writes are
// checkpointed. WAL append, flush, fsync, close and recovery use real storage.
func openLSNTestEngine(t *testing.T, dir string, backend DBEngine) *Engine {
	t.Helper()
	conf := NewDefaultEngineConfig()
	conf.DBEngine = backend
	conf.ArenaSize = minArenaSize
	conf.WalConfig.SegmentSize = 4096
	conf.WalConfig.AutoCleanup = false
	engine, err := NewStorageEngine(dir, "lsn", conf)
	require.NoError(t, err)
	engine.cancel()
	engine.wg.Wait()
	t.Cleanup(func() {
		if !engine.shutdown.Load() {
			require.NoError(t, engine.Close(context.Background()))
		}
	})
	return engine
}

type failLSNTestCommit struct {
	internal.TxnBatcher
	fail *bool
}

func (b *failLSNTestCommit) Commit() error {
	if *b.fail {
		*b.fail = false
		return errors.New("injected B-tree commit failure")
	}
	return b.TxnBatcher.Commit()
}

func TestChunkedFlushRetryRecordCount(t *testing.T) {
	for _, backend := range []DBEngine{BoltDBEngine, LMDBEngine} {
		t.Run(string(backend), func(t *testing.T) {
			e := openLSNTestEngine(t, t.TempDir(), backend)
			fail := true
			e.activeMemTable = memtable.NewMemTable(e.config.ArenaSize, e.walIO, e.namespace, func(size int) internal.TxnBatcher {
				return &failLSNTestCommit{TxnBatcher: e.newTxnBatcher(size), fail: &fail}
			})
			for _, key := range []string{"one", "two"} {
				require.NoError(t, prepareLSNTestTxn(t, e, logrecord.LogEntryTypeChunked, key, []byte("value")).Commit())
			}
			count, err := e.activeMemTable.Flush(context.Background())
			require.ErrorContains(t, err, "injected B-tree commit failure")
			require.Zero(t, count)
			count, err = e.activeMemTable.Flush(context.Background())
			require.NoError(t, err)
			require.Equal(t, 8, count, "retry must count each of the eight WAL records once")
		})
	}
}

func TestLSNRecoveryRejectsInvalidSequence(t *testing.T) {
	for _, sequence := range [][]uint64{{0}, {1, 1}, {1, 3}, {1, 2, 1}} {
		t.Run(fmt.Sprint(sequence), func(t *testing.T) {
			dir := t.TempDir()
			e := openLSNTestEngine(t, dir, BoltDBEngine)
			for _, lsn := range sequence {
				record := logcodec.LogRecord{
					LSN: lsn, TxnState: logrecord.TransactionStateBegin, EntryType: logrecord.LogEntryTypeKV,
				}
				_, err := e.walIO.Append(record.FBEncode(128), lsn)
				require.NoError(t, err)
			}
			conf := *e.config
			require.NoError(t, e.Close(context.Background()))
			e, err := NewStorageEngine(dir, "lsn", &conf)
			if e != nil {
				require.NoError(t, e.Close(context.Background()))
			}
			require.ErrorContains(t, err, "invalid WAL LSN")
		})
	}
}

func TestLSNRecoveryRequiresCheckpointRecord(t *testing.T) {
	dir := t.TempDir()
	e := openLSNTestEngine(t, dir, BoltDBEngine)
	require.NoError(t, e.PutKV([]byte("key"), []byte("value")))
	checkpointLSNTestEngine(t, e)
	meta, err := e.GetWalCheckPoint()
	require.NoError(t, err)
	meta.Pos.Offset = 4000 // Inside the segment, beyond its last record.
	require.NoError(t, e.dataStore.StoreMetadata(internal.SysKeyWalCheckPoint, meta.MarshalBinary()))
	conf := *e.config
	require.NoError(t, e.Close(context.Background()))
	e, err = NewStorageEngine(dir, "lsn", &conf)
	if e != nil {
		require.NoError(t, e.Close(context.Background()))
	}
	require.ErrorContains(t, err, "checkpoint")
}

func TestLSNRecoveryRejectsInconsistentCheckpointIndex(t *testing.T) {
	for _, badLSN := range []uint64{1, 3} {
		t.Run(fmt.Sprint(badLSN), func(t *testing.T) {
			dir := t.TempDir()
			e := openLSNTestEngine(t, dir, BoltDBEngine)
			require.NoError(t, e.PutKV([]byte("first"), []byte("value")))
			record := logcodec.LogRecord{LSN: badLSN, TxnState: logrecord.TransactionStateBegin}
			pos, err := e.walIO.Append(record.FBEncode(128), badLSN)
			require.NoError(t, err)
			meta := internal.Metadata{Pos: pos, RecordProcessed: 2}
			require.NoError(t, e.dataStore.StoreMetadata(internal.SysKeyWalCheckPoint, meta.MarshalBinary()))
			conf := *e.config
			require.NoError(t, e.Close(context.Background()))
			e, err = NewStorageEngine(dir, "lsn", &conf)
			if e != nil {
				require.NoError(t, e.Close(context.Background()))
			}
			require.ErrorContains(t, err, "checkpoint LSN")
		})
	}
}

func TestLocalTxnLSNWithoutCommittedData(t *testing.T) {
	for _, backend := range []DBEngine{BoltDBEngine, LMDBEngine} {
		for _, kind := range []logrecord.LogEntryType{logrecord.LogEntryTypeKV, logrecord.LogEntryTypeRow, logrecord.LogEntryTypeChunked} {
			for _, state := range []string{"begin", "prepared", "aborted"} {
				t.Run(fmt.Sprintf("%s/%s/%s", backend, kind, state), func(t *testing.T) {
					dir := t.TempDir()
					e := openLSNTestEngine(t, dir, backend)
					var lastLSN uint64 = 1
					if state == "begin" {
						_, err := e.NewTxn(logrecord.LogOperationTypeInsert, kind)
						require.NoError(t, err)
					} else {
						txn := prepareLSNTestTxn(t, e, kind, "hidden", []byte("value"))
						if state == "aborted" {
							txn.Abort()
						}
						lastLSN = 3
					}
					require.NoError(t, e.Close(context.Background()))
					// Reopen twice before any new writes: recovery must retain the
					// sequence even when it materializes zero committed operations.
					for range 2 {
						e = openLSNTestEngine(t, dir, backend)
						assert.Equal(t, lastLSN, e.OpsReceivedCount())
						var err error
						switch kind {
						case logrecord.LogEntryTypeKV:
							_, err = e.GetKV([]byte("hidden0"))
						case logrecord.LogEntryTypeRow:
							_, err = e.GetRowColumns("hidden", nil)
						case logrecord.LogEntryTypeChunked:
							_, err = e.GetLOB([]byte("hidden"))
						}
						require.ErrorIs(t, err, ErrKeyNotFound)
						require.NoError(t, e.Close(context.Background()))
					}
					e = openLSNTestEngine(t, dir, backend)
					require.NoError(t, e.PutKV([]byte("next"), []byte("value")))
					assert.Equal(t, lastLSN+1, checkLSNTestWAL(t, e, nil))
				})
			}
		}
	}
}

func TestLocalOversizedAppendDoesNotConsumeLSN(t *testing.T) {
	e := openLSNTestEngine(t, t.TempDir(), BoltDBEngine)
	txn, err := e.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)
	err = txn.AppendKVTxn([]byte("large"), bytes.Repeat([]byte("v"), 4096))
	require.Error(t, err)
	assert.EqualValues(t, 1, e.OpsReceivedCount())
	require.NoError(t, e.PutKV([]byte("next"), []byte("value")))
	assert.EqualValues(t, 2, checkLSNTestWAL(t, e, nil))
}

func TestLocalTxnLSNAcrossMemtables(t *testing.T) {
	for _, backend := range []DBEngine{BoltDBEngine, LMDBEngine} {
		for _, kind := range []logrecord.LogEntryType{logrecord.LogEntryTypeKV, logrecord.LogEntryTypeRow} {
			t.Run(fmt.Sprintf("%s/%s", backend, kind), func(t *testing.T) {
				dir := t.TempDir()
				e := openLSNTestEngine(t, dir, backend)
				txn, err := e.NewTxn(logrecord.LogOperationTypeInsert, kind)
				require.NoError(t, err)
				value := bytes.Repeat([]byte("v"), 900)
				for i := range 240 {
					key := []byte(fmt.Sprint(i))
					if kind == logrecord.LogEntryTypeKV {
						require.NoError(t, txn.AppendKVTxn(key, value))
					} else {
						require.NoError(t, txn.AppendColumnTxn(key, map[string][]byte{"column": value}))
					}
				}
				require.NoError(t, txn.Commit())
				require.NotEmpty(t, e.sealedMemTables)
				checkpointLSNTestEngine(t, e)
				assert.EqualValues(t, 242, e.OpsFlushedCount())
				require.NoError(t, e.Close(context.Background()))
				e = openLSNTestEngine(t, dir, backend)
				assert.EqualValues(t, 242, e.OpsReceivedCount())
				require.NoError(t, e.PutKV([]byte("after"), value))
				assert.EqualValues(t, 243, checkLSNTestWAL(t, e, nil))
				for _, key := range []string{"0", "239"} {
					if kind == logrecord.LogEntryTypeKV {
						got, err := e.GetKV([]byte(key))
						require.NoError(t, err)
						require.Equal(t, value, got)
					} else {
						got, err := e.GetRowColumns(key, nil)
						require.NoError(t, err)
						require.Equal(t, map[string][]byte{"column": value}, got)
					}
				}
			})
		}
	}
}

func checkpointLSNTestEngine(t *testing.T, engine *Engine) {
	t.Helper()
	if !engine.activeMemTable.IsEmpty() {
		engine.rotateMemTableNoFlush()
	}
	for len(engine.sealedMemTables) > 0 {
		before := len(engine.sealedMemTables)
		engine.handleFlush(context.Background())
		require.Less(t, len(engine.sealedMemTables), before, "flush must progress")
	}
	engine.fSyncStore()
}

func prepareLSNTestTxn(t *testing.T, engine *Engine, kind logrecord.LogEntryType, key string, value []byte) *Txn {
	t.Helper()
	txn, err := engine.NewTxn(logrecord.LogOperationTypeInsert, kind)
	require.NoError(t, err)
	for i := range 2 {
		switch kind {
		case logrecord.LogEntryTypeRow:
			require.NoError(t, txn.AppendColumnTxn([]byte(key), map[string][]byte{fmt.Sprint(i): value}))
		case logrecord.LogEntryTypeKV:
			require.NoError(t, txn.AppendKVTxn([]byte(fmt.Sprintf("%s%d", key, i)), value))
		case logrecord.LogEntryTypeChunked:
			require.NoError(t, txn.AppendKVTxn([]byte(key), value))
		}
	}
	return txn
}

func checkLSNTestValue(t *testing.T, engine *Engine, kind logrecord.LogEntryType, key string, value []byte) {
	t.Helper()
	switch kind {
	case logrecord.LogEntryTypeKV:
		for i := range 2 {
			got, err := engine.GetKV([]byte(fmt.Sprintf("%s%d", key, i)))
			require.NoError(t, err)
			require.Equal(t, value, got)
		}
	case logrecord.LogEntryTypeChunked:
		got, err := engine.GetLOB([]byte(key))
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat(value, 2), got)
	case logrecord.LogEntryTypeRow:
		got, err := engine.GetRowColumns(key, nil)
		require.NoError(t, err)
		require.Equal(t, map[string][]byte{"0": value, "1": value}, got)
	}
}

// Check both the physical record order and the LSN index. Optionally copy the
// stream to a real follower before restarting the source.
func checkLSNTestWAL(t *testing.T, engine *Engine, follower *ReplicaWALHandler) uint64 {
	t.Helper()
	reader, err := engine.NewReader()
	require.NoError(t, err)
	defer reader.Close()
	var count uint64
	for {
		data, pos, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		count++
		assert.Equal(t, count, logrecord.GetRootAsLogRecord(data, 0).Lsn())
		indexed, err := engine.walIO.WAL().PositionForIndex(count)
		assert.NoError(t, err)
		assert.Equal(t, pos, indexed, "index for LSN %d", count)
		if follower != nil {
			require.NoError(t, follower.ApplyRecord(bytes.Clone(data), pos))
		}
	}
	return count
}

func TestLocalTxnLSNRecovery(t *testing.T) {
	for _, backend := range []DBEngine{BoltDBEngine, LMDBEngine} {
		for _, kind := range []logrecord.LogEntryType{logrecord.LogEntryTypeKV, logrecord.LogEntryTypeRow, logrecord.LogEntryTypeChunked} {
			for _, scenario := range []string{
				"uncheckpointed", "checkpointed", "empty_active_segment", "legacy_low_count", "legacy_high_count",
				"aborted_before_checkpoint", "unfinished_after_checkpoint", "begin_only_after_checkpoint",
				"interleaved_commit", "overwrite", "segment_rotation",
			} {
				t.Run(fmt.Sprintf("%s/%s/%s", backend, kind, scenario), func(t *testing.T) {
					dir := t.TempDir()
					e := openLSNTestEngine(t, dir, backend)
					var other *Txn
					if scenario == "aborted_before_checkpoint" || scenario == "interleaved_commit" {
						other = prepareLSNTestTxn(t, e, kind, "other", []byte("other"))
						if scenario == "aborted_before_checkpoint" {
							other.Abort()
						}
					}
					value := []byte("value")
					if scenario == "segment_rotation" {
						value = bytes.Repeat([]byte("v"), 1800)
					}
					txn := prepareLSNTestTxn(t, e, kind, "key", value)
					require.NoError(t, txn.Commit())
					if scenario == "overwrite" {
						value = []byte("replacement")
						require.NoError(t, prepareLSNTestTxn(t, e, kind, "key", value).Commit())
					}
					if scenario != "uncheckpointed" {
						checkpointLSNTestEngine(t, e)
					}
					if scenario == "checkpointed" {
						assert.EqualValues(t, 4, e.OpsFlushedCount(), "Begin + two Prepare + Commit")
					}
					switch scenario {
					case "legacy_low_count", "legacy_high_count":
						meta, err := e.GetWalCheckPoint()
						require.NoError(t, err)
						if scenario == "legacy_low_count" {
							meta.RecordProcessed = 1
						} else {
							meta.RecordProcessed = 100
						}
						require.NoError(t, e.dataStore.StoreMetadata(internal.SysKeyWalCheckPoint, meta.MarshalBinary()))
					case "unfinished_after_checkpoint":
						prepareLSNTestTxn(t, e, kind, "unfinished", []byte("hidden"))
					case "begin_only_after_checkpoint":
						_, err := e.NewTxn(logrecord.LogOperationTypeInsert, kind)
						require.NoError(t, err)
					case "interleaved_commit":
						require.NoError(t, other.Commit())
					}
					follower := openLSNTestEngine(t, t.TempDir(), backend)
					handler := NewReplicaWALHandler(follower)
					lastLSN := checkLSNTestWAL(t, e, handler)
					if scenario == "empty_active_segment" {
						require.NoError(t, e.walIO.WAL().RotateSegment())
					}
					if scenario == "segment_rotation" {
						require.Greater(t, len(e.walIO.WAL().Segments()), 1)
					}
					require.NoError(t, e.Close(context.Background()))
					for restart := range 2 {
						e = openLSNTestEngine(t, dir, backend)
						assert.Equal(t, lastLSN, e.OpsReceivedCount(), "restart %d", restart)
						checkLSNTestValue(t, e, kind, "key", value)
						if scenario == "interleaved_commit" {
							checkLSNTestValue(t, e, kind, "other", []byte("other"))
						}
						require.NoError(t, e.PutKV([]byte(fmt.Sprintf("next%d", restart)), []byte("value")))
						data, err := e.walIO.Read(e.CurrentOffset())
						require.NoError(t, err)
						lastLSN++
						assert.Equal(t, lastLSN, logrecord.GetRootAsLogRecord(data, 0).Lsn())
						assert.NoError(t, handler.ApplyRecordsLSNOnly([][]byte{bytes.Clone(data)}))
						assert.Equal(t, lastLSN, checkLSNTestWAL(t, e, nil))
						require.NoError(t, e.Close(context.Background()))
					}
				})
			}
		}
	}
}

func TestLocalAppendFailureDoesNotConsumeLSN(t *testing.T) {
	for _, operation := range []string{"begin", "prepare_kv", "prepare_row", "commit", "put_kv", "put_row"} {
		t.Run(operation, func(t *testing.T) {
			dir := t.TempDir()
			e := openLSNTestEngine(t, dir, BoltDBEngine)
			var appendRecord func() error
			switch operation {
			case "begin":
				appendRecord = func() error {
					_, err := e.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
					return err
				}
			case "prepare_kv", "prepare_row", "commit":
				kind := logrecord.LogEntryTypeKV
				if operation == "prepare_row" {
					kind = logrecord.LogEntryTypeRow
				}
				txn := prepareLSNTestTxn(t, e, kind, "key", []byte("value"))
				switch operation {
				case "prepare_kv":
					appendRecord = func() error { return txn.AppendKVTxn([]byte("another"), []byte("value")) }
				case "prepare_row":
					appendRecord = func() error { return txn.AppendColumnTxn([]byte("another"), map[string][]byte{"c": []byte("v")}) }
				case "commit":
					appendRecord = txn.Commit
				}
			case "put_kv":
				appendRecord = func() error { return e.PutKV([]byte("key"), []byte("value")) }
			case "put_row":
				appendRecord = func() error { return e.PutColumnsForRow([]byte("row"), map[string][]byte{"c": []byte("v")}) }
			}
			// A sealed segment rejects the append before writing any bytes.
			require.NoError(t, e.walIO.WAL().Current().SealSegment())
			before := e.OpsReceivedCount()
			require.Error(t, appendRecord())
			assert.Equal(t, before, e.OpsReceivedCount())
			require.NoError(t, e.walIO.WAL().RotateSegment())
			require.NoError(t, e.PutKV([]byte("after_failure"), []byte("value")))
			assert.Equal(t, before+1, checkLSNTestWAL(t, e, nil))
			require.NoError(t, e.Close(context.Background()))
			e = openLSNTestEngine(t, dir, BoltDBEngine)
			assert.Equal(t, before+1, e.OpsReceivedCount())
		})
	}
}
