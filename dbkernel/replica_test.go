package dbkernel_test

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"maps"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicaWALHandler_ApplyRecord(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"
	replicatorNameSpace := "test_persistence_replicator"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	replicaDir := filepath.Join(baseDir, "replica")

	replicaEngine, err := dbkernel.NewStorageEngine(replicaDir, replicatorNameSpace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := replicaEngine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close replica: %v", err)
		}
	})

	replicator := dbkernel.NewReplicaWALHandler(replicaEngine)

	kvInserted := make(map[string][]byte)

	t.Run("insert_tx_none_kv", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key_%d", i)
			value := gofakeit.Sentence(i + 1)
			kvInserted[key] = []byte(value)
			err := engine.PutKV([]byte(key), []byte(value))
			assert.NoError(t, err)
		}
	})

	t.Run("replicate_tx_none_kv", func(t *testing.T) {
		reader, err := engine.NewReader()
		assert.NoError(t, err, "error creating reader")
		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "error reading from reader")
			err = replicator.ApplyRecord(walEncoded, offset)
			assert.NoError(t, err, "error applying record to replicator")
		}
	})

	t.Run("validate_replicate_tx_none_kv", func(t *testing.T) {
		for k, v := range kvInserted {
			got, err := replicaEngine.GetKV([]byte(k))
			assert.NoError(t, err, "error reading from replicator")
			assert.Equal(t, v, got, "invalid replicator value")
		}
	})

	var rowKeys [][]byte
	var columnsEntries []map[string][]byte

	t.Run("batch_insert_tx_none_row_columns", func(t *testing.T) {
		for i := uint64(0); i < 100; i++ {
			rowKey := gofakeit.UUID()
			entries := make(map[string][]byte)
			for k := 0; k < 10; k++ {
				key := gofakeit.Name()
				val := gofakeit.LetterN(uint(i + 1))
				entries[key] = []byte(val)
			}

			rowKeys = append(rowKeys, []byte(rowKey))
			columnsEntries = append(columnsEntries, entries)
		}

		err := engine.PutColumnsForRows(rowKeys, columnsEntries)
		assert.NoError(t, err, "PutColumnsForRows operation should succeed")
	})

	t.Run("replicate_tx_row_columns", func(t *testing.T) {
		fromOffset := replicaEngine.CurrentOffset()
		reader, err := getReader(engine, fromOffset)
		assert.NoError(t, err, "error creating reader")
		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "error reading from reader")
			err = replicator.ApplyRecord(walEncoded, offset)
			assert.NoError(t, err, "error applying record to replicator")
		}
	})

	t.Run("validate_replicate_tx_row_columns", func(t *testing.T) {
		for i, key := range rowKeys {
			got, err := replicaEngine.GetRowColumns(string(key), nil)
			assert.NoError(t, err, "error reading from replicator")
			assert.Equal(t, columnsEntries[i], got, "invalid replicator value")
		}
	})

	var deletedColumn string
	t.Run("delete_tx_none_rowColumns", func(t *testing.T) {
		deletedColumn = gofakeit.RandomMapKey(columnsEntries[0]).(string)
		deleteColumn := map[string][]byte{deletedColumn: nil}
		err := engine.DeleteColumnsForRow(rowKeys[0], deleteColumn)
		assert.NoError(t, err, "DeleteColumnsForRow operation should succeed")
	})

	t.Run("replicate_tx_row_columns", func(t *testing.T) {
		fromOffset := replicaEngine.CurrentOffset()
		reader, err := getReader(engine, fromOffset)
		assert.NoError(t, err, "error creating reader")
		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "error reading from reader")
			err = replicator.ApplyRecord(walEncoded, offset)
			assert.NoError(t, err, "error applying record to replicator")
		}
	})

	t.Run("validate_replicate_tx_row_delete_columns", func(t *testing.T) {
		got, err := replicaEngine.GetRowColumns(string(rowKeys[0]), nil)
		assert.NoError(t, err, "error reading from replicator")
		assert.NotContains(t, got, deletedColumn)
		assert.Equal(t, 9, len(got))
	})

	kvTXN, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	assert.NoError(t, err, "error creating txn")

	t.Run("insert_tx_kv_non_commit", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("txn_key_%d", i)
			value := gofakeit.Sentence(i + 1)
			kvInserted[key] = []byte(value)
			err := kvTXN.AppendKVTxn([]byte(key), []byte(value))
			assert.NoError(t, err, "error inserting txn")
		}
	})

	deletedKeys := make(map[string]struct{})
	t.Run("insert_delete_non_txn_kv", func(t *testing.T) {
		deleteKey := fmt.Sprintf("key_%d", 1)
		deletedKeys[deleteKey] = struct{}{}
		assert.NoError(t, engine.DeleteKV([]byte(deleteKey)), "error deleting key")
	})

	t.Run("replicate_combined_txn_delete", func(t *testing.T) {
		fromOffset := replicaEngine.CurrentOffset()
		reader, err := getReader(engine, fromOffset)
		assert.NoError(t, err, "error creating reader")
		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "error reading from reader")
			err = replicator.ApplyRecord(walEncoded, offset)
			assert.NoError(t, err, "error applying record to replicator")
		}

		assert.Equal(t, replicaEngine.CurrentOffset(), engine.CurrentOffset(), "offset of both the engine should be the same")
	})

	t.Run("replicate_validate_txn_not_commited", func(t *testing.T) {
		for k := range deletedKeys {
			_, err := replicaEngine.GetKV([]byte(k))
			assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "deleted key should error with Key Not Found")
		}
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("txn_key_%d", i)
			_, err := replicaEngine.GetKV([]byte(key))
			assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "non commited key should error with Key Not Found")
		}
	})

	t.Run("commit_txn_replicate_and_validate", func(t *testing.T) {
		assert.NoError(t, kvTXN.Commit(), "error committing txn")
		fromOffset := replicaEngine.CurrentOffset()
		reader, err := getReader(engine, fromOffset)
		assert.NoError(t, err, "error creating reader")
		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "error reading from reader")
			err = replicator.ApplyRecord(walEncoded, offset)
			assert.NoError(t, err, "error applying record to replicator")
		}

		assert.Equal(t, replicaEngine.CurrentOffset(), engine.CurrentOffset(), "offset of both the engine should be the same")
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("txn_key_%d", i)
			got, err := replicaEngine.GetKV([]byte(key))
			assert.NoError(t, err, "error reading from replica")
			assert.Equal(t, got, kvInserted[key], "invalid replicator value")
		}
	})

	t.Run("chunked_replication", func(t *testing.T) {
		chunkedTXN, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
		assert.NoError(t, err, "error creating txn")
		chunkedKey := gofakeit.UUID()
		var chunkedAppendedValue []byte
		for i := 0; i < 10; i++ {
			value := fmt.Sprintf("chunked_txn_value_%d", i)
			chunkedAppendedValue = append(chunkedAppendedValue, []byte(value)...)
			assert.NoError(t, chunkedTXN.AppendKVTxn([]byte(chunkedKey), []byte(value)), "error appending chunked")
		}
		assert.NoError(t, chunkedTXN.Commit(), "error committing chunked txn")

		fromOffset := replicaEngine.CurrentOffset()
		reader, err := getReader(engine, fromOffset)
		assert.NoError(t, err, "error creating reader")
		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "error reading from reader")
			err = replicator.ApplyRecord(walEncoded, offset)
			assert.NoError(t, err, "error applying record to replicator")
		}

		assert.Equal(t, replicaEngine.CurrentOffset(), engine.CurrentOffset(), "offset of both the engine should be the same")
		chunkedValue, err := replicaEngine.GetLOB([]byte(chunkedKey))
		assert.NoError(t, err, "error reading from chunked reader")
		assert.Equal(t, chunkedValue, chunkedAppendedValue, "invalid chunked value")
	})

	var txnRowKeys []string
	var txnRowValues []map[string][]byte

	t.Run("row_columns_insert_txn_replication", func(t *testing.T) {
		txn, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeRow)
		assert.NoError(t, err, "error creating txn")

		for i := 0; i < 10; i++ {
			rowKey := gofakeit.UUID()
			txnRowKeys = append(txnRowKeys, rowKey)
			entries := make(map[string][]byte)
			for k := 0; k < 10; k++ {
				key := gofakeit.Name()
				val := gofakeit.LetterN(uint(k + 1))
				entries[key] = []byte(val)
			}
			txnRowValues = append(txnRowValues, entries)
			err := txn.AppendColumnTxn([]byte(rowKey), entries)
			assert.NoError(t, err, "error appending row")
		}

		assert.NoError(t, txn.Commit(), "error committing txn")

		fromOffset := replicaEngine.CurrentOffset()
		reader, err := getReader(engine, fromOffset)
		assert.NoError(t, err, "error creating reader")
		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "error reading from reader")
			err = replicator.ApplyRecord(walEncoded, offset)
			assert.NoError(t, err, "error applying record to replicator")
		}

		assert.Equal(t, replicaEngine.CurrentOffset(), engine.CurrentOffset(), "offset of both the engine should be the same")

		for i, rowKey := range txnRowKeys {
			got, err := replicaEngine.GetRowColumns(rowKey, nil)
			assert.NoError(t, err, "error reading from replicator")
			assert.Equal(t, txnRowValues[i], got, "invalid replicator value")
		}

		assert.Equal(t, engine.OpsReceivedCount(), replicaEngine.OpsReceivedCount(), "ops received should be equal")
	})

	t.Run("parallel_txn", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			txn, err := engine.NewTxn(logrecord.LogOperationTypeDelete, logrecord.LogEntryTypeKV)
			if err != nil {
				log.Fatalf("error creating txn: %v", err)
			}

			keys := maps.Keys(kvInserted)
			next, stop := iter.Pull(keys)
			defer stop()

			for i := 0; i < 10; i++ {
				key, ok := next()
				if !ok {
					break
				}
				deletedKeys[key] = struct{}{}
				err := txn.AppendKVTxn([]byte(key), []byte{})
				if err != nil {
					log.Fatalf("error creating txn: %v", err)
				}
			}

			err = txn.Commit()
			if err != nil {
				log.Fatalf("error committing txn: %v", err)
			}
		}()

		go func() {
			defer wg.Done()
			txn, err := engine.NewTxn(logrecord.LogOperationTypeDelete, logrecord.LogEntryTypeRow)
			if err != nil {
				log.Fatalf("error creating txn: %v", err)
			}

			for i, key := range txnRowKeys {
				columns := maps.Keys(txnRowValues[i])
				next, stop := iter.Pull(columns)

				deleteColumn := make(map[string][]byte)
				for j := 0; j < 5; j++ {
					ck, ok := next()
					if !ok {
						stop()
						break
					}
					deleteColumn[ck] = nil
				}

				stop()
				err := txn.AppendColumnTxn([]byte(key), deleteColumn)
				if err != nil {
					log.Fatalf("error creating txn: %v", err)
				}
			}
			err = txn.Commit()
			if err != nil {
				log.Fatalf("error committing txn: %v", err)
			}
		}()

		wg.Wait()
	})

	t.Run("entire_row_delete", func(t *testing.T) {
		err := engine.BatchDeleteRows(rowKeys)
		assert.NoError(t, err, "error deleting rows")
	})

	t.Run("sync_validate", func(t *testing.T) {
		fromOffset := replicaEngine.CurrentOffset()
		reader, err := getReader(engine, fromOffset)
		assert.NoError(t, err, "error creating reader")
		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "error reading from reader")
			err = replicator.ApplyRecord(walEncoded, offset)
			assert.NoError(t, err, "error applying record to replicator")
		}

		assert.Equal(t, replicaEngine.CurrentOffset(), engine.CurrentOffset(), "offset of both the engine should be the same")

		for _, rowKey := range txnRowKeys {
			got, err := replicaEngine.GetRowColumns(rowKey, nil)
			assert.NoError(t, err, "error reading from replicator")
			assert.Equal(t, 5, len(got), "invalid replicator value")
		}

		for _, rowKey := range rowKeys {
			_, err := replicaEngine.GetRowColumns(string(rowKey), nil)
			assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "Deleted row should not return")
		}

		for key := range deletedKeys {
			_, err := replicaEngine.GetKV([]byte(key))
			assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "deleted key should not return")
		}
	})

}

func TestReplicaWALHandler_ApplyRecord_Invalid(t *testing.T) {
	baseDir := t.TempDir()
	replicatorNameSpace := "test_persistence_replicator"
	replicaDir := filepath.Join(baseDir, "replica")

	replicaEngine, err := dbkernel.NewStorageEngine(replicaDir, replicatorNameSpace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := replicaEngine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close replica: %v", err)
		}
	})

	replicator := dbkernel.NewReplicaWALHandler(replicaEngine)

	record := logcodec.LogRecord{
		LSN:             0,
		HLC:             0,
		CRC32Checksum:   0,
		OperationType:   0,
		TxnState:        0,
		EntryType:       0,
		TxnID:           nil,
		PrevTxnWalIndex: nil,
		Entries:         nil,
	}

	encoded := record.FBEncode(1024)
	err = replicator.ApplyRecord(encoded, dbkernel.Offset{Offset: 123})
	assert.ErrorIs(t, err, dbkernel.ErrInvalidLSN, "expected LSN Error")
	assert.Equal(t, uint64(0), replicaEngine.OpsReceivedCount(), "ops received should be equal")
	err = replicator.ApplyRecord(encoded, dbkernel.Offset{Offset: 0})
	assert.ErrorIs(t, err, dbkernel.ErrInvalidOffset, "expected Invalid Offset Error")
	assert.Equal(t, uint64(0), replicaEngine.OpsReceivedCount(), "ops received should be equal")

	record = logcodec.LogRecord{
		LSN:             1,
		HLC:             0,
		CRC32Checksum:   0,
		OperationType:   0,
		TxnState:        0,
		EntryType:       0,
		TxnID:           nil,
		PrevTxnWalIndex: nil,
		Entries:         nil,
	}

	encoded = record.FBEncode(1024)
	err = replicator.ApplyRecord(encoded, dbkernel.Offset{Offset: 123})
	assert.ErrorIs(t, err, dbkernel.ErrInvalidOffset, "expected Invalid Offset Error")
	assert.Equal(t, uint64(1), replicaEngine.OpsReceivedCount(), "ops received should be equal")
}

func getReader(en *dbkernel.Engine, lastOffset *dbkernel.Offset) (*dbkernel.Reader, error) {
	reader, err := en.NewReaderWithStart(lastOffset)
	if err != nil {
		return nil, err
	}

	if lastOffset != nil {
		// we consume the first record.
		_, _, err = reader.Next()
		if err != nil {
			return nil, err
		}
	}

	return reader, err
}

func TestReplicaWALHandler_ApplyRecords(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_batch_replication"
	replicatorNameSpace := "test_batch_replicator"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	replicaDir := filepath.Join(baseDir, "replica")

	replicaEngine, err := dbkernel.NewStorageEngine(replicaDir, replicatorNameSpace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := replicaEngine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close replica: %v", err)
		}
	})

	replicator := dbkernel.NewReplicaWALHandler(replicaEngine)

	kvInserted := make(map[string][]byte)

	t.Run("insert_kv_records", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("batch_key_%d", i)
			value := gofakeit.Sentence(i + 1)
			kvInserted[key] = []byte(value)
			err := engine.PutKV([]byte(key), []byte(value))
			assert.NoError(t, err)
		}
	})

	t.Run("batch_replicate_kv", func(t *testing.T) {
		reader, err := engine.NewReader()
		assert.NoError(t, err, "error creating reader")

		batchSize := 10
		var encodedBatch [][]byte
		var offsetBatch []dbkernel.Offset

		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				if len(encodedBatch) > 0 {
					err = replicator.ApplyRecords(encodedBatch, offsetBatch)
					assert.NoError(t, err, "error applying batch to replicator")
				}
				break
			}
			assert.NoError(t, err, "error reading from reader")

			encodedBatch = append(encodedBatch, walEncoded)
			offsetBatch = append(offsetBatch, offset)

			if len(encodedBatch) >= batchSize {
				err = replicator.ApplyRecords(encodedBatch, offsetBatch)
				assert.NoError(t, err, "error applying batch to replicator")
				encodedBatch = nil
				offsetBatch = nil
			}
		}
	})

	t.Run("validate_batch_replicated_kv", func(t *testing.T) {
		for k, v := range kvInserted {
			got, err := replicaEngine.GetKV([]byte(k))
			assert.NoError(t, err, "error reading from replicator")
			assert.Equal(t, v, got, "invalid replicator value for key %s", k)
		}
	})

	t.Run("error_mismatched_lengths", func(t *testing.T) {
		encodedBatch := [][]byte{[]byte("test1"), []byte("test2")}
		offsetBatch := []dbkernel.Offset{{SegmentID: 1, Offset: 100}}

		err := replicator.ApplyRecords(encodedBatch, offsetBatch)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch")
	})

	t.Run("error_empty_batch", func(t *testing.T) {
		err := replicator.ApplyRecords(nil, nil)
		assert.NoError(t, err, "empty batch should not error")
	})
}

func TestReplicaApplyRecordPersistsLogIndex(t *testing.T) {
	baseDir := t.TempDir()
	leaderDir := filepath.Join(baseDir, "leader")
	followerDir := filepath.Join(baseDir, "follower")
	leaderNamespace := "replica_log_index_leader"
	followerNamespace := "replica_log_index_follower"

	leader, err := dbkernel.NewStorageEngine(leaderDir, leaderNamespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		if leader != nil {
			_ = leader.Close(context.Background())
		}
	})

	follower, err := dbkernel.NewStorageEngine(followerDir, followerNamespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	handler := dbkernel.NewReplicaWALHandler(follower)

	require.NoError(t, leader.PutKV([]byte("key"), []byte("value")))

	reader, err := leader.NewReader()
	require.NoError(t, err)
	defer reader.Close()

	walEncoded, offset, err := reader.Next()
	require.NoError(t, err)
	encodedCopy := append([]byte(nil), walEncoded...)

	fbRecord := logrecord.GetRootAsLogRecord(encodedCopy, 0)
	lsn := fbRecord.Lsn()

	require.NoError(t, handler.ApplyRecord(encodedCopy, offset))
	require.NoError(t, follower.Close(context.Background()))
	follower = nil

	walDir := filepath.Join(followerDir, followerNamespace, "wal")
	walog, err := walfs.NewWALog(walDir, ".seg")
	require.NoError(t, err)
	defer walog.Close()

	requireSegmentFirstIndex(t, walog, lsn)

	segID, slot, err := walog.SegmentForIndex(lsn)
	require.NoError(t, err)
	indexEntries, err := walog.SegmentIndex(segID)
	require.NoError(t, err)
	require.Less(t, slot, len(indexEntries))

	data, _, err := walog.Segments()[segID].Read(indexEntries[slot].Offset)
	require.NoError(t, err)
	copyData := append([]byte(nil), data...)
	recordFB := logrecord.GetRootAsLogRecord(copyData, 0)
	decoded := logcodec.DeserializeFBRootLogRecord(recordFB)
	require.Equal(t, lsn, decoded.LSN)
}

func TestReplicaApplyRecordsPersistLogIndex(t *testing.T) {
	baseDir := t.TempDir()
	leaderDir := filepath.Join(baseDir, "leader_batch")
	followerDir := filepath.Join(baseDir, "follower_batch")
	leaderNamespace := "replica_batch_leader"
	followerNamespace := "replica_batch_follower"

	leader, err := dbkernel.NewStorageEngine(leaderDir, leaderNamespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		if leader != nil {
			_ = leader.Close(context.Background())
		}
	})

	follower, err := dbkernel.NewStorageEngine(followerDir, followerNamespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	handler := dbkernel.NewReplicaWALHandler(follower)

	const writeCount = 5
	for i := 0; i < writeCount; i++ {
		key := fmt.Sprintf("batch-log-index-%d", i)
		val := fmt.Sprintf("value-%d", i)
		require.NoError(t, leader.PutKV([]byte(key), []byte(val)))
	}

	reader, err := leader.NewReader()
	require.NoError(t, err)
	defer reader.Close()

	var encodedBatch [][]byte
	var offsetBatch []dbkernel.Offset
	var lsns []uint64

	for len(encodedBatch) < writeCount {
		walEncoded, offset, err := reader.Next()
		require.NoError(t, err)
		dataCopy := append([]byte(nil), walEncoded...)
		encodedBatch = append(encodedBatch, dataCopy)
		offsetBatch = append(offsetBatch, offset)
		fbRecord := logrecord.GetRootAsLogRecord(dataCopy, 0)
		lsns = append(lsns, fbRecord.Lsn())
	}

	require.NoError(t, handler.ApplyRecords(encodedBatch, offsetBatch))
	require.NoError(t, follower.Close(context.Background()))
	follower = nil

	walDir := filepath.Join(followerDir, followerNamespace, "wal")
	walog, err := walfs.NewWALog(walDir, ".seg")
	require.NoError(t, err)
	defer walog.Close()

	requireSegmentFirstIndex(t, walog, lsns[0])

	for _, lsn := range lsns {
		segID, slot, err := walog.SegmentForIndex(lsn)
		require.NoError(t, err)
		indexEntries, err := walog.SegmentIndex(segID)
		require.NoError(t, err)
		require.Less(t, slot, len(indexEntries))

		data, _, err := walog.Segments()[segID].Read(indexEntries[slot].Offset)
		require.NoError(t, err)
		recordCopy := append([]byte(nil), data...)
		fb := logrecord.GetRootAsLogRecord(recordCopy, 0)
		decoded := logcodec.DeserializeFBRootLogRecord(fb)
		require.Equal(t, lsn, decoded.LSN)
	}
}

func requireSegmentFirstIndex(t *testing.T, walog *walfs.WALog, expected uint64) {
	t.Helper()
	segments := walog.Segments()
	require.NotEmpty(t, segments, "wal has no segments")

	for segID, seg := range segments {
		if seg.GetEntryCount() == 0 {
			continue
		}
		first := seg.FirstLogIndex()
		require.NotZerof(t, first, "segment %d has entries but zero FirstLogIndex", segID)
		if first == expected {
			return
		}
	}

	t.Fatalf("no segment persisted FirstLogIndex %d", expected)
}

func TestReplicaWALHandler_RaftInternalRecords(t *testing.T) {
	baseDir := t.TempDir()
	replicatorNameSpace := "test_raft_internal_replicator"
	replicaDir := filepath.Join(baseDir, "replica")

	replicaEngine, err := dbkernel.NewStorageEngine(replicaDir, replicatorNameSpace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := replicaEngine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close replica: %v", err)
		}
	})

	replicator := dbkernel.NewReplicaWALHandler(replicaEngine)

	t.Run("raft_internal_record_applies_without_error", func(t *testing.T) {
		record := logcodec.LogRecord{
			LSN:           1,
			HLC:           uint64(dbkernel.HLCNow()),
			OperationType: logrecord.LogOperationTypeRaftInternal,
			EntryType:     logrecord.LogEntryTypeKV,
			TxnState:      logrecord.TransactionStateNone,
		}

		encoded := record.FBEncode(64)
		expectedOffset := dbkernel.Offset{
			SegmentID: 1,
			Offset:    int64(len(encoded)),
		}

		err := replicator.ApplyRecord(encoded, expectedOffset)
		require.NoError(t, err, "RaftInternal record should apply without error")
		assert.Equal(t, uint64(1), replicaEngine.OpsReceivedCount(),
			"OpsReceivedCount should be incremented for RaftInternal")
	})

	t.Run("raft_internal_does_not_write_data_to_store", func(t *testing.T) {
		_, err := replicaEngine.GetKV([]byte("any-key"))
		assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound,
			"RaftInternal should not write any KV data")
	})

	t.Run("raft_internal_via_leader_follower_pattern", func(t *testing.T) {
		leaderDir := filepath.Join(baseDir, "leader_raft_internal")
		followerDir := filepath.Join(baseDir, "follower_raft_internal")

		leader, err := dbkernel.NewStorageEngine(leaderDir, "leader", dbkernel.NewDefaultEngineConfig())
		require.NoError(t, err)
		defer leader.Close(context.Background())

		follower, err := dbkernel.NewStorageEngine(followerDir, "follower", dbkernel.NewDefaultEngineConfig())
		require.NoError(t, err)
		defer follower.Close(context.Background())

		followerHandler := dbkernel.NewReplicaWALHandler(follower)
		key1 := "leader_key_1"
		val1 := "leader_value_1"
		require.NoError(t, leader.PutKV([]byte(key1), []byte(val1)))

		key2 := "leader_key_2"
		val2 := "leader_value_2"
		require.NoError(t, leader.PutKV([]byte(key2), []byte(val2)))

		reader, err := leader.NewReader()
		require.NoError(t, err)
		defer reader.Close()

		recordCount := 0
		for {
			walEncoded, offset, err := reader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)

			err = followerHandler.ApplyRecord(walEncoded, offset)
			require.NoError(t, err)
			recordCount++
		}

		assert.Equal(t, 2, recordCount, "should have replicated 2 records")
		assert.Equal(t, leader.OpsReceivedCount(), follower.OpsReceivedCount(),
			"leader and follower should have same ops count")

		gotVal1, err := follower.GetKV([]byte(key1))
		require.NoError(t, err)
		assert.Equal(t, []byte(val1), gotVal1)

		gotVal2, err := follower.GetKV([]byte(key2))
		require.NoError(t, err)
		assert.Equal(t, []byte(val2), gotVal2)
	})

	t.Run("replica_wal_has_no_holes_with_raft_internal", func(t *testing.T) {
		leaderDir := filepath.Join(baseDir, "leader_no_holes")
		followerDir := filepath.Join(baseDir, "follower_no_holes")

		leader, err := dbkernel.NewStorageEngine(leaderDir, "leader_nh", dbkernel.NewDefaultEngineConfig())
		require.NoError(t, err)
		defer leader.Close(context.Background())

		follower, err := dbkernel.NewStorageEngine(followerDir, "follower_nh", dbkernel.NewDefaultEngineConfig())
		require.NoError(t, err)
		defer follower.Close(context.Background())

		followerHandler := dbkernel.NewReplicaWALHandler(follower)

		expectedLSNs := make(map[uint64]bool)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key_%d", i)
			val := fmt.Sprintf("value_%d", i)
			require.NoError(t, leader.PutKV([]byte(key), []byte(val)))
		}

		leaderReader, err := leader.NewReader()
		require.NoError(t, err)
		defer leaderReader.Close()

		for {
			walEncoded, offset, err := leaderReader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)

			fb := logrecord.GetRootAsLogRecord(walEncoded, 0)
			expectedLSNs[fb.Lsn()] = true

			err = followerHandler.ApplyRecord(walEncoded, offset)
			require.NoError(t, err)
		}

		assert.Equal(t, 10, len(expectedLSNs), "should have 10 unique LSNs")

		followerReader, err := follower.NewReader()
		require.NoError(t, err)
		defer followerReader.Close()

		var actualLSNs []uint64
		for {
			walEncoded, _, err := followerReader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)

			fb := logrecord.GetRootAsLogRecord(walEncoded, 0)
			actualLSNs = append(actualLSNs, fb.Lsn())
		}

		assert.Equal(t, len(expectedLSNs), len(actualLSNs),
			"follower WAL should have same number of records as leader")

		for i, lsn := range actualLSNs {
			expectedLSN := uint64(i + 1)
			assert.Equal(t, expectedLSN, lsn,
				"LSN at position %d should be %d, got %d (no holes allowed)", i, expectedLSN, lsn)
		}

		for _, lsn := range actualLSNs {
			assert.True(t, expectedLSNs[lsn], "LSN %d should be in expected set", lsn)
		}
	})

	t.Run("raft_internal_then_kv_maintains_lsn_sequence", func(t *testing.T) {
		reader, err := replicaEngine.NewReader()
		require.NoError(t, err)
		defer reader.Close()

		var lsns []uint64
		var opTypes []logrecord.LogOperationType
		for {
			walEncoded, _, err := reader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)

			fb := logrecord.GetRootAsLogRecord(walEncoded, 0)
			lsns = append(lsns, fb.Lsn())
			opTypes = append(opTypes, fb.OperationType())
		}

		require.GreaterOrEqual(t, len(lsns), 1, "should have at least 1 record")

		assert.Equal(t, uint64(1), lsns[0], "first record should have LSN 1")
		assert.Equal(t, logrecord.LogOperationTypeRaftInternal, opTypes[0],
			"first record should be RaftInternal")

		for i, lsn := range lsns {
			expectedLSN := uint64(i + 1)
			assert.Equal(t, expectedLSN, lsn,
				"LSN at position %d should be %d, got %d (no holes)", i, expectedLSN, lsn)
		}
	})
}
