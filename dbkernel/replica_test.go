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
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
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
			err := engine.Put([]byte(key), []byte(value))
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
			err = replicator.ApplyRecord(walEncoded, offset.Encode())
			assert.NoError(t, err, "error applying record to replicator")
		}
	})

	t.Run("validate_replicate_tx_none_kv", func(t *testing.T) {
		for k, v := range kvInserted {
			got, err := replicaEngine.Get([]byte(k))
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
			err = replicator.ApplyRecord(walEncoded, offset.Encode())
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
			err = replicator.ApplyRecord(walEncoded, offset.Encode())
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
		assert.NoError(t, engine.Delete([]byte(deleteKey)), "error deleting key")
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
			err = replicator.ApplyRecord(walEncoded, offset.Encode())
			assert.NoError(t, err, "error applying record to replicator")
		}

		assert.Equal(t, replicaEngine.CurrentOffset(), engine.CurrentOffset(), "offset of both the engine should be the same")
	})

	t.Run("replicate_validate_txn_not_commited", func(t *testing.T) {
		for k := range deletedKeys {
			_, err := replicaEngine.Get([]byte(k))
			assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "deleted key should error with Key Not Found")
		}
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("txn_key_%d", i)
			_, err := replicaEngine.Get([]byte(key))
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
			err = replicator.ApplyRecord(walEncoded, offset.Encode())
			assert.NoError(t, err, "error applying record to replicator")
		}

		assert.Equal(t, replicaEngine.CurrentOffset(), engine.CurrentOffset(), "offset of both the engine should be the same")
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("txn_key_%d", i)
			got, err := replicaEngine.Get([]byte(key))
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
			err = replicator.ApplyRecord(walEncoded, offset.Encode())
			assert.NoError(t, err, "error applying record to replicator")
		}

		assert.Equal(t, replicaEngine.CurrentOffset(), engine.CurrentOffset(), "offset of both the engine should be the same")
		chunkedValue, err := replicaEngine.Get([]byte(chunkedKey))
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
			err = replicator.ApplyRecord(walEncoded, offset.Encode())
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
			err = replicator.ApplyRecord(walEncoded, offset.Encode())
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
			_, err := replicaEngine.Get([]byte(key))
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
	err = replicator.ApplyRecord(encoded, []byte{123})
	assert.ErrorIs(t, err, dbkernel.ErrInvalidLSN, "expected LSN Error")
	assert.Equal(t, uint64(0), replicaEngine.OpsReceivedCount(), "ops received should be equal")
	err = replicator.ApplyRecord(encoded, nil)
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
	err = replicator.ApplyRecord(encoded, []byte{123})
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
