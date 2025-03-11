package dbengine

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"testing"
	"time"

	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestStorageEngine_Suite(t *testing.T) {
	dir := t.TempDir()
	namespace := "testnamespace"
	callbackSignal := make(chan struct{}, 1)
	callback := func() {
		select {
		case callbackSignal <- struct{}{}:
		default:
		}
	}
	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	engine.callback = callback

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assert.NoError(t, err, "NewStorageEngine should not error")
	t.Cleanup(func() {
		err := engine.close(ctx)
		assert.NoError(t, err, "storage engine close should not error")
	})

	insertedKV := make(map[string]string)
	t.Run("persist_key_value", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(100)
			insertedKV[key] = value
			err := engine.persistKeyValue([]byte(key), []byte(value), walrecord.LogOperationInsert)
			assert.NoError(t, err, "persistKeyValue should not error")
		}
	})

	t.Run("handle_mem_table_flush_no_sealed_table", func(t *testing.T) {
		engine.handleFlush(ctx)
		select {
		case <-callbackSignal:
			t.Errorf("should not have received callback signal")
		case <-time.After(1 * time.Second):
		}
	})

	t.Run("handle_mem_table_flush", func(t *testing.T) {
		engine.rotateMemTable()
		select {
		case <-callbackSignal:
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for callback signal")
		}
	})

	t.Run("check_store_after_flush", func(t *testing.T) {
		assert.Equal(t, uint64(100), engine.opsFlushedCounter.Load(), "expected opsFlushed counter to be 100")
		assert.Equal(t, uint64(100), engine.writeSeenCounter.Load(), "expected writeSeenCounter counter to be 100")

		result, err := engine.dataStore.RetrieveMetadata(sysKeyBloomFilter)
		assert.NoError(t, err, "RetrieveMetadata should not error for bloom filter after flush")
		// Deserialize Bloom Filter
		buf := bytes.NewReader(result)
		bloomFilter := bloom.NewWithEstimates(1_000_000, 0.0001)
		_, err = bloomFilter.ReadFrom(buf)
		assert.NoError(t, err, "bloom filter should not error")

		// both the bloom should have the presence of value.
		for k := range insertedKV {
			assert.True(t, bloomFilter.Test([]byte(k)))
			assert.True(t, engine.bloom.Test([]byte(k)))
		}

		result, err = engine.dataStore.RetrieveMetadata(sysKeyWalCheckPoint)
		assert.NoError(t, err, "RetrieveMetadata should not error")
		metadata := UnmarshalMetadata(result)
		assert.Equal(t, uint64(100), metadata.RecordProcessed, "metadata.RecordProcessed should be 100")
		assert.Equal(t, *metadata.Pos, *engine.currentOffset.Load(), "metadata.offset should be equal to current offset")
	})

	t.Run("test_mem_table_write_with_rotate", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(1024)
			insertedKV[key] = value
			err := engine.persistKeyValue([]byte(key), []byte(value), walrecord.LogOperationInsert)
			assert.NoError(t, err, "persistKeyValue should not error")
		}

		select {
		case <-callbackSignal:
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for callback signal for rotation")
		}

		assert.Equal(t, uint64(1100), engine.writeSeenCounter.Load(), "expected writeSeenCounter counter to be 100")
		// both the bloom should have the presence of value.
		for k := range insertedKV {
			assert.True(t, engine.bloom.Test([]byte(k)))
		}

		result, err := engine.dataStore.RetrieveMetadata(sysKeyWalCheckPoint)
		assert.NoError(t, err, "RetrieveMetadata should not error")
		metadata := UnmarshalMetadata(result)
		assert.Equal(t, engine.opsFlushedCounter.Load(), metadata.RecordProcessed, "flushed counter should match")

	})
}

func TestArenaReplacement_Snapshot_And_Recover(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_arena_flush"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 200 * 1024
	config.ValueThreshold = 50
	config.DBEngine = BoltDBEngine
	config.BtreeConfig.Namespace = namespace

	engine, err := NewStorageEngine(baseDir, namespace, config)
	assert.NoError(t, err)

	signal := make(chan struct{}, 10)
	engine.callback = func() {
		signal <- struct{}{}
	}
	assert.NoError(t, err)
	valueSize := 1024 // 1KB per value (approx.)

	// create a batch records.
	batchKey := []byte(gofakeit.UUID())

	// open a batch writer:
	batch, err := engine.NewTxn(walrecord.LogOperationInsert, walrecord.EntryTypeChunked)
	assert.NoError(t, err, "NewBatch operation should succeed")
	assert.NotNil(t, batch, "NewBatch operation should succeed")

	var batchValues []string
	fullValue := new(bytes.Buffer)
	var checksum uint32

	for i := 0; i < 1000; i++ {
		value := gofakeit.LetterN(uint(valueSize))
		batchValues = append(batchValues, value)
		fullValue.Write([]byte(batchValues[i]))
		checksum = crc32.Update(checksum, crc32.IEEETable, []byte(value))
		err := batch.AppendKVTxn(batchKey, []byte(value))
		assert.NoError(t, err, "NewBatch operation should succeed")
	}

	keyPrefix := "flush_test_key_"

	value := []byte(gofakeit.LetterN(uint(valueSize)))
	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyPrefix, i))

		err := engine.Put(key, value)
		assert.NoError(t, err, "Put operation should not fail")

		if i%20 == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Errorf("Timed out waiting for flush")
	}

	f, err := os.CreateTemp("", "backup.bolt")
	assert.NoError(t, err)
	_, err = engine.BtreeSnapshot(f)
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
	name := f.Name()
	f.Close()

	// Open BoltDB
	db, err := bbolt.Open(name, 0600, nil)
	assert.NoError(t, err)
	defer db.Close()
	keysCount := 0
	db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(namespace))
		assert.NotNil(t, bucket)
		bucket.ForEach(func(k, v []byte) error {
			keysCount++
			return nil
		})
		return nil
	})
	err = engine.Close(context.Background())
	assert.NoError(t, err, "Failed to close engine")

	engine, err = NewStorageEngine(baseDir, namespace, config)
	assert.NoError(t, err)
	assert.NotNil(t, engine)
	// 4000 keys, total boltdb keys, batch is never commited.
	assert.Equal(t, 4000, keysCount+engine.RecoveredWALCount())

	defer func() {
		err := engine.Close(context.Background())
		assert.NoError(t, err, "Failed to close engine")
	}()

	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyPrefix, i))
		retrievedValue, err := engine.Get(key)

		assert.NoError(t, err, "Get operation should succeed")
		assert.NotNil(t, retrievedValue, "Retrieved value should not be nil")
		assert.Equal(t, len(retrievedValue), valueSize, "Value length mismatch")
	}

	// 4000 ops, for keys, > As Batch is not Commited, (1 batch start + (not 1 batch commit.) not included)
	assert.Equal(t, uint64(4000), engine.OpsReceivedCount())

	value, err = engine.Get(batchKey)
	assert.ErrorIs(t, err, ErrKeyNotFound, "Get operation should not succeed")
	assert.Nil(t, value, "Get value should not be nil")
}

func TestEngine_RecoveredWalShouldNotRecoverAgain(t *testing.T) {
	dir := t.TempDir()
	namespace := "testnamespace"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 30
	engine, err := NewStorageEngine(dir, namespace, config)
	assert.NoError(t, err, "NewStorage should not error")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	insertedKV := make(map[string]string)
	t.Run("persist_key_value", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(100)
			insertedKV[key] = value
			err := engine.persistKeyValue([]byte(key), []byte(value), walrecord.LogOperationInsert)
			assert.NoError(t, err, "persistKeyValue should not error")
		}
	})

	t.Run("engine_close", func(t *testing.T) {
		err := engine.close(ctx)
		assert.NoError(t, err, "storage engine close should not error")
	})

	t.Run("restart_engine_and_close", func(t *testing.T) {
		engine, err = NewStorageEngine(dir, namespace, config)
		assert.NoError(t, err, "NewStorageEngine should not error")
		assert.Equal(t, 100, engine.RecoveredWALCount(), "recovered wal count should match")
		err := engine.close(ctx)
		assert.NoError(t, err, "storage engine close should not error")
	})

	t.Run("restart_engine_and_close", func(t *testing.T) {
		engine, err = NewStorageEngine(dir, namespace, config)
		assert.NoError(t, err, "NewStorageEngine should not error")
		assert.Equal(t, 0, engine.RecoveredWALCount(), "recovered wal count should match")
		err := engine.close(ctx)
		assert.NoError(t, err, "storage engine close should not error")
	})
}

func TestEngine_GetRowColumns_WithMemTableRotateNoFlush(t *testing.T) {
	dir := t.TempDir()
	namespace := "testnamespace"
	callbackSignal := make(chan struct{}, 1)
	callback := func() {
		select {
		case callbackSignal <- struct{}{}:
		default:
		}
	}
	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	assert.NoError(t, err, "NewStorageEngine should not error")
	engine.callback = callback
	t.Cleanup(func() {
		err := engine.close(t.Context())
		assert.NoError(t, err, "storage engine close should not error")
	})

	rowsEntries := make(map[string]map[string][]byte)
	t.Run("put_row_columns", func(t *testing.T) {
		for i := uint64(0); i < 10; i++ {
			rowKey := gofakeit.UUID()

			if rowsEntries[rowKey] == nil {
				rowsEntries[rowKey] = make(map[string][]byte)
			}

			// for each row Key generate 5 ops
			for j := 0; j < 5; j++ {

				entries := make(map[string][]byte)
				for k := 0; k < 10; k++ {
					key := gofakeit.Name()
					val := gofakeit.LetterN(uint(i + 1))
					rowsEntries[rowKey][key] = []byte(val)
					entries[key] = []byte(val)
				}

				err := engine.SetColumnsInRow(rowKey, entries)
				assert.NoError(t, err, "SetColumnsInRow operation should succeed")
			}
		}
	})

	t.Run("get_rows_columns", func(t *testing.T) {
		for k, v := range rowsEntries {
			rowEntry, err := engine.GetRowColumns(k, nil)
			assert.NoError(t, err, "failed to build column map")
			assert.Equal(t, len(v), len(rowEntry), "unexpected number of column values")
			assert.Equal(t, v, rowEntry, "unexpected column values")
		}
	})

	randomRow := gofakeit.RandomMapKey(rowsEntries).(string)
	columnMap := rowsEntries[randomRow]
	deleteEntries := make(map[string][]byte)

	t.Run("delete_row_columns", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			key := gofakeit.RandomMapKey(columnMap).(string)
			deleteEntries[key] = nil
		}

		err = engine.DeleteColumnsFromRow(randomRow, deleteEntries)
		assert.NoError(t, err, "DeleteColumnsFromRow operation should succeed")
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.NoError(t, err, "failed to build column map")
		assert.Equal(t, len(rowEntry), len(columnMap)-len(deleteEntries), "unexpected number of column values")
		assert.NotContains(t, rowEntry, deleteEntries, "unexpected column values")
	})

	t.Run("handle_mem_table_flush", func(t *testing.T) {
		engine.rotateMemTableNoFlush()
		select {
		case <-callbackSignal:
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for callback signal")
		}
	})

	t.Run("get_rows_columns_after_flush", func(t *testing.T) {
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.NoError(t, err, "failed to build column map")
		assert.NotEqual(t, len(rowEntry), 0, "unexpected number of column values")
		assert.Equal(t, len(columnMap), len(rowEntry)+len(deleteEntries), "unexpected number of column values")
		//assert.Equal(t, v, rowEntry, "unexpected column values")
	})

	newEntries := make(map[string][]byte)
	for k := range deleteEntries {
		newEntries[k] = []byte(gofakeit.Name())
	}

	t.Run("update_deleted_values", func(t *testing.T) {
		err = engine.SetColumnsInRow(randomRow, newEntries)
		assert.NoError(t, err, "SetColumnsInRow operation should succeed")
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.NoError(t, err, "failed to build column map")
		assert.Equal(t, len(rowEntry), len(columnMap), "unexpected number of column values")
		for k, v := range newEntries {
			assert.Equal(t, v, rowEntry[k], "unexpected column values")
		}
	})

	t.Run("predicate_func_check", func(t *testing.T) {
		predicate := func(key string) bool {
			if _, ok := newEntries[key]; ok {
				return true
			}
			return false
		}
		rowEntry, err := engine.GetRowColumns(randomRow, predicate)
		assert.NoError(t, err, "failed to build column map")
		assert.Equal(t, len(rowEntry), len(newEntries), "unexpected number of column values")
	})

	t.Run("entire_row_delete", func(t *testing.T) {
		// delete the entire row.
		err = engine.DeleteRow(randomRow)
		assert.NoError(t, err, "DeleteRow operation should succeed")
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.ErrorIs(t, err, ErrKeyNotFound, "failed to build column map")
		assert.Nil(t, rowEntry, "unexpected column values")
	})

}

func TestEngine_GetRowColumns_WithMemTableRotate(t *testing.T) {
	dir := t.TempDir()
	namespace := "testnamespace"
	callbackSignal := make(chan struct{}, 1)
	callback := func() {
		select {
		case callbackSignal <- struct{}{}:
		default:
		}
	}
	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	assert.NoError(t, err, "NewStorageEngine should not error")
	engine.callback = callback
	t.Cleanup(func() {
		err := engine.close(t.Context())
		assert.NoError(t, err, "storage engine close should not error")
	})

	rowsEntries := make(map[string]map[string][]byte)
	t.Run("put_row_columns", func(t *testing.T) {
		for i := uint64(0); i < 10; i++ {
			rowKey := gofakeit.UUID()

			if rowsEntries[rowKey] == nil {
				rowsEntries[rowKey] = make(map[string][]byte)
			}

			// for each row Key generate 5 ops
			for j := 0; j < 5; j++ {

				entries := make(map[string][]byte)
				for k := 0; k < 10; k++ {
					key := gofakeit.Name()
					val := gofakeit.LetterN(uint(i + 1))
					rowsEntries[rowKey][key] = []byte(val)
					entries[key] = []byte(val)
				}

				err := engine.SetColumnsInRow(rowKey, entries)
				assert.NoError(t, err, "SetColumnsInRow operation should succeed")
			}
		}
	})

	t.Run("get_rows_columns", func(t *testing.T) {
		for k, v := range rowsEntries {
			rowEntry, err := engine.GetRowColumns(k, nil)
			assert.NoError(t, err, "failed to build column map")
			assert.Equal(t, len(v), len(rowEntry), "unexpected number of column values")
			assert.Equal(t, v, rowEntry, "unexpected column values")
		}
	})

	t.Run("handle_mem_table_flush", func(t *testing.T) {
		engine.rotateMemTable()
		select {
		case <-callbackSignal:
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for callback signal")
		}
	})

	t.Run("get_rows_columns__from_db_after_flush", func(t *testing.T) {
		for k, v := range rowsEntries {
			rowEntry, err := engine.dataStore.GetRowColumns([]byte(k), nil)
			assert.NoError(t, err, "failed to build column map")
			assert.Equal(t, len(v), len(rowEntry), "unexpected number of column values")
			assert.Equal(t, v, rowEntry, "unexpected column values")
		}
	})

	randomRow := gofakeit.RandomMapKey(rowsEntries).(string)
	columnMap := rowsEntries[randomRow]
	deleteEntries := make(map[string][]byte)

	t.Run("delete_row_columns", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			key := gofakeit.RandomMapKey(columnMap).(string)
			deleteEntries[key] = nil
		}

		err = engine.DeleteColumnsFromRow(randomRow, deleteEntries)
		assert.NoError(t, err, "DeleteColumnsFromRow operation should succeed")
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.NoError(t, err, "failed to build column map")
		assert.Equal(t, len(rowEntry), len(columnMap)-len(deleteEntries), "unexpected number of column values")
		assert.NotContains(t, rowEntry, deleteEntries, "unexpected column values")
	})

	fmt.Println("entries", len(deleteEntries), "entries", len(rowsEntries))
	t.Run("handle_mem_table_flush", func(t *testing.T) {
		engine.rotateMemTable()
		select {
		case <-callbackSignal:
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for callback signal")
		}
	})

	t.Run("get_rows_columns_after_flush", func(t *testing.T) {
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.NoError(t, err, "failed to build column map")
		assert.NotEqual(t, len(rowEntry), 0, "unexpected number of column values")
		assert.Equal(t, len(columnMap), len(rowEntry)+len(deleteEntries), "unexpected number of column values")
	})

	newEntries := make(map[string][]byte)
	for k := range deleteEntries {
		newEntries[k] = []byte(gofakeit.Name())
	}

	t.Run("update_deleted_values", func(t *testing.T) {
		err = engine.SetColumnsInRow(randomRow, newEntries)
		assert.NoError(t, err, "SetColumnsInRow operation should succeed")
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.NoError(t, err, "failed to build column map")
		assert.Equal(t, len(rowEntry), len(columnMap), "unexpected number of column values")
		for k, v := range newEntries {
			assert.Equal(t, v, rowEntry[k], "unexpected column values")
		}
	})

	t.Run("predicate_func_check", func(t *testing.T) {
		predicate := func(key string) bool {
			if _, ok := newEntries[key]; ok {
				return true
			}
			return false
		}
		rowEntry, err := engine.GetRowColumns(randomRow, predicate)
		assert.NoError(t, err, "failed to build column map")
		assert.Equal(t, len(rowEntry), len(newEntries), "unexpected number of column values")
	})

	t.Run("entire_row_delete", func(t *testing.T) {
		// delete the entire row.
		err = engine.DeleteRow(randomRow)
		assert.NoError(t, err, "DeleteRow operation should succeed")
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.ErrorIs(t, err, ErrKeyNotFound, "failed to build column map")
		assert.Nil(t, rowEntry, "unexpected column values")
	})

}
