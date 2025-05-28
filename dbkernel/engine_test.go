package dbkernel

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	"github.com/ankur-anand/unisondb/dbkernel/internal/memtable"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/hashicorp/go-metrics"
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

	assert.NoError(t, err, "NewStorageEngine should not error")
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err, "storage engine close should not error")
	})

	insertedKV := make(map[string]string)
	t.Run("persist_key_value", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(100)
			insertedKV[key] = value
			err := engine.persistKeyValue([][]byte{[]byte(key)}, [][]byte{[]byte(value)}, logrecord.LogOperationTypeInsert)
			assert.NoError(t, err, "persistKeyValue should not error")
			valueType, ok := engine.getEntryTypeForKeyFromMemTable([]byte(key))
			assert.True(t, ok, "getEntryTypeForKeyFromMemTable should return true")
			assert.Equal(t, valueType, logrecord.LogEntryTypeKV)
		}
	})

	t.Run("handle_mem_table_flush_no_sealed_table", func(t *testing.T) {
		engine.handleFlush(t.Context())
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

		result, err := engine.dataStore.RetrieveMetadata(internal.SysKeyBloomFilter)
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

		result, err = engine.dataStore.RetrieveMetadata(internal.SysKeyWalCheckPoint)
		assert.NoError(t, err, "RetrieveMetadata should not error")
		metadata := internal.UnmarshalMetadata(result)
		assert.Equal(t, uint64(100), metadata.RecordProcessed, "metadata.RecordProcessed should be 100")
		assert.Equal(t, *metadata.Pos, *engine.currentOffset.Load(), "metadata.offset should be equal to current offset")
	})

	t.Run("test_mem_table_write_with_rotate", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(1024)
			insertedKV[key] = value
			err := engine.persistKeyValue([][]byte{[]byte(key)}, [][]byte{[]byte(value)}, logrecord.LogOperationTypeInsert)
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

		result, err := engine.dataStore.RetrieveMetadata(internal.SysKeyWalCheckPoint)
		assert.NoError(t, err, "RetrieveMetadata should not error")
		metadata := internal.UnmarshalMetadata(result)
		assert.Equal(t, engine.opsFlushedCounter.Load(), metadata.RecordProcessed, "flushed counter should match")

	})
}

func TestArenaReplacement_Snapshot_And_Recover(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_arena_flush"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 200 * 1024
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
	batch, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
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

	for i := 0; i < 1; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyPrefix, i))
		valueRec, err := engine.Get(key)
		assert.NoError(t, err, "Get operation should not fail")
		assert.Equal(t, valueRec, value, "failed here as well")

	}
	f, err := os.CreateTemp("", "backup.bolt")
	assert.NoError(t, err)
	// flush everything so no race with db View and
	// OpsFlushed and pause flush.
	engine.fSyncStore()
	engine.pauseFlush()
	time.Sleep(1 * time.Second)
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

	var metadataBytes []byte
	db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("sys.kv.unison.db.wal.metadata.bucket"))
		assert.NotNil(t, bucket)
		val := bucket.Get(internal.SysKeyWalCheckPoint)
		assert.NotNil(t, val)
		metadataBytes = make([]byte, len(val))
		copy(metadataBytes, val)
		return nil
	})

	metadata := internal.UnmarshalMetadata(metadataBytes)

	assert.Equal(t, uint64(5001), engine.OpsReceivedCount())

	err = engine.Close(t.Context())
	assert.NoError(t, err, "Failed to close engine")

	engine, err = NewStorageEngine(baseDir, namespace, config)
	assert.NoError(t, err)
	assert.NotNil(t, engine)
	// 4000 keys, total boltdb keys, batch is never commited.
	assert.Equal(t, 4000, int(metadata.RecordProcessed)+engine.RecoveredWALCount())

	defer func() {
		err := engine.Close(t.Context())
		assert.NoError(t, err, "Failed to close engine")
	}()

	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyPrefix, i))
		retrievedValue, err := engine.Get(key)

		assert.NoError(t, err, "Get operation should succeed")
		assert.NotNil(t, retrievedValue, "Retrieved value should not be nil")
		assert.Equal(t, len(retrievedValue), valueSize, "Value length mismatch")
		assert.Equal(t, value, retrievedValue, "Retrieved value should match")

		valueType, ok := engine.getEntryType(key)
		assert.Equal(t, valueType, logrecord.LogEntryTypeKV, "")
		assert.True(t, ok, "getEntryTypeForKey should succeed")
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

	insertedKV := make(map[string]string)
	t.Run("persist_key_value", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(100)
			insertedKV[key] = value
			err := engine.persistKeyValue([][]byte{[]byte(key)}, [][]byte{[]byte(value)}, logrecord.LogOperationTypeInsert)
			assert.NoError(t, err, "persistKeyValue should not error")
		}
	})

	t.Run("engine_close", func(t *testing.T) {
		err := engine.close(t.Context())
		assert.NoError(t, err, "storage engine close should not error")
	})

	t.Run("restart_engine_and_close", func(t *testing.T) {
		engine, err = NewStorageEngine(dir, namespace, config)
		assert.NoError(t, err, "NewStorageEngine should not error")
		assert.Equal(t, 100, engine.RecoveredWALCount(), "recovered wal count should match")
		err := engine.close(t.Context())
		assert.NoError(t, err, "storage engine close should not error")
	})

	t.Run("restart_engine_and_close", func(t *testing.T) {
		engine, err = NewStorageEngine(dir, namespace, config)
		assert.NoError(t, err, "NewStorageEngine should not error")
		assert.Equal(t, 0, engine.RecoveredWALCount(), "recovered wal count should match")
		err := engine.close(t.Context())
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
		err := engine.close(context.Background())
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

				err := engine.PutColumnsForRow([]byte(rowKey), entries)
				assert.NoError(t, err, "PutColumnsForRow operation should succeed")
			}
		}
	})

	t.Run("get_rows_columns", func(t *testing.T) {
		for k, v := range rowsEntries {
			rowEntry, err := engine.GetRowColumns(k, nil)
			assert.NoError(t, err, "failed to build column map")
			assert.Equal(t, len(v), len(rowEntry), "unexpected number of column values")
			assert.Equal(t, v, rowEntry, "unexpected column values")
			valueType, ok := engine.getEntryTypeForKeyFromMemTable([]byte(k))
			assert.Equal(t, valueType, logrecord.LogEntryTypeRow, "")
			assert.True(t, ok, "getEntryTypeForKeyFromMemTable should succeed")
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

		err = engine.DeleteColumnsForRow([]byte(randomRow), deleteEntries)
		assert.NoError(t, err, "DeleteColumnsForRow operation should succeed")
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
		err = engine.PutColumnsForRow([]byte(randomRow), newEntries)
		assert.NoError(t, err, "PutColumnsForRow operation should succeed")
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
		err = engine.DeleteRow([]byte(randomRow))
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
		err := engine.close(context.Background())
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

				err := engine.PutColumnsForRow([]byte(rowKey), entries)
				assert.NoError(t, err, "PutColumnsForRow operation should succeed")
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

		err = engine.DeleteColumnsForRow([]byte(randomRow), deleteEntries)
		assert.NoError(t, err, "DeleteColumnsForRow operation should succeed")
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.NoError(t, err, "failed to build column map")
		assert.Equal(t, len(rowEntry), len(columnMap)-len(deleteEntries), "unexpected number of column values")
		assert.NotContains(t, rowEntry, deleteEntries, "unexpected column values")
	})

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
		err = engine.PutColumnsForRow([]byte(randomRow), newEntries)
		assert.NoError(t, err, "PutColumnsForRow operation should succeed")
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
		err = engine.DeleteRow([]byte(randomRow))
		assert.NoError(t, err, "DeleteRow operation should succeed")
		rowEntry, err := engine.GetRowColumns(randomRow, nil)
		assert.ErrorIs(t, err, ErrKeyNotFound, "failed to build column map")
		assert.Nil(t, rowEntry, "unexpected column values")
	})

}

func TestRecoveryAndCheckPoint(t *testing.T) {
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
	config.DBEngine = BoltDBEngine
	engine, err := NewStorageEngine(dir, namespace, config)
	assert.NoError(t, err, "NewStorageEngine should not error")
	engine.callback = callback

	kv := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(100)
		kv[key] = value
		err := engine.Put([]byte(key), []byte(value))
		assert.NoError(t, err, "Put operation should succeed")
	}

	// rotate and flush memTable.
	engine.rotateMemTable()

	select {
	case <-callbackSignal:
	case <-time.After(5 * time.Second):
		t.Errorf("timed out waiting for callback signal")
	}

	err = engine.close(context.Background())
	assert.NoError(t, err, "storage engine close should not error")
	engine, err = NewStorageEngine(dir, namespace, config)
	assert.NoError(t, err, "NewStorageEngine should not error")
	checkPoint, err := engine.GetWalCheckPoint()
	assert.NoError(t, err, "GetWalCheckPoint should not error")
	assert.Equal(t, checkPoint.Pos, engine.CurrentOffset())

	entryCount := 100
	for i := 0; i < 5; i++ {
		err = engine.close(context.Background())
		assert.NoError(t, err, "storage engine close should not error")
		engine, err = NewStorageEngine(dir, namespace, config)
		assert.NoError(t, err, "NewStorageEngine should not error")
		for i := 0; i < 10; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(100)
			kv[key] = value
			err := engine.Put([]byte(key), []byte(value))
			assert.NoError(t, err, "Put operation should succeed")
		}

		assert.Equal(t, entryCount, int(engine.OpsFlushedCount()), "expected ops flushed count")
		entryCount += 10
		assert.Equal(t, entryCount, int(engine.OpsReceivedCount()), "expected ops received count")
	}

	for k, v := range kv {
		value, err := engine.Get([]byte(k))
		assert.NoError(t, err, "Get operation should succeed")
		assert.Equal(t, v, string(value), "unexpected value for key")
	}

}

func Test_MisMatch_Entry_Type(t *testing.T) {
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
	config.DBEngine = BoltDBEngine
	engine, err := NewStorageEngine(dir, namespace, config)
	assert.NoError(t, err, "NewStorageEngine should not error")
	engine.callback = callback

	kv := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(100)
		kv[key] = value
		err := engine.Put([]byte(key), []byte(value))
		assert.NoError(t, err, "Put operation should succeed")
	}

	engine.rotateMemTableNoFlush()

	for k := range kv {
		entryType, ok := engine.getEntryType([]byte(k))
		assert.True(t, ok, "getEntryType operation should succeed")
		assert.Equal(t, entryType, logrecord.LogEntryTypeKV, "unexpected entry type")
	}

	rowsEntries := make(map[string]map[string][]byte)
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

			err := engine.PutColumnsForRow([]byte(rowKey), entries)
			assert.NoError(t, err, "PutColumnsForRow operation should succeed")
		}
	}

	engine.rotateMemTableNoFlush()
	for k := range rowsEntries {
		entryType, ok := engine.getEntryType([]byte(k))
		assert.True(t, ok, "getEntryType operation should succeed")
		assert.Equal(t, entryType, logrecord.LogEntryTypeRow, "unexpected entry type")
	}

	chunkedTxn, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
	assert.NoError(t, err, "NewTxn should not error")
	chunkedKey := gofakeit.UUID()
	for i := 0; i < 5; i++ {
		err := chunkedTxn.AppendKVTxn([]byte(chunkedKey), []byte(gofakeit.LetterN(10)))
		assert.NoError(t, err, "AppendTxn operation should succeed")
	}

	assert.NoError(t, chunkedTxn.Commit())

	valueType, ok := engine.getEntryType([]byte(chunkedKey))
	assert.True(t, ok, "getEntryType operation should succeed")
	assert.Equal(t, valueType, logrecord.LogEntryTypeChunked, "unexpected entry type")
	engine.rotateMemTableNoFlush()
	valueType, ok = engine.getEntryType([]byte(chunkedKey))
	assert.True(t, ok, "getEntryType operation should succeed")
	assert.Equal(t, valueType, logrecord.LogEntryTypeChunked, "unexpected entry type")

	for i := uint64(0); i < 10; i++ {
		_, ok := engine.getEntryType([]byte(gofakeit.Name()))
		assert.False(t, ok, "getEntryType operation should return !ok")
	}

	for rk := range rowsEntries {
		err := engine.Put([]byte(rk), []byte(gofakeit.Name()))
		assert.ErrorIs(t, err, ErrMisMatchKeyType)
	}

	for k := range kv {
		err := engine.PutColumnsForRow([]byte(k), rowsEntries[k])
		assert.ErrorIs(t, err, ErrMisMatchKeyType)
	}
}

func Test_ASyncFSync_Coalescing(t *testing.T) {
	dir := t.TempDir()
	namespace := "testnamespace"
	fsyncCall := atomic.Int64{}
	callback := func() {
		fsyncCall.Add(1)
	}
	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20

	initMonotonic(t.Context())
	label := []metrics.Label{{Name: "namespace", Value: namespace}}
	signal := make(chan struct{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	engine := &Engine{
		namespace:       namespace,
		config:          config,
		wg:              &sync.WaitGroup{},
		metricsLabel:    label,
		flushReqSignal:  signal,
		pendingMetadata: &pendingMetadata{pendingMetadataWrites: make([]*flushedMetadata, 0)},
		ctx:             ctx,
		cancel:          cancel,
		callback:        func() {},
		fsyncReqSignal:  make(chan struct{}, 1),
	}

	err := engine.initStorage(dir, namespace, config)
	assert.NoError(t, err, "NewStorageEngine should not error")

	engine.appendNotify = make(chan struct{})

	engine.callback = callback

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.asyncMemTableFlusher(ctx)
	}()

	for i := 0; i < 100; i++ {
		engine.pendingMetadata.queueMetadata(&flushedMetadata{
			metadata: &internal.Metadata{
				RecordProcessed: uint64(100 + i),
				Pos: &Offset{
					SegmentID: 0,
					Offset:    int64(1 + i),
				},
			},
		})

		engine.fsyncReqSignal <- struct{}{}
	}

	assert.Eventually(t, func() bool {
		engine.pendingMetadata.mu.Lock()
		defer engine.pendingMetadata.mu.Unlock()
		return len(engine.pendingMetadata.pendingMetadataWrites) == 0
	}, 2*time.Second, 30*time.Millisecond)

	cancel()
	wg.Wait()
	assert.Less(t, fsyncCall.Load(), int64(100), "Coalescing should have happened")
	assert.Greater(t, fsyncCall.Load(), int64(1), "Coalescing should have happened")
	metadata, err := engine.dataStore.RetrieveMetadata(internal.SysKeyWalCheckPoint)
	assert.NoError(t, err, "RetrieveMetadata should not error")
	um := internal.UnmarshalMetadata(metadata)
	assert.Equal(t, uint64(199), um.RecordProcessed, "record processed should be 199")
	assert.Equal(t, int64(100), um.Pos.Offset, "block number should be 100")
}

type mockWalSyncer struct {
	syncCount atomic.Int64
	cancel    context.CancelFunc
}

func (m *mockWalSyncer) Sync() error {
	m.syncCount.Add(1)
	m.cancel()
	return nil
}

func Test_WalSyncer_Sync(t *testing.T) {
	dir := t.TempDir()
	namespace := "testnamespace"
	fsyncCall := atomic.Int64{}
	callback := func() {
		fsyncCall.Add(1)
	}
	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20

	initMonotonic(t.Context())
	label := []metrics.Label{{Name: "namespace", Value: namespace}}
	signal := make(chan struct{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	engine := &Engine{
		namespace:       namespace,
		config:          config,
		wg:              &sync.WaitGroup{},
		metricsLabel:    label,
		flushReqSignal:  signal,
		pendingMetadata: &pendingMetadata{pendingMetadataWrites: make([]*flushedMetadata, 0)},
		ctx:             ctx,
		cancel:          cancel,
		callback:        func() {},
		fsyncReqSignal:  make(chan struct{}, 1),
	}

	err := engine.initStorage(dir, namespace, config)
	assert.NoError(t, err, "NewStorageEngine should not error")

	engine.appendNotify = make(chan struct{})

	engine.callback = callback
	syncer := &mockWalSyncer{cancel: cancel}
	engine.walSyncer = syncer
	engine.syncWalAtInterval(ctx)
	engine.wg.Wait()

	assert.Equal(t, int64(1), syncer.syncCount.Load(), "WalSyncer should have happened")
}

type mockedTree struct {
	fsyncCall atomic.Int64
}

func (m *mockedTree) BatchPut(keys, values [][]byte) error {
	return nil
}

func (m *mockedTree) BatchDelete(keys [][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) BatchPutRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) BatchDeleteRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) BatchDeleteRows(rowKeys [][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) Commit() error {
	return nil
}

func (m *mockedTree) Stats() kvdrivers.TxnStats {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) Set(key []byte, value []byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) SetMany(keys [][]byte, values [][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) SetChunks(key []byte, chunks [][]byte, checksum uint32) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) Delete(key []byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) DeleteMany(keys [][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) SetManyRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) DeleteManyRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) DeleteEntireRows(rowKeys [][]byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) StoreMetadata(key []byte, value []byte) error {
	return nil
}

func (m *mockedTree) FSync() error {
	m.fsyncCall.Add(1)
	return nil
}

func (m *mockedTree) Get(key []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) GetRowColumns(rowKey []byte, filter func([]byte) bool) (map[string][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) Snapshot(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) RetrieveMetadata(key []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) GetValueType(key []byte) (kvdrivers.ValueEntryType, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockedTree) Close() error {
	//TODO implement me
	panic("implement me")
}

func TestBtreeSyncInterval(t *testing.T) {

	namespace := "testnamespace"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20

	initMonotonic(t.Context())
	label := []metrics.Label{{Name: "namespace", Value: namespace}}
	signal := make(chan struct{}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	mt := &mockedTree{}

	engine := &Engine{
		namespace:                 namespace,
		config:                    config,
		wg:                        &sync.WaitGroup{},
		metricsLabel:              label,
		flushReqSignal:            signal,
		pendingMetadata:           &pendingMetadata{pendingMetadataWrites: make([]*flushedMetadata, 0)},
		ctx:                       ctx,
		cancel:                    cancel,
		callback:                  func() {},
		fsyncReqSignal:            make(chan struct{}, 1),
		btreeFlushInterval:        10 * time.Millisecond,
		btreeFlushIntervalEnabled: true,
		dataStore:                 mt,
		bloom:                     bloom.NewWithEstimates(1_000_000, 0.0001),
		sealedMemTables:           make([]*memtable.MemTable, 0),
	}

	for i := 0; i < 100; i++ {
		engine.pendingMetadata.queueMetadata(&flushedMetadata{
			metadata: &internal.Metadata{
				RecordProcessed: uint64(100 + i),
				Pos: &Offset{
					SegmentID: 0,
					Offset:    int64(1 + i),
				},
			},
		})
	}

	engine.fsyncBtreeAtInterval(ctx)
	engine.wg.Wait()
	assert.Equal(t, int64(1), mt.fsyncCall.Load())
	mmTable := memtable.NewMemTable(1<<20, nil, namespace, func(maxBatchSize int) internal.TxnBatcher {
		return mt
	})
	err := mmTable.Put([]byte("test_key"), getValueStruct(internal.LogOperationInsert, internal.EntryTypeKV, []byte("hi")))
	assert.NoError(t, err)
	engine.sealedMemTables = append(engine.sealedMemTables, mmTable)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	engine.asyncMemTableFlusher(ctx)
	engine.flushReqSignal <- struct{}{}
	engine.wg.Wait()
	assert.Equal(t, int64(1), mt.fsyncCall.Load())
}

func TestBtreeSyncInterval_Engine(t *testing.T) {
	dir := t.TempDir()
	namespace := "testnamespace"
	callbackSignal := make(chan struct{}, 2)
	callback := func() {
		close(callbackSignal)
	}
	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	config.DBEngine = BoltDBEngine
	// this should default to least 1 second
	config.BTreeFlushInterval = 1 * time.Second
	engine, err := NewStorageEngine(dir, namespace, config)
	assert.NoError(t, err, "NewStorageEngine should not error")
	engine.callback = callback

	kv := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(100)
		kv[key] = value
		err := engine.Put([]byte(key), []byte(value))
		assert.NoError(t, err, "Put operation should succeed")
	}
	engine.rotateMemTable()

	select {
	case <-callbackSignal:
	case <-time.After(5 * time.Second):
		t.Errorf("callback timeout waiting for timer fsync")
	}
}

func TestNotificationCoalesce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	engine := &Engine{
		ctx:          ctx,
		notifierMu:   sync.RWMutex{},
		appendNotify: make(chan struct{}),
		coalesceFlag: atomic.Bool{},
		config: &EngineConfig{
			WriteNotifyCoalescing: WriteNotifyCoalescingConfig{
				Enabled:  true,
				Duration: 10 * time.Millisecond,
			},
		},
	}

	notChan := make(chan struct{})
	engine.notifierMu.Lock()
	engine.appendNotify = notChan
	engine.notifierMu.Unlock()
	dur := 10 * time.Millisecond
	engine.coalesceDuration = dur

	engine.notifyAppend()
	start := time.Now()
	select {
	case <-notChan:
		elapsed := time.Since(start)
		if elapsed < dur {
			t.Errorf("Notification arrived too early: %v", elapsed)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Notification did not arrive within expected time")
	}

	notChan = make(chan struct{})
	engine.notifierMu.Lock()
	engine.appendNotify = notChan
	engine.notifierMu.Unlock()
	engine.coalesceFlag.Store(false)

	engine.notifyAppend()
	// should not block.
	engine.notifyAppend()
	engine.notifyAppend()

	start = time.Now()
	select {
	case <-notChan:
		elapsed := time.Since(start)
		if elapsed < dur {
			t.Errorf("Notification arrived too early: %v", elapsed)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Notification did not arrive within expected time")
	}

	notChan = make(chan struct{})
	engine.notifierMu.Lock()
	engine.appendNotify = notChan
	engine.notifierMu.Unlock()
	engine.config.WriteNotifyCoalescing.Enabled = false
	engine.notifyAppend()

	select {
	case <-notChan:
	case <-time.After(10 * time.Millisecond):
		t.Errorf("Immediate notification did not occur when coalescing was disabled")
	}
}
