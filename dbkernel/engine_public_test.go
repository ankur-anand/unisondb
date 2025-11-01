package dbkernel_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine(t *testing.T) {
	// Create a temporary directory for storage
	baseDir := t.TempDir()
	namespace := "test_namespace"

	conf := dbkernel.NewDefaultEngineConfig()
	// Initialize Engine
	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, conf)
	assert.NoError(t, err, "Failed to initialize storage engine")
	assert.NotNil(t, engine, "Engine should not be nil")

	assert.NoError(t, engine.Close(t.Context()), "Failed to close engine")
}

func TestPutGet_Delete(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_put_get"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	key := []byte("test_key")
	value := []byte(gofakeit.Sentence(100))

	t.Run("set", func(t *testing.T) {
		err = engine.PutKV(key, value)
		assert.NoError(t, err, "PutKV operation should succeed")
		assert.Equal(t, uint64(1), engine.OpsReceivedCount())
	})

	t.Run("get", func(t *testing.T) {
		retrievedValue, err := engine.GetKV(key)
		assert.NoError(t, err, "GetKV operation should succeed")
		assert.Equal(t, uint64(1), engine.OpsReceivedCount(), "get should not increase ops count")
		assert.Equal(t, value, retrievedValue, "Retrieved value should match the inserted value")
	})

	t.Run("delete", func(t *testing.T) {
		err = engine.DeleteKV(key)
		assert.NoError(t, err, "DeleteKV operation should succeed")
		assert.Equal(t, uint64(2), engine.OpsReceivedCount(), "delete should increase ops count")
	})

	t.Run("delete_get", func(t *testing.T) {
		retrievedValue, err := engine.GetKV(key)
		assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "GetKV operation should succeed")
		assert.Nil(t, retrievedValue, "Retrieved value should match the inserted value")
	})
}

func TestConcurrentWrites(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_concurrent"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	wg := sync.WaitGroup{}
	numOps := 10

	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte("key_" + strconv.Itoa(i))
			value := []byte("value_" + strconv.Itoa(i))
			assert.NoError(t, engine.PutKV(key, value))
		}(i)
	}

	wg.Wait()

	// validate all keys exist
	for i := 0; i < numOps; i++ {
		key := []byte("key_" + strconv.Itoa(i))
		value := []byte("value_" + strconv.Itoa(i))

		retrievedValue, err := engine.GetKV(key)
		assert.NoError(t, err)
		assert.Equal(t, value, retrievedValue, "Concurrent write mismatch")
	}
	assert.Equal(t, uint64(numOps), engine.OpsReceivedCount())
}

func TestSnapshot(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)

	key := []byte("persist_key")
	value := []byte("persist_value")

	err = engine.PutKV(key, value)
	assert.NoError(t, err)

	err = engine.Close(t.Context())
	assert.NoError(t, err, "Failed to close engine")

	engine, err = dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)

	retrievedValue, err := engine.GetKV(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue, "data should be recoverable")
	assert.Equal(t, uint64(1), engine.OpsReceivedCount())
	assert.NoError(t, engine.Close(t.Context()))
}

func TestNoMultiple_ProcessNot_Allowed(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	// Second run: should error out.
	_, err = dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.ErrorIs(t, err, dbkernel.ErrDatabaseDirInUse, "expected pid lock err")
}

func TestEngine_WaitForAppend(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	key := []byte("test-key")
	value := []byte("test-value")

	callerDone := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		err := engine.PutKV(key, value)
		assert.NoError(t, err, "PutKV operation should succeed")
	}()

	err = engine.WaitForAppendOrDone(callerDone, nil)
	assert.NoError(t, err, "WaitForAppendOrDone should return without timeout after PutKV")

	err = engine.WaitForAppendOrDone(callerDone, nil)
	assert.NoError(t, err, "WaitForAppendOrDone should return without timeout after PutKV and when last seen is nil")

	cancelErr := make(chan error)
	go func() {
		callerDone := make(chan struct{})
		_, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
			close(callerDone)
		}()
		defer cancel()
		err := engine.WaitForAppendOrDone(callerDone, engine.CurrentOffset())
		cancelErr <- err
		close(cancelErr)
	}()

	err = <-cancelErr
	assert.ErrorIs(t, err, context.Canceled, "WaitForAppendOrDone should return error for cancelled context")

	lastOffset := engine.CurrentOffset()
	err = engine.PutKV(key, value)
	assert.NoError(t, err, "PutKV operation should succeed")

	err = engine.WaitForAppendOrDone(callerDone, lastOffset)
	assert.NoError(t, err, "WaitForAppendOrDone should return without error after PutKV")
	wg.Wait()
}

func TestEngine_WaitForAppend_NGoroutine(t *testing.T) {

	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	key := []byte("test-key")
	value := []byte("test-value")

	goroutines := 10
	var wg sync.WaitGroup
	var readyWg sync.WaitGroup
	wg.Add(goroutines)
	readyWg.Add(goroutines)

	callerDone := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			readyWg.Done()
			err := engine.WaitForAppendOrDone(callerDone, nil)
			assert.NoError(t, err, "WaitForAppendOrDone should return without timeout after PutKV")
		}()
	}

	readyWg.Wait()
	err = engine.PutKV(key, value)
	assert.NoError(t, err, "PutKV operation should succeed")
	wg.Wait()
	close(callerDone)
}

func TestEngine_WaitForAppend_And_Reader(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	putKey := []byte("test-key")
	putValue := []byte("test-value")

	putKV := make(map[string][]byte)
	putKV[string(putKey)] = putValue

	eof := make(chan struct{})
	callerDone := make(chan struct{})
	defer close(callerDone)
	read := func(reader *dbkernel.Reader) {
		err := engine.WaitForAppendOrDone(callerDone, nil)
		assert.NoError(t, err, "WaitForAppendOrDone should return without timeout after PutKV")
		for {
			value, _, err := reader.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					eof <- struct{}{}
					break
				}
				assert.NoError(t, err, "Reader should only return EOF Error")
			}
			record := logrecord.GetRootAsLogRecord(value, 0)
			decoded := logcodec.DeserializeFBRootLogRecord(record)
			kv := logcodec.DeserializeKVEntry(decoded.Entries[0])
			assert.Equal(t, putKV[string(kv.Key)], kv.Value, "decompressed value should be equal to put value")
		}
	}

	reader, err := engine.NewReader()
	assert.NoError(t, err, "NewReader should return without error")
	go read(reader)

	err = engine.PutKV(putKey, putValue)
	assert.NoError(t, err, "PutKV operation should succeed")

	select {
	case <-eof:
	case <-time.After(5 * time.Second):
		t.Errorf("timeout waiting for eof error")
	}

	for i := 0; i < 10; i++ {
		putKV[gofakeit.UUID()] = []byte(gofakeit.Sentence(20))
	}

	reader, err = engine.NewReaderWithStart(engine.CurrentOffset())
	assert.NoError(t, err, "NewReader should return without error")
	go read(reader)

	for k, v := range putKV {
		err = engine.PutKV([]byte(k), v)
		assert.NoError(t, err, "PutKV operation should succeed")
	}
	select {
	case <-eof:
	case <-time.After(5 * time.Second):
		t.Errorf("timeout waiting for eof error")
	}
}

// https://anishathalye.com/testing-distributed-systems-for-linearizability/
// Linearizability Testing with Porcupine.
// Linearizability is a property of the system where the order of operation is in the same order in which it has
// happened in the real world.

type dbInput struct {
	op    uint8 // 0 => read, 1 => write
	key   string
	value string
}

const (
	opGet = 0
	opPut = 1
)

func TestEngineLinearizability(t *testing.T) {
	dir := t.TempDir()
	nameSpace := "test_namespace"
	// Create the engine
	engine, err := dbkernel.NewStorageEngine(dir, nameSpace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)

	t.Cleanup(func() {
		err := engine.Close(context.Background())
		require.NoError(t, err)
	})

	// linearizability model for our key-value store
	// model is a sequential specification of a register.
	model := porcupine.Model{
		Init: func() interface{} {
			// init state: empty map
			return make(map[string]string)
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			st := state.(map[string]string)
			inp := input.(dbInput)
			out := output.(string)

			newState := make(map[string]string)
			for k, v := range st {
				newState[k] = v
			}

			switch inp.op {
			case opGet:
				val, exists := newState[inp.key]
				if !exists {
					return out == "", newState
				}

				return out == val, newState

			case opPut:
				newState[inp.key] = inp.value
				return out == "ok", newState

			default:
				return false, state
			}
		},
		DescribeOperation: func(input, output interface{}) string {
			inp := input.(dbInput)
			switch inp.op {
			case 0:
				return fmt.Sprintf("get('%s') -> '%s'", inp.key, inp.value)
			case 1:
				return fmt.Sprintf("put('%s', '%s')", inp.key, inp.value)
			default:
				return "<invalid>"
			}
		},
	}

	parallelism := 8
	opsPerClient := 200

	var wg sync.WaitGroup
	var mu sync.Mutex
	var events []porcupine.Event

	var eventID int

	recordOperation := func(clientId int, op dbInput, result string) {
		mu.Lock()
		defer mu.Unlock()

		eventID++
		events = append(events, porcupine.Event{
			Kind:     porcupine.CallEvent,
			Id:       eventID,
			ClientId: clientId,
			Value:    op,
		})

		events = append(events, porcupine.Event{
			Kind:     porcupine.ReturnEvent,
			Id:       eventID,
			ClientId: clientId,
			Value:    result,
		})
	}

	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func(clientId int) {
			defer wg.Done()

			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("key-%d-%d", clientId, j%10)

				if j%3 == 0 {
					val, err := engine.GetKV([]byte(key))

					result := ""
					if err == nil {
						result = string(val)
					}

					recordOperation(clientId, dbInput{
						op:    opGet,
						key:   key,
						value: result,
					}, result)
				} else {
					value := fmt.Sprintf("value-%d-%d", clientId, j)
					err := engine.PutKV([]byte(key), []byte(value))

					result := "ok"
					if err != nil {
						result = "error"
					}

					recordOperation(clientId, dbInput{
						op:    opPut,
						key:   key,
						value: value,
					}, result)
				}
			}
		}(i)
	}

	wg.Wait()

	res := porcupine.CheckEvents(model, events)
	assert.True(t, res)
}

func TestEngine_RowOperations(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
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
		assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "failed to build column map")
		assert.Nil(t, rowEntry, "unexpected column values")
	})
}

func TestBatchRowColumns_APIs(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	var rowKeys [][]byte
	var columnsEntries []map[string][]byte

	t.Run("put_row_columns", func(t *testing.T) {
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

	t.Run("get_row_columns", func(t *testing.T) {
		for i, rowKey := range rowKeys {
			got, err := engine.GetRowColumns(string(rowKey), nil)
			assert.NoError(t, err, "failed to get column entries")
			assert.Equal(t, columnsEntries[i], got, "unexpected column entries")
		}
	})

	t.Run("delete_row_columns", func(t *testing.T) {
		err := engine.DeleteColumnsForRows(rowKeys, columnsEntries)
		assert.NoError(t, err, "DeleteColumnsForRows operation should succeed")
	})

	t.Run("get_row_columns", func(t *testing.T) {
		for _, rowKey := range rowKeys {
			got, err := engine.GetRowColumns(string(rowKey), nil)
			assert.NoError(t, err, "failed to get column entries")
			assert.Zero(t, len(got), "unexpected number of column values")
		}
	})

	t.Run("delete_rows", func(t *testing.T) {
		err := engine.BatchDeleteRows(rowKeys)
		assert.NoError(t, err, "BatchDeleteRows operation should succeed")
		for _, rowKey := range rowKeys {
			_, err := engine.GetRowColumns(string(rowKey), nil)
			assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "failed to get column map")
		}
	})
}

func TestEngineBatchKV_APIs(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	var keys [][]byte
	var values [][]byte

	t.Run("put_kv", func(t *testing.T) {
		for i := uint64(0); i < 100; i++ {
			key := gofakeit.UUID()
			val := gofakeit.LetterN(uint(i + 1))
			keys = append(keys, []byte(key))
			values = append(values, []byte(val))
		}

		err := engine.BatchPutKV(keys, values)
		assert.NoError(t, err, "BatchPutKV operation should succeed")
	})

	t.Run("get_kv", func(t *testing.T) {
		for i, key := range keys {
			got, err := engine.GetKV(key)
			assert.NoError(t, err, "failed to get kv")
			assert.Equal(t, values[i], got, "unexpected kv")
		}
	})

	t.Run("delete_kv", func(t *testing.T) {
		err := engine.BatchDeleteKV(keys)
		assert.NoError(t, err, "BatchDeleteKV operation should succeed")
	})

	t.Run("get_kv", func(t *testing.T) {
		for _, key := range keys {
			_, err := engine.GetKV(key)
			assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "failed to get kv")
		}
	})
}

func TestEngine_NewReaderWithStart(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})
	var keys [][]byte
	var values [][]byte

	t.Run("put_kv", func(t *testing.T) {
		for i := uint64(0); i < 100; i++ {
			key := gofakeit.UUID()
			val := gofakeit.LetterN(uint(i + 1))
			keys = append(keys, []byte(key))
			values = append(values, []byte(val))
		}

		err := engine.BatchPutKV(keys, values)
		assert.NoError(t, err, "BatchPutKV operation should succeed")
	})

	offset := &wal.Offset{SegmentID: 10000}
	_, err = engine.NewReaderWithStart(offset)
	assert.ErrorIs(t, err, walfs.ErrSegmentNotFound)
}

func TestReadWriteEngine_Works(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_readwrite"

	conf := dbkernel.NewDefaultEngineConfig()
	conf.ReadOnly = false

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, conf)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	key := []byte("test_key")
	value := []byte("test_value")

	err = engine.PutKV(key, value)
	assert.NoError(t, err, "PutKV should succeed in read-write mode")

	retrievedValue, err := engine.GetKV(key)
	assert.NoError(t, err, "GetKV should succeed")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match")

	txn, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	assert.NoError(t, err, "NewTxn should succeed in read-write mode")
	assert.NotNil(t, txn, "Transaction should not be nil")
}

func TestReadOnlyEngine_WritesBlocked(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_readonly"

	conf := dbkernel.NewDefaultEngineConfig()
	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, conf)
	require.NoError(t, err)

	key := []byte("test_key")
	value := []byte("test_value")

	err = engine.PutKV(key, value)
	require.NoError(t, err)

	rowKey := []byte("row1")
	columns := map[string][]byte{
		"col1": []byte("value1"),
		"col2": []byte("value2"),
	}
	err = engine.PutColumnsForRow(rowKey, columns)
	require.NoError(t, err)

	err = engine.Close(context.Background())
	require.NoError(t, err)

	readOnlyConf := dbkernel.NewDefaultEngineConfig()
	readOnlyConf.ReadOnly = true

	readOnlyEngine, err := dbkernel.NewStorageEngine(baseDir, namespace, readOnlyConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := readOnlyEngine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	t.Run("writes_blocked", func(t *testing.T) {
		err = readOnlyEngine.PutKV([]byte("new_key"), []byte("new_value"))
		assert.ErrorIs(t, err, dbkernel.ErrEngineReadOnly, "PutKV should fail in read-only mode")

		err = readOnlyEngine.DeleteKV(key)
		assert.ErrorIs(t, err, dbkernel.ErrEngineReadOnly, "DeleteKV should fail in read-only mode")

		err = readOnlyEngine.BatchPutKV([][]byte{[]byte("k1")}, [][]byte{[]byte("v1")})
		assert.ErrorIs(t, err, dbkernel.ErrEngineReadOnly, "BatchPutKV should fail in read-only mode")

		err = readOnlyEngine.BatchDeleteKV([][]byte{[]byte("k1")})
		assert.ErrorIs(t, err, dbkernel.ErrEngineReadOnly, "BatchDeleteKV should fail in read-only mode")

		err = readOnlyEngine.PutColumnsForRow([]byte("row2"), map[string][]byte{"col": []byte("val")})
		assert.ErrorIs(t, err, dbkernel.ErrEngineReadOnly, "PutColumnsForRow should fail in read-only mode")

		err = readOnlyEngine.DeleteRow([]byte("row1"))
		assert.ErrorIs(t, err, dbkernel.ErrEngineReadOnly, "DeleteRow should fail in read-only mode")

		txn, err := readOnlyEngine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
		assert.ErrorIs(t, err, dbkernel.ErrEngineReadOnly, "NewTxn should fail in read-only mode")
		assert.Nil(t, txn, "Transaction should be nil")
	})

	t.Run("reads_allowed", func(t *testing.T) {
		retrievedValue, err := readOnlyEngine.GetKV(key)
		assert.NoError(t, err, "GetKV should work in read-only mode")
		assert.Equal(t, value, retrievedValue, "Retrieved value should match")

		retrievedColumns, err := readOnlyEngine.GetRowColumns(string(rowKey), func(columnKey string) bool {
			return true
		})
		assert.NoError(t, err, "GetRowColumns should work in read-only mode")
		assert.Equal(t, columns, retrievedColumns, "Retrieved columns should match")

		reader, err := readOnlyEngine.NewReader()
		assert.NoError(t, err, "NewReader should work in read-only mode")
		assert.NotNil(t, reader, "Reader should not be nil")
	})
}
