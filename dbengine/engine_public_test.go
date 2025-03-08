package dbengine_test

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
	"github.com/ankur-anand/kvalchemy/dbengine"
	"github.com/ankur-anand/kvalchemy/dbengine/compress"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine(t *testing.T) {
	// Create a temporary directory for storage
	baseDir := t.TempDir()
	namespace := "test_namespace"

	conf := dbengine.NewDefaultEngineConfig()
	// Initialize Engine
	engine, err := dbengine.NewStorageEngine(baseDir, namespace, conf)
	assert.NoError(t, err, "Failed to initialize storage engine")
	assert.NotNil(t, engine, "Engine should not be nil")

	assert.NoError(t, engine.Close(context.Background()), "Failed to close engine")
}

func TestPutGet_Delete(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_put_get"

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, dbengine.NewDefaultEngineConfig())
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
		err = engine.Put(key, value)
		assert.NoError(t, err, "Put operation should succeed")
		assert.Equal(t, uint64(1), engine.OpsReceivedCount())
	})

	t.Run("get", func(t *testing.T) {
		retrievedValue, err := engine.Get(key)
		assert.NoError(t, err, "Get operation should succeed")
		assert.Equal(t, uint64(1), engine.OpsReceivedCount(), "get should not increase ops count")
		assert.Equal(t, value, retrievedValue, "Retrieved value should match the inserted value")
	})

	t.Run("delete", func(t *testing.T) {
		err = engine.Delete(key)
		assert.NoError(t, err, "Delete operation should succeed")
		assert.Equal(t, uint64(2), engine.OpsReceivedCount(), "delete should increase ops count")
	})

	t.Run("delete_get", func(t *testing.T) {
		retrievedValue, err := engine.Get(key)
		assert.ErrorIs(t, err, dbengine.ErrKeyNotFound, "Get operation should succeed")
		assert.Nil(t, retrievedValue, "Retrieved value should match the inserted value")
	})
}

func TestConcurrentWrites(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_concurrent"

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, dbengine.NewDefaultEngineConfig())
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
			assert.NoError(t, engine.Put(key, value))
		}(i)
	}

	wg.Wait()

	// validate all keys exist
	for i := 0; i < numOps; i++ {
		key := []byte("key_" + strconv.Itoa(i))
		value := []byte("value_" + strconv.Itoa(i))

		retrievedValue, err := engine.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value, retrievedValue, "Concurrent write mismatch")
	}
	assert.Equal(t, uint64(numOps), engine.OpsReceivedCount())
}

func TestSnapshot(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, dbengine.NewDefaultEngineConfig())
	assert.NoError(t, err)

	key := []byte("persist_key")
	value := []byte("persist_value")

	err = engine.Put(key, value)
	assert.NoError(t, err)

	err = engine.Close(context.Background())
	assert.NoError(t, err, "Failed to close engine")

	engine, err = dbengine.NewStorageEngine(baseDir, namespace, dbengine.NewDefaultEngineConfig())
	assert.NoError(t, err)

	retrievedValue, err := engine.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue, "data should be recoverable")
	assert.Equal(t, uint64(1), engine.OpsReceivedCount())
	assert.NoError(t, engine.Close(context.Background()))
}

func TestNoMultiple_ProcessNot_Allowed(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, dbengine.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	// Second run: should error out.
	_, err = dbengine.NewStorageEngine(baseDir, namespace, dbengine.NewDefaultEngineConfig())
	assert.ErrorIs(t, err, dbengine.ErrDatabaseDirInUse, "expected pid lock err")
}

func TestEngine_WaitForAppend(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, dbengine.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	key := []byte("test-key")
	value := []byte("test-value")

	ctx := context.Background()
	timeout := 100 * time.Millisecond

	err = engine.WaitForAppend(ctx, timeout, nil)
	assert.ErrorIs(t, err, dbengine.ErrWaitTimeoutExceeded, "no put op has been called should timeout")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		time.Sleep(100 * time.Millisecond)
		err := engine.Put(key, value)
		assert.NoError(t, err, "Put operation should succeed")
	}()

	err = engine.WaitForAppend(ctx, 3*time.Second, nil)
	assert.NoError(t, err, "WaitForAppend should return without timeout after Put")

	err = engine.WaitForAppend(ctx, timeout, nil)
	assert.NoError(t, err, "WaitForAppend should return without timeout after Put and when last seen is nil")

	cancelErr := make(chan error)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		defer cancel()
		err = engine.WaitForAppend(ctx, timeout, engine.CurrentOffset())
		cancelErr <- err
		close(cancelErr)
	}()

	err = <-cancelErr
	assert.ErrorIs(t, err, context.Canceled, "WaitForAppend should return error for cancelled context")

	lastOffset := engine.CurrentOffset()
	err = engine.Put(key, value)
	assert.NoError(t, err, "Put operation should succeed")

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = engine.WaitForAppend(ctx, timeout, lastOffset)
	assert.NoError(t, err, "WaitForAppend should return without error after Put")
}

func TestEngine_WaitForAppend_NGoroutine(t *testing.T) {

	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, dbengine.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	goroutines := 100
	var wg sync.WaitGroup
	var readyWg sync.WaitGroup
	wg.Add(goroutines)
	readyWg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			readyWg.Done()
			err := engine.WaitForAppend(ctx, 10*time.Second, nil)
			assert.NoError(t, err, "WaitForAppend should return without timeout after Put")
		}()
	}

	readyWg.Wait()
	err = engine.Put(key, value)
	assert.NoError(t, err, "Put operation should succeed")
	wg.Wait()
}

func TestEngine_WaitForAppend_And_Reader(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, dbengine.NewDefaultEngineConfig())
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
	read := func(reader *dbengine.Reader) {
		err := engine.WaitForAppend(context.Background(), 1*time.Minute, nil)
		assert.NoError(t, err, "WaitForAppend should return without timeout after Put")
		for {
			value, _, err := reader.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					eof <- struct{}{}
					break
				}
				assert.NoError(t, err, "Reader should only return EOF Error")
			}
			record := walrecord.GetRootAsWalRecord(value, 0)
			decompressed, err := compress.DecompressLZ4(record.ValueBytes())
			assert.NoError(t, err, "Decompress should not fail for valid compressed value")
			assert.Equal(t, putKV[string(record.KeyBytes())], decompressed, "decompressed value should be equal to put value")
		}
	}

	reader, err := engine.NewReader()
	assert.NoError(t, err, "NewReader should return without error")
	go read(reader)

	err = engine.Put(putKey, putValue)
	assert.NoError(t, err, "Put operation should succeed")

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
		err = engine.Put([]byte(k), v)
		assert.NoError(t, err, "Put operation should succeed")
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
	engine, err := dbengine.NewStorageEngine(dir, nameSpace, dbengine.NewDefaultEngineConfig())
	require.NoError(t, err)

	t.Cleanup(func() {
		err := engine.Close(t.Context())
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
					val, err := engine.Get([]byte(key))

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
					err := engine.Put([]byte(key), []byte(value))

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
