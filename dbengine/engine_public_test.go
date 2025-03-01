package dbengine_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/ankur-anand/kvalchemy/dbengine"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
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
