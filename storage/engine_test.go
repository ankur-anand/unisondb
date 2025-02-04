package storage_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/ankur-anand/kvreplicator/storage"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

// Public API Testing

func TestStorageEngine(t *testing.T) {
	// Create a temporary directory for storage
	baseDir := t.TempDir()
	namespace := "test_namespace"

	// Initialize Engine
	engine, err := storage.NewStorageEngine(baseDir, namespace, nil)
	assert.NoError(t, err, "Failed to initialize storage engine")
	assert.NotNil(t, engine, "Engine should not be nil")
}

func TestPutGet(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_put_get"

	engine, err := storage.NewStorageEngine(baseDir, namespace, nil)
	assert.NoError(t, err)
	defer func(engine *storage.Engine) {
		err := engine.Close()
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	}(engine)

	key := []byte("test_key")
	value := []byte(gofakeit.Sentence(100))

	// Put key-value pair
	err = engine.Put(key, value)
	assert.NoError(t, err, "Put operation should succeed")

	// Retrieve value
	retrievedValue, err := engine.Get(key)
	assert.NoError(t, err, "Get operation should succeed")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match the inserted value")
}

func TestDelete(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_delete"

	engine, err := storage.NewStorageEngine(baseDir, namespace, nil)
	assert.NoError(t, err)
	defer func(engine *storage.Engine) {
		err := engine.Close()
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	}(engine)

	key := []byte("delete_me")
	value := []byte("value_to_delete")

	// Insert key
	err = engine.Put(key, value)
	assert.NoError(t, err)

	// Delete key
	err = engine.Delete(key)
	assert.NoError(t, err)

	// Ensure key no longer exists
	_, err = engine.Get(key)
	assert.ErrorIs(t, err, storage.ErrKeyNotFound, "Deleted key should return key not found error")
}

func TestConcurrentWrites(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_concurrent"

	engine, err := storage.NewStorageEngine(baseDir, namespace, nil)
	assert.NoError(t, err)
	defer func(engine *storage.Engine) {
		err := engine.Close()
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	}(engine)

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

	// Validate all keys exist
	for i := 0; i < numOps; i++ {
		key := []byte("key_" + strconv.Itoa(i))
		value := []byte("value_" + strconv.Itoa(i))

		retrievedValue, err := engine.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value, retrievedValue, "Concurrent write mismatch")
	}
}
