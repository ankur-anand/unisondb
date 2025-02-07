package storage_test

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
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
	assert.Equal(t, uint64(1), engine.LastSeq())
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
	assert.Equal(t, uint64(2), engine.LastSeq())
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
	assert.Equal(t, uint64(numOps), engine.LastSeq())
}

func TestPersistence(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	// First run: Insert data
	engine, err := storage.NewStorageEngine(baseDir, namespace, nil)
	assert.NoError(t, err)

	key := []byte("persist_key")
	value := []byte("persist_value")

	err = engine.Put(key, value)
	assert.NoError(t, err)

	err = engine.Close()
	assert.NoError(t, err, "Failed to close engine")
	// Second run: Reopen and check data persists
	engine, err = storage.NewStorageEngine(baseDir, namespace, nil)
	assert.NoError(t, err)

	retrievedValue, err := engine.Get(key)
	assert.NoError(t, err)
	defer func(engine *storage.Engine) {
		err := engine.Close()
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	}(engine)
	assert.Equal(t, value, retrievedValue, "Persisted data should be recoverable")
}

func TestNoMultiple_Process_Allowed(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_persistence"

	// First run: Insert data
	engine, err := storage.NewStorageEngine(baseDir, namespace, nil)
	assert.NoError(t, err)

	key := []byte("persist_key")
	value := []byte("persist_value")

	err = engine.Put(key, value)
	assert.NoError(t, err)
	defer func(engine *storage.Engine) {
		err := engine.Close()
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	}(engine)

	// Second run: should error out.
	engine, err = storage.NewStorageEngine(baseDir, namespace, nil)
	assert.ErrorIs(t, err, storage.ErrDatabaseDirInUse, "expected pid lock err")
}

func TestArenaReplacementAndFlush(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_arena_flush"

	config := &storage.StorageConfig{
		ArenaSize: 100 * wal.KB,
		// **Small threshold for direct memTable storage
		// So arena could store large number of values.
		ValueThreshold: 50,
	}

	engine, err := storage.NewStorageEngine(baseDir, namespace, config)
	assert.NoError(t, err)
	signal := make(chan struct{}, 10)
	engine.Callback = func() {
		signal <- struct{}{}
	}
	assert.NoError(t, err)

	keyPrefix := "flush_test_key_"
	valueSize := 1024 // 1KB per value (approx.)
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
	err = engine.PersistenceSnapShot(f)
	assert.NoError(t, err)
	err = f.Close()

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
	err = engine.Close()
	assert.NoError(t, err, "Failed to close engine")

	engine, err = storage.NewStorageEngine(baseDir, namespace, config)
	assert.NoError(t, err)
	assert.NotNil(t, engine)
	assert.Equal(t, keysCount+engine.RecoveredEntriesCount(), 4000)

	defer func() {
		err := engine.Close()
		assert.NoError(t, err, "Failed to close engine")
	}()

	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyPrefix, i))
		retrievedValue, err := engine.Get(key)

		assert.NoError(t, err, "Get operation should succeed")
		assert.NotNil(t, retrievedValue, "Retrieved value should not be nil")
		assert.GreaterOrEqual(t, len(retrievedValue), valueSize, "Value length mismatch")
	}

	assert.Equal(t, uint64(4000), engine.LastSeq())
}
