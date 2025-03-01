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
	batch, err := engine.NewTxn(walrecord.LogOperationInsert, walrecord.ValueTypeChunked)
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
		err := batch.AppendTxnEntry(batchKey, []byte(value))
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
