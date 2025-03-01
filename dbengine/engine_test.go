package dbengine

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
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
