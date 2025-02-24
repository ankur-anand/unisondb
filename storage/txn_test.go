package storage_test

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

func TestTxn_Chunked_Commit(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test__txn_put_get"

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
	assert.Equal(t, uint64(1), engine.TotalOpsReceived())
	// Retrieve value
	retrievedValue, err := engine.Get(key)
	assert.NoError(t, err, "Get operation should succeed")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match the inserted value")

	batchKey := []byte(gofakeit.Name())

	var batchValues []string
	fullValue := new(bytes.Buffer)

	for i := 0; i < 10; i++ {
		batchValues = append(batchValues, gofakeit.Sentence(5))
		fullValue.Write([]byte(batchValues[i]))
	}

	// open a txn writer:
	txn, err := engine.NewTxn(wrecord.LogOperationInsert, wrecord.ValueTypeChunked)
	assert.NoError(t, err, "NewBatch operation should succeed")
	assert.NotNil(t, txn, "NewBatch operation should succeed")

	var checksum uint32
	for _, batchValue := range batchValues {
		err := txn.AppendTxnEntry(batchKey, []byte(batchValue))
		checksum = crc32.Update(checksum, crc32.IEEETable, []byte(batchValue))
		assert.NoError(t, err, "NewBatch operation should succeed")
	}

	// get value without commit
	// write should not be visible for now.
	got, err := engine.Get(batchKey)
	assert.ErrorIs(t, err, storage.ErrKeyNotFound, "Key not Found Error should be present.")
	assert.Nil(t, got, "Get operation should succeed")

	err = txn.Commit()
	assert.NoError(t, err, "Commit operation should succeed")

	// get value without commit
	got, err = engine.Get(batchKey)
	assert.NoError(t, err, "Get operation should succeed")
	assert.NotNil(t, got, "Get operation should succeed")
	assert.Equal(t, got, fullValue.Bytes(), "Retrieved value should match the inserted value")
}
