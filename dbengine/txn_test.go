package dbengine_test

import (
	"bytes"
	"context"
	"hash/crc32"
	"testing"

	"github.com/ankur-anand/kvalchemy/dbengine"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

func TestTxnNew(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test__txn_put_get"

	conf := dbengine.NewDefaultEngineConfig()
	conf.DBEngine = dbengine.BoltDBEngine
	conf.BtreeConfig.Namespace = namespace

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, conf)
	assert.NoError(t, err)
	ctx := context.Background()
	t.Cleanup(func() {
		err := engine.Close(ctx)
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	_, err = engine.NewTxn(walrecord.LogOperationInsert, walrecord.ValueTypeChunked)
	assert.NoError(t, err, "NewBatch operation should succeed")

	_, err = engine.NewTxn(walrecord.LogOperationDelete, walrecord.ValueTypeChunked)
	assert.ErrorIs(t, err, dbengine.ErrUnsupportedTxnType, "delete should not allow chunked  value type")

	_, err = engine.NewTxn(walrecord.LogOperationNoop, walrecord.ValueTypeChunked)
	assert.ErrorIs(t, err, dbengine.ErrUnsupportedTxnType, "Noop Txn should not succeed")
}

func TestTxn_Chunked_Commit(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test__txn_put_get"

	conf := dbengine.NewDefaultEngineConfig()
	conf.BtreeConfig.Namespace = namespace

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, conf)
	assert.NoError(t, err)
	ctx := context.Background()
	t.Cleanup(func() {
		err := engine.Close(ctx)
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	key := []byte("test_key")
	value := []byte(gofakeit.Sentence(100))

	// Put key-value pair
	err = engine.Put(key, value)
	assert.NoError(t, err, "Put operation should succeed")
	assert.Equal(t, uint64(1), engine.OpsReceivedCount())
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

	txn, err := engine.NewTxn(walrecord.LogOperationInsert, walrecord.ValueTypeChunked)
	assert.NoError(t, err, "NewBatch operation should succeed")
	assert.NotNil(t, txn, "NewBatch operation should succeed")
	err = txn.AppendTxnEntry(batchKey, []byte(batchValues[0]))
	assert.NoError(t, err, "Append operation should succeed")
	err = txn.AppendTxnEntry(key, []byte(batchValues[0]))
	// changing key from the chunked value type should error out,
	assert.ErrorIs(t, err, dbengine.ErrKeyChangedForChunkedType, "Append operation should fail")

	txn, err = engine.NewTxn(walrecord.LogOperationInsert, walrecord.ValueTypeChunked)
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
	assert.ErrorIs(t, err, dbengine.ErrKeyNotFound, "Key not Found Error should be present.")
	assert.Nil(t, got, "Get operation should succeed")

	err = txn.Commit()
	assert.NoError(t, err, "Commit operation should succeed")

	// get value without commit
	got, err = engine.Get(batchKey)
	assert.NoError(t, err, "Get operation should succeed")
	assert.NotNil(t, got, "Get operation should succeed")
	assert.Equal(t, got, fullValue.Bytes(), "Retrieved value should match the inserted value")
}

func TestTxn_Batch_KV_Commit(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test__txn_put_get"

	conf := dbengine.NewDefaultEngineConfig()
	conf.BtreeConfig.Namespace = namespace

	engine, err := dbengine.NewStorageEngine(baseDir, namespace, conf)
	assert.NoError(t, err)
	ctx := context.Background()
	t.Cleanup(func() {
		err := engine.Close(ctx)
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	kv := make(map[string][]byte)
	deletedKeys := make(map[string]struct{})

	t.Run("batch_insert", func(t *testing.T) {
		txn, err := engine.NewTxn(walrecord.LogOperationInsert, walrecord.ValueTypeFull)
		assert.NoError(t, err, "NewBatch operation should succeed")
		assert.NotNil(t, txn, "NewBatch operation should succeed")

		for i := 0; i < 100; i++ {
			key := []byte(gofakeit.UUID())
			value := []byte(gofakeit.Sentence(500))
			kv[string(key)] = value
			err := txn.AppendTxnEntry(key, value)
			assert.NoError(t, err, "Append operation should succeed")

			value, err = engine.Get(key)
			assert.ErrorIs(t, err, dbengine.ErrKeyNotFound, "Key not Found Error should be present.")
			assert.Nil(t, value, "Get operation should succeed")
		}

		assert.NoError(t, txn.Commit(), "Commit operation should succeed")
		for key, value := range kv {
			receivedValue, err := engine.Get([]byte(key))
			assert.NoError(t, err, "Get operation should succeed")
			assert.Equal(t, value, receivedValue, "Retrieved value should match the inserted value")
		}
	})

	t.Run("batch_delete", func(t *testing.T) {
		txn, err := engine.NewTxn(walrecord.LogOperationDelete, walrecord.ValueTypeFull)
		assert.NoError(t, err, "NewBatch operation should succeed")
		assert.NotNil(t, txn, "NewBatch operation should succeed")

		for i := 0; i < 20; i++ {
			key := gofakeit.RandomMapKey(kv).(string)
			deletedKeys[key] = struct{}{}
			err := txn.AppendTxnEntry([]byte(key), nil)
			assert.NoError(t, err, "Append operation should succeed")
			value, err := engine.Get([]byte(key))
			assert.NoError(t, err, "Get operation should succeed")
			assert.Equal(t, kv[key], value, "uncommited delete should not delete the inserted value")
		}

		assert.NoError(t, txn.Commit(), "Commit operation should succeed")
	})

	t.Run("verify_kv", func(t *testing.T) {
		for key, value := range kv {
			receivedValue, err := engine.Get([]byte(key))
			_, ok := deletedKeys[key]
			if !ok {
				assert.NoError(t, err, "Get operation should succeed")
				assert.Equal(t, value, receivedValue, "Retrieved value should match the inserted value")
			}

			if ok {
				assert.ErrorIs(t, err, dbengine.ErrKeyNotFound, "Key not Found Error should be present.")
				assert.Nil(t, receivedValue, "Get operation should succeed")
			}

		}
	})

}
