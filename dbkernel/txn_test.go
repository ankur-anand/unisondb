package dbkernel_test

import (
	"bytes"
	"context"
	"hash/crc32"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

func TestTxnNew(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test__txn_put_get"

	conf := dbkernel.NewDefaultEngineConfig()
	conf.DBEngine = dbkernel.BoltDBEngine
	conf.BtreeConfig.Namespace = namespace

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, conf)
	assert.NoError(t, err)
	ctx := context.Background()
	t.Cleanup(func() {
		err := engine.Close(ctx)
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	_, err = engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
	assert.NoError(t, err, "NewBatch operation should succeed")

	_, err = engine.NewTxn(logrecord.LogOperationTypeDelete, logrecord.LogEntryTypeChunked)
	assert.ErrorIs(t, err, dbkernel.ErrUnsupportedTxnType, "delete should not allow chunked  value type")

	_, err = engine.NewTxn(logrecord.LogOperationTypeNoOperation, logrecord.LogEntryTypeChunked)
	assert.ErrorIs(t, err, dbkernel.ErrUnsupportedTxnType, "Noop Txn should not succeed")
}

func TestTxn_Chunked_Commit(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test__txn_put_get"

	conf := dbkernel.NewDefaultEngineConfig()
	conf.BtreeConfig.Namespace = namespace

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, conf)
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

	txn, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
	assert.NoError(t, err, "NewBatch operation should succeed")
	assert.NotNil(t, txn, "NewBatch operation should succeed")
	err = txn.AppendKVTxn(batchKey, []byte(batchValues[0]))
	assert.NoError(t, err, "Append operation should succeed")
	err = txn.AppendKVTxn(key, []byte(batchValues[0]))
	// changing key from the chunked value type should error out,
	assert.ErrorIs(t, err, dbkernel.ErrKeyChangedForChunkedType, "Append operation should fail")

	txn, err = engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
	assert.NoError(t, err, "NewBatch operation should succeed")
	assert.NotNil(t, txn, "NewBatch operation should succeed")

	var checksum uint32
	for _, batchValue := range batchValues {
		err := txn.AppendKVTxn(batchKey, []byte(batchValue))
		checksum = crc32.Update(checksum, crc32.IEEETable, []byte(batchValue))
		assert.NoError(t, err, "NewBatch operation should succeed")
	}

	// get value without commit
	// write should not be visible for now.
	got, err := engine.Get(batchKey)
	assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "Key not Found Error should be present.")
	assert.Nil(t, got, "Get operation should succeed")

	err = txn.Commit()
	assert.NoError(t, err, "Commit operation should succeed")

	got, err = engine.Get(batchKey)

	assert.NoError(t, err, "Get operation should succeed")
	assert.NotNil(t, got, "Get operation should succeed")
	assert.Equal(t, got, fullValue.Bytes(), "Retrieved value should match the inserted value")
}

func TestTxn_Batch_KV_Commit(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test__txn_put_get"

	conf := dbkernel.NewDefaultEngineConfig()
	conf.BtreeConfig.Namespace = namespace

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, conf)
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
		txn, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
		assert.NoError(t, err, "NewBatch operation should succeed")
		assert.NotNil(t, txn, "NewBatch operation should succeed")

		for i := 0; i < 100; i++ {
			key := []byte(gofakeit.UUID())
			value := []byte(gofakeit.Sentence(500))
			kv[string(key)] = value
			err := txn.AppendKVTxn(key, value)
			assert.NoError(t, err, "Append operation should succeed")

			value, err = engine.Get(key)
			assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "Key not Found Error should be present.")
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
		txn, err := engine.NewTxn(logrecord.LogOperationTypeDelete, logrecord.LogEntryTypeKV)
		assert.NoError(t, err, "NewBatch operation should succeed")
		assert.NotNil(t, txn, "NewBatch operation should succeed")

		for i := 0; i < 20; i++ {
			key := gofakeit.RandomMapKey(kv).(string)
			deletedKeys[key] = struct{}{}
			err := txn.AppendKVTxn([]byte(key), nil)
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
				assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "Key not Found Error should be present.")
				assert.Nil(t, receivedValue, "Get operation should succeed")
			}

		}
	})

}

func TestTxn_Interrupted(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test__txn_put_get"

	conf := dbkernel.NewDefaultEngineConfig()
	conf.BtreeConfig.Namespace = namespace

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, conf)
	assert.NoError(t, err)
	ctx := context.Background()
	t.Cleanup(func() {
		err := engine.Close(ctx)
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	kv := make(map[string][]byte)
	txn, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	
	t.Run("batch_insert", func(t *testing.T) {
		assert.NoError(t, err, "NewBatch operation should succeed")
		assert.NotNil(t, txn, "NewBatch operation should succeed")

		for i := 0; i < 10; i++ {
			key := []byte(gofakeit.UUID())
			value := []byte(gofakeit.Sentence(500))
			kv[string(key)] = value
			err := txn.AppendKVTxn(key, value)
			assert.NoError(t, err, "Append operation should succeed")

			value, err = engine.Get(key)
			assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "Key not Found Error should be present.")
			assert.Nil(t, value, "Get operation should succeed")
			assert.Nil(t, engine.CurrentOffset(), "uncommited should not cause the offset increase")
		}
	})

	t.Run("simple_kv_insert", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := []byte(gofakeit.UUID())
			value := []byte(gofakeit.Sentence(500))
			kv[string(key)] = value
			err := engine.Put(key, value)
			assert.NoError(t, err, "Append operation should succeed")

			value, err = engine.Get(key)
			assert.NoError(t, err, "Get operation should succeed")
		}
	})

	t.Run("txn_commit", func(t *testing.T) {
		assert.NoError(t, txn.Commit(), "Commit operation should succeed")
		assert.Equal(t, txn.CommitOffset(), engine.CurrentOffset(), "Commit operation should succeed")
	})
}

func Test_RowColumn_Txn(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test__txn_put_get"

	conf := dbkernel.NewDefaultEngineConfig()
	conf.BtreeConfig.Namespace = namespace

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, conf)
	assert.NoError(t, err)
	ctx := context.Background()
	t.Cleanup(func() {
		err := engine.Close(ctx)
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	txn, err := engine.NewTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeRow)
	assert.NoError(t, err, "NewBatch operation should succeed")

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
				key := gofakeit.UUID()
				val := gofakeit.LetterN(uint(i + 1))
				rowsEntries[rowKey][key] = []byte(val)
				entries[key] = []byte(val)
			}

			err := txn.AppendColumnTxn([]byte(rowKey), entries)
			assert.NoError(t, err, "Append operation should succeed")
		}
	}

	assert.NoError(t, txn.Commit(), "Commit operation should succeed")
	assert.Equal(t, txn.CommitOffset(), engine.CurrentOffset(), "Commit operation should succeed")

	for rowKey, entries := range rowsEntries {
		value, err := engine.GetRowColumns(rowKey, nil)
		assert.NoError(t, err, "Get operation should succeed")
		assert.Equal(t, entries, value, "Get operation should succeed")
	}

	txn2, err := engine.NewTxn(logrecord.LogOperationTypeDelete, logrecord.LogEntryTypeRow)
	assert.NoError(t, err, "NewBatch operation should succeed")

	for rowKey, entries := range rowsEntries {
		err := txn2.AppendColumnTxn([]byte(rowKey), nil)
		assert.ErrorIs(t, err, dbkernel.ErrEmptyColumns, "Append operation should succeed")
		err = txn2.AppendColumnTxn([]byte(rowKey), entries)
		assert.NoError(t, err, "Append operation should succeed")
	}

	assert.NoError(t, txn2.Commit(), "Commit operation should succeed")
	for rowKey := range rowsEntries {
		value, err := engine.GetRowColumns(rowKey, nil)
		assert.NoError(t, err, "Get operation should succeed")
		assert.Equal(t, len(value), 0, "Get operation should succeed")
	}

	txn3, err := engine.NewTxn(logrecord.LogOperationTypeDeleteRowByKey, logrecord.LogEntryTypeRow)
	assert.NoError(t, err, "NewBatch operation should succeed")
	for rowKey, entries := range rowsEntries {
		err = txn3.AppendColumnTxn([]byte(rowKey), entries)
		assert.NoError(t, err, "Append operation should succeed")
	}

	assert.NoError(t, txn3.Commit(), "Commit operation should succeed")
	for rowKey := range rowsEntries {
		_, err := engine.GetRowColumns(rowKey, nil)
		assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound, "Get operation should succeed")
	}
	assert.Equal(t, txn3.CommitOffset(), engine.CurrentOffset(), "Commit operation should succeed")
}
